using IBM.WMQ;
using Microsoft.Extensions.Configuration;
using MqiptConsumer;
using System.Collections;

// ----------------------------------------------------------------------------
// Load configuration: appsettings.json -> environment (MQ_*) -> command line.
// ----------------------------------------------------------------------------
IConfiguration config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables(prefix: "MQ_")
    .AddCommandLine(args)
    .Build();

var settings = config.GetSection("Mq").Get<MqSettings>() ?? new MqSettings();
var kinesisSettings = config.GetSection("Kinesis").Get<KinesisSettings>() ?? new KinesisSettings();

// Optional Kinesis publishing — enabled only when a stream ARN is configured.
KinesisPublisher? kinesis = null;
if (!string.IsNullOrWhiteSpace(kinesisSettings.StreamArn))
{
    try
    {
        kinesis = KinesisPublisher.FromArn(kinesisSettings.StreamArn.Trim());
    }
    catch (FormatException ex)
    {
        Console.Error.WriteLine($"Invalid Kinesis StreamArn: {ex.Message}");
        return 1;
    }
}

// Verbosity: "Debug" prints per-message detail; anything else (default) prints
// only the startup banner and a periodic throughput summary.
bool debug = string.Equals(config["LogLevel"], "Debug", StringComparison.OrdinalIgnoreCase);
// Records per Kinesis PutRecords call / MQ unit of work (Kinesis hard cap is 500).
int batchSize = int.TryParse(config["BatchSize"], out var configuredBatch) ? Math.Clamp(configuredBatch, 1, 500) : 500;
// Delivery mode. "Reliable" (default) gets under syncpoint and only commits after a
// successful publish — no message loss, but each get is a separate server round-trip.
// "Relaxed" enables read-ahead (much faster) but a crash/publish failure can drop
// messages, since they leave the queue before they're published.
bool reliable = !string.Equals(config["DeliveryMode"], "Relaxed", StringComparison.OrdinalIgnoreCase);

Console.WriteLine("IBM MQ (via MQIPT) consumer");
Console.WriteLine($"  Host         : {settings.Host}:{settings.Port}");
Console.WriteLine($"  Channel      : {settings.Channel}");
Console.WriteLine($"  QueueManager : {(string.IsNullOrEmpty(settings.QueueManager) ? "(default)" : settings.QueueManager)}");
Console.WriteLine($"  Queue        : {settings.QueueName}");
Console.WriteLine($"  Kinesis      : {(kinesis is null ? "(disabled)" : $"{kinesis.StreamName} [{kinesis.Region}]")}");
Console.WriteLine($"  Batch size   : {batchSize}");
Console.WriteLine($"  Delivery     : {(reliable ? "Reliable (syncpoint)" : "Relaxed (read-ahead)")}");
Console.WriteLine($"  Log level    : {(debug ? "Debug" : "Info")}");
Console.WriteLine();

// Graceful shutdown on Ctrl+C.
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nShutdown requested, finishing current operation...");
};

// ----------------------------------------------------------------------------
// Build the MQI connection properties. For MQIPT the client just points at the
// MQIPT host/port as if it were the queue manager's listener; MQIPT forwards on.
// MQC.TRANSPORT_MQSERIES_MANAGED selects the fully-managed .NET client (no native
// GSKit/MQ install required).
// ----------------------------------------------------------------------------
var connectionProps = new Hashtable
{
    { MQC.HOST_NAME_PROPERTY, settings.Host },
    { MQC.PORT_PROPERTY, settings.Port },
    { MQC.CHANNEL_PROPERTY, settings.Channel },
    { MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED },
};

if (!string.IsNullOrEmpty(settings.UserId))
{
    connectionProps.Add(MQC.USER_ID_PROPERTY, settings.UserId);
    if (!string.IsNullOrEmpty(settings.Password))
        connectionProps.Add(MQC.PASSWORD_PROPERTY, settings.Password);
}

MQQueueManager? qmgr = null;
MQQueue? queue = null;

try
{
    Console.WriteLine("Connecting...");
    qmgr = new MQQueueManager(settings.QueueManager, connectionProps);
    Console.WriteLine("Connected.");

    // Open the queue for input (consuming). Accept whatever the queue's default
    // input setting allows (shared by default). MQOO_INQUIRE lets us read CurrentDepth.
    // In relaxed mode, MQOO_READ_AHEAD lets the server stream messages to the client
    // buffer instead of one round-trip per get (incompatible with syncpoint).
    int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_INQUIRE | MQC.MQOO_FAIL_IF_QUIESCING;
    if (!reliable) openOptions |= MQC.MQOO_READ_AHEAD;
    queue = qmgr.AccessQueue(settings.QueueName, openOptions);
    Console.Write($"Opened queue '{settings.QueueName}'.");
    try
    {
        // CurrentDepth is only valid for local queues; remote/alias queues report 2068.
        Console.Write($" Current depth: {queue.CurrentDepth} message(s).");
    }
    catch (MQException) { /* depth not available for this queue type */ }
    Console.WriteLine("\nWaiting for messages (Ctrl+C to stop)...\n");

    var getOptions = new MQGetMessageOptions
    {
        // Wait up to WaitIntervalMs for a message, then time out and loop.
        // Reliable mode adds SYNCPOINT so each get stays uncommitted until the batch
        // is published — messages are only removed after a successful PutRecords.
        // Relaxed mode omits syncpoint so read-ahead can pipeline gets.
        Options = MQC.MQGMO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING | (reliable ? MQC.MQGMO_SYNCPOINT : MQC.MQGMO_NO_SYNCPOINT),
        WaitInterval = settings.WaitIntervalMs,
    };

    // Accumulate messages, then publish the whole batch with one PutRecords call.
    var batch = new List<(string Body, string PartitionKey)>(batchSize);
    long processed = 0; // total committed; read from the stats timer thread too.

    // Publish the current batch. In reliable mode, commit (remove) the messages only
    // after a successful publish, or back them out for redelivery on failure. In
    // relaxed mode the messages already left the queue (no syncpoint), so a failure
    // is an unavoidable drop. No-op for an empty batch.
    async Task FlushAsync()
    {
        if (batch.Count == 0) return;
        try
        {
            if (kinesis is not null)
                await kinesis.PublishBatchAsync(batch, cts.Token);

            if (reliable) qmgr!.Commit();
            long total = Interlocked.Add(ref processed, batch.Count);
            if (debug) Console.WriteLine($"  -> published {batch.Count} (total {total})");
        }
        catch (OperationCanceledException)
        {
            // Shutting down mid-publish. Reliable: leave uncommitted so MQ backs it
            // out on disconnect. Relaxed: messages already gone — nothing to do.
        }
        catch (Exception ex)
        {
            if (reliable)
            {
                // Roll back so nothing is lost; MQ will redeliver these messages.
                try { qmgr!.Backout(); } catch { /* ignore */ }
                Console.Error.WriteLine($"Kinesis publish failed, backed out {batch.Count} message(s): {ex.Message}");
            }
            else
            {
                // No syncpoint to roll back to — these messages are lost.
                Console.Error.WriteLine($"Kinesis publish failed, DROPPED {batch.Count} message(s) (relaxed mode): {ex.Message}");
            }
        }
        finally
        {
            batch.Clear();
        }
    }

    // Periodic throughput summary so the consumer isn't silent when per-message
    // logging is off. Reads only the interlocked counter — never touches the MQ
    // objects, which are not thread-safe.
    var reportInterval = TimeSpan.FromSeconds(15);
    long lastReported = 0;
    using var statsTimer = new Timer(_ =>
    {
        long total = Interlocked.Read(ref processed);
        long delta = total - Interlocked.Exchange(ref lastReported, total);
        double rate = delta / reportInterval.TotalSeconds;
        Console.WriteLine($"[stats] processed {total} total | {delta} in last {reportInterval.TotalSeconds:0}s ({rate:0.#}/s)");
    }, null, reportInterval, reportInterval);

    while (!cts.IsCancellationRequested)
    {
        // A fresh message object each iteration so we don't accumulate buffer state.
        var message = new MQMessage();
        try
        {
            queue.Get(message, getOptions);

            string body = ReadBody(message);
            string msgId = ToHex(message.MessageId);
            batch.Add((body, msgId));

            if (debug)
            {
                Console.WriteLine($"[{Interlocked.Read(ref processed) + batch.Count}] MsgId={msgId} " +
                                  $"Format={message.Format.Trim()} Bytes={message.MessageLength}");
                Console.WriteLine(body);
                Console.WriteLine(new string('-', 60));
            }

            if (batch.Count >= batchSize)
                await FlushAsync();
        }
        catch (MQException mqe) when (mqe.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE)
        {
            // Queue drained for now — publish whatever we've accumulated, then poll again.
            await FlushAsync();
            continue;
        }
    }
}
catch (MQException mqe)
{
    Console.Error.WriteLine($"MQ error: CompCode={mqe.CompCode} Reason={mqe.ReasonCode} ({mqe.Message})");
    return 1;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Unexpected error: {ex}");
    return 1;
}
finally
{
    try { queue?.Close(); } catch { /* ignore */ }
    try { qmgr?.Disconnect(); } catch { /* ignore */ }
    kinesis?.Dispose();
    Console.WriteLine("Disconnected.");
}

return 0;

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------
static string ReadBody(MQMessage message)
{
    try
    {
        // Most text/JSON/XML payloads come back as a string in the message charset.
        return message.ReadString(message.MessageLength);
    }
    catch
    {
        // Fall back to hex if the payload isn't decodable as text.
        message.Seek(0);
        byte[] bytes = message.ReadBytes(message.MessageLength);
        return ToHex(bytes);
    }
}

static string ToHex(byte[] bytes) => Convert.ToHexString(bytes);

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

Console.WriteLine("IBM MQ (via MQIPT) consumer");
Console.WriteLine($"  Host         : {settings.Host}:{settings.Port}");
Console.WriteLine($"  Channel      : {settings.Channel}");
Console.WriteLine($"  QueueManager : {(string.IsNullOrEmpty(settings.QueueManager) ? "(default)" : settings.QueueManager)}");
Console.WriteLine($"  Queue        : {settings.QueueName}");
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
    int openOptions = MQC.MQOO_INPUT_AS_Q_DEF | MQC.MQOO_INQUIRE | MQC.MQOO_FAIL_IF_QUIESCING;
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
        Options = MQC.MQGMO_WAIT | MQC.MQGMO_FAIL_IF_QUIESCING,
        WaitInterval = settings.WaitIntervalMs,
    };

    long count = 0;
    while (!cts.IsCancellationRequested)
    {
        // A fresh message object each iteration so we don't accumulate buffer state.
        var message = new MQMessage();
        try
        {
            queue.Get(message, getOptions);

            count++;
            string body = ReadBody(message);
            Console.WriteLine($"[{count}] MsgId={ToHex(message.MessageId)} " +
                              $"Format={message.Format.Trim()} Bytes={message.MessageLength}");
            Console.WriteLine(body);
            Console.WriteLine(new string('-', 60));
        }
        catch (MQException mqe) when (mqe.ReasonCode == MQC.MQRC_NO_MSG_AVAILABLE)
        {
            // No message arrived within the wait interval — normal, just poll again.
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

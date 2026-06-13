using System.Text;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;

namespace MqiptConsumer;

/// <summary>
/// Publishes message bodies to a Kinesis data stream. The region and stream name
/// are parsed from the stream ARN; AWS credentials come from the default credential
/// chain (EC2 instance role, shared profile, environment, etc.).
/// </summary>
public sealed class KinesisPublisher : IDisposable
{
    private readonly AmazonKinesisClient _client;
    private readonly string _streamArn;
    private readonly string _streamName;

    public string StreamName => _streamName;
    public string Region { get; }

    private KinesisPublisher(string streamArn, string region, string streamName)
    {
        _streamArn = streamArn;
        _streamName = streamName;
        Region = region;
        _client = new AmazonKinesisClient(RegionEndpoint.GetBySystemName(region));
    }

    /// <summary>
    /// Build a publisher from a stream ARN of the form
    /// arn:aws:kinesis:&lt;region&gt;:&lt;account&gt;:stream/&lt;name&gt;.
    /// Throws <see cref="FormatException"/> if the ARN is not a Kinesis stream ARN.
    /// </summary>
    public static KinesisPublisher FromArn(string streamArn)
    {
        // arn : aws : kinesis : region : account : stream/name
        var parts = streamArn.Split(':');
        if (parts.Length < 6 || parts[0] != "arn" || parts[2] != "kinesis")
            throw new FormatException($"Not a valid Kinesis stream ARN: '{streamArn}'.");

        string region = parts[3];
        string resource = parts[5]; // "stream/<name>"
        int slash = resource.IndexOf('/');
        if (slash < 0 || slash == resource.Length - 1)
            throw new FormatException($"Kinesis ARN is missing a stream name: '{streamArn}'.");

        string name = resource[(slash + 1)..];
        return new KinesisPublisher(streamArn, region, name);
    }

    /// <summary>
    /// Publish a batch of records, one PutRecords call per 500 records (the Kinesis
    /// per-call limit). Individually failed records — typically throttling — are
    /// retried with backoff; if any still fail after several attempts the method
    /// throws so the caller can roll back and redeliver. The partition key controls
    /// shard assignment; using the message id spreads load across shards.
    /// </summary>
    public async Task PublishBatchAsync(IReadOnlyList<(string Body, string PartitionKey)> records, CancellationToken ct)
    {
        const int maxPerCall = 500;
        const int maxAttempts = 5;

        for (int offset = 0; offset < records.Count; offset += maxPerCall)
        {
            int take = Math.Min(maxPerCall, records.Count - offset);

            // Keep payloads as byte[] so streams can be rebuilt on retry.
            var pending = new List<(byte[] Data, string PartitionKey)>(take);
            for (int i = 0; i < take; i++)
            {
                var (body, pk) = records[offset + i];
                pending.Add((Encoding.UTF8.GetBytes(body), string.IsNullOrEmpty(pk) ? "default" : pk));
            }

            for (int attempt = 1; ; attempt++)
            {
                var entries = new List<PutRecordsRequestEntry>(pending.Count);
                foreach (var (data, pk) in pending)
                    entries.Add(new PutRecordsRequestEntry { Data = new MemoryStream(data), PartitionKey = pk });

                var response = await _client.PutRecordsAsync(
                    new PutRecordsRequest { StreamARN = _streamArn, Records = entries }, ct);

                if (response.FailedRecordCount.GetValueOrDefault() == 0)
                    break;

                // Retain only the records that failed, for the next attempt.
                var retry = new List<(byte[], string)>(response.FailedRecordCount.GetValueOrDefault());
                for (int i = 0; i < response.Records.Count; i++)
                {
                    if (!string.IsNullOrEmpty(response.Records[i].ErrorCode))
                        retry.Add(pending[i]);
                }
                pending = retry;

                if (attempt >= maxAttempts)
                    throw new InvalidOperationException(
                        $"{pending.Count} record(s) still failing after {attempt} PutRecords attempts " +
                        $"(last error: {response.Records.FirstOrDefault(r => !string.IsNullOrEmpty(r.ErrorCode))?.ErrorMessage}).");

                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt), ct);
            }
        }
    }

    public void Dispose() => _client.Dispose();
}

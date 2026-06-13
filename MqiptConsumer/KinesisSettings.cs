namespace MqiptConsumer;

/// <summary>
/// Optional Kinesis publishing settings, bound from the "Kinesis" section of
/// appsettings.json / environment / command line.
///
/// Publishing is enabled only when <see cref="StreamArn"/> is set. The region and
/// stream name are derived from the ARN, and AWS credentials are resolved from the
/// default credential chain (e.g. the EC2 instance role).
/// </summary>
public sealed class KinesisSettings
{
    /// <summary>
    /// ARN of the target Kinesis data stream, e.g.
    /// arn:aws:kinesis:eu-west-2:093711202389:stream/VSCSLEI.
    /// Leave empty to disable Kinesis publishing.
    /// </summary>
    public string? StreamArn { get; set; }
}

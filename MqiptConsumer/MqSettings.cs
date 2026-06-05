namespace MqiptConsumer;

/// <summary>
/// Connection and consumer settings, bound from appsettings.json / environment / command line.
/// </summary>
public sealed class MqSettings
{
    /// <summary>Host where MQIPT is listening (the proxy front-end, not the back-end queue manager).</summary>
    public string Host { get; set; } = "localhost";

    /// <summary>Port MQIPT is listening on.</summary>
    public int Port { get; set; } = 1414;

    /// <summary>Server-connection (SVRCONN) channel name on the target queue manager.</summary>
    public string Channel { get; set; } = "SYSTEM.DEF.SVRCONN";

    /// <summary>Queue manager name. May be empty to connect to the default queue manager.</summary>
    public string QueueManager { get; set; } = "";

    /// <summary>Name of the queue to consume from.</summary>
    public string QueueName { get; set; } = "DEV.QUEUE.1";

    /// <summary>Optional MQ user id for authentication. Leave empty for no auth.</summary>
    public string? UserId { get; set; }

    /// <summary>Optional password for authentication.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Milliseconds to wait for a message before looping again.
    /// The consumer keeps polling until you press Ctrl+C.
    /// </summary>
    public int WaitIntervalMs { get; set; } = 5000;
}

# MqiptConsumer

A .NET 8 console app that connects to IBM MQ **through MQIPT** (MQ Internet Pass-Thru)
and consumes messages from a queue, using the fully-managed IBM.WMQ .NET client
(`IBMMQDotnetClient` NuGet package — no native MQ install or GSKit required).

## How MQIPT fits in

MQIPT is a proxy. The client connects to the **MQIPT** host/port exactly as if it
were the queue manager's listener; MQIPT forwards the channel traffic to the
back-end queue manager. So in config you point `Host`/`Port` at MQIPT, and set
`Channel`/`QueueManager` to match the back-end queue manager.

## Configure

Edit [appsettings.json](appsettings.json):

```json
{
  "Mq": {
    "Host": "localhost",          // MQIPT host
    "Port": 1414,                 // MQIPT listener port
    "Channel": "SYSTEM.DEF.SVRCONN",
    "QueueManager": "",           // back-end QM name ("" = default)
    "QueueName": "DEV.QUEUE.1",
    "UserId": "",                 // optional
    "Password": "",               // optional
    "WaitIntervalMs": 5000
  }
}
```

Settings can be overridden without editing the file:

- Environment variables (prefix `MQ_`), e.g. `MQ_Mq__Host=mqipt.example.com`
- Command line, e.g. `dotnet run -- --Mq:Port=1415 --Mq:QueueName=DEV.QUEUE.2`

## Run

```pwsh
cd MqiptConsumer
dotnet run
```

It connects, opens the queue, and polls continuously — printing each message's
id, format, length, and body. Press **Ctrl+C** to stop; it closes the queue and
disconnects cleanly.

## Notes

- Messages are removed from the queue as they're read (destructive get). If you
  want to peek without removing, that's an `MQGMO_BROWSE_*` change in
  [Program.cs](Program.cs).
- Each get currently auto-commits (no syncpoint). For exactly-once / transactional
  consumption, add `MQC.MQGMO_SYNCPOINT` to the get options and call
  `qmgr.Commit()` / `qmgr.Backout()`.
- TLS is **not** configured (per the plain-TCP setup). MQIPT can also terminate or
  re-originate TLS; if you later need it, set `MQC.SSL_CIPHER_SPEC_PROPERTY` and a
  key repository on the connection properties.

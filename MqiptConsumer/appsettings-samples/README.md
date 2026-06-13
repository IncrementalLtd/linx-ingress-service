# appsettings samples (one per stream)

One `appsettings.<UNIQUE>.json` per Linx topic/stream pair. Each uses
`localhost` as the MQ host, the full `NR.ALG.<UNIQUE>.TO.INC01` queue name, and
the ARN of the matching Kinesis data stream in account `093711202389`
(region `eu-west-2`).

| File                          | Queue                       | Kinesis stream | Delivery   |
| ----------------------------- | --------------------------- | -------------- | ---------- |
| `appsettings.VSCSLEI.json`    | `NR.ALG.VSCSLEI.TO.INC01`     | `VSCSLEI`      | `Reliable` |
| `appsettings.TDCSLEI.json`    | `NR.ALG.TDCSLEI.TO.INC01`     | `TDCSLEI`      | `Relaxed`  |
| `appsettings.TMCSTCLEI.json`  | `NR.ALG.TMCSTCLEI.TO.INC01`   | `TMCSTCLEI`    | `Reliable` |
| `appsettings.TMCSTRLEI.json`  | `NR.ALG.TMCSTRLEI.TO.INC01`   | `TMCSTRLEI`    | `Reliable` |
| `appsettings.TMCSIDLEI.json`  | `NR.ALG.TMCSIDLEI.TO.INC01`   | `TMCSIDLEI`    | `Reliable` |
| `appsettings.TMCSTMLEI.json`  | `NR.ALG.TMCSTMLEI.TO.INC01`   | `TMCSTMLEI`    | `Reliable` |

## Usage

The app reads `appsettings.json` from its working directory. To run a given
stream, copy the matching sample over the active config:

```powershell
Copy-Item appsettings-samples\appsettings.VSCSLEI.json .\appsettings.json -Force
.\MqiptConsumer.exe
```

To **disable** Kinesis publishing, set `Kinesis.StreamArn` to `""` (the consumer
then just prints messages to the console).

## Optional settings

These top-level keys are optional; omit them to use the defaults.

| Key            | Default      | Purpose                                                            |
| -------------- | ------------ | ------------------------------------------------------------------ |
| `DeliveryMode` | `"Reliable"` | `Reliable` = no message loss (syncpoint, commit only after a successful Kinesis publish). `Relaxed` = read-ahead for much higher throughput, but a crash or publish failure drops the in-flight batch. |
| `BatchSize`    | `500`        | Records per `PutRecords` call / MQ unit of work (clamped to 1–500). |
| `LogLevel`     | `"Info"`     | `Info` prints only the startup banner and a `[stats]` line every 15s. `Debug` adds per-message detail. |

`TDCSLEI` ships as `Relaxed` (throughput favoured); the rest are `Reliable`
(a dropped message matters more than speed).

> Relaxed mode needs the queue/channel to permit read-ahead (`DEFREADA` not `NO`).
> If it doesn't, MQ silently falls back to normal gets — no error, just no speedup.

> Note: only the `VSCSLEI` stream is currently provisioned by the CDK stack — the
> others are commented out in `infrastructure/lib/linx-ingress-stack.ts`. Publishing
> to a not-yet-created stream will fail at the PutRecord call (logged, non-fatal).

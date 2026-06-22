# Linx consumers as Windows services (NSSM)

Each Linx MQ→Kinesis consumer runs as its own Windows service — **one folder, one
exe, one config, one process per feed**. [Deploy-LinxConsumers.ps1](Deploy-LinxConsumers.ps1)
registers each instance folder as an NSSM service, refreshes the exe, and removes
services. It **never touches your `appsettings.json`** — each folder's config is
hand-maintained.

## Why services

Two layers of resilience:

| Failure | Handled by |
| ------- | ---------- |
| Transient MQ connection drop (QM/MQIPT restart, network blip) | the app's **reconnect loop** — no restart, keeps cumulative stats |
| Process crash / fatal config exit / machine reboot | **NSSM** — restarts the process (throttled) and starts it on boot |

## Folder-per-instance layout

Each feed lives in its own directory containing a self-contained `MqiptConsumer.exe`
(the .NET runtime is baked in) and its own `appsettings.json`:

```
C:\Codebase\
├── VSCS\     MqiptConsumer.exe + appsettings.json   (NR.ALG.VSCSLEI.TO.INC01)
├── TDCS\     MqiptConsumer.exe + appsettings.json   (NR.ALG.TDCSLEI.TO.INC01, Relaxed)
├── TMCSTC\   ...                                     (NR.ALG.TMCSTCLEI.TO.INC01)
├── TMCSTR\   ...                                     (NR.ALG.TMCSTRLEI.TO.INC01)
├── TMCSID\   ...                                     (NR.ALG.TMCSIDLEI.TO.INC01)
└── TMCSTM\   ...                                     (NR.ALG.TMCSTMLEI.TO.INC01)
```

The exe reads `appsettings.json` from its own folder, so each instance is fully
isolated. Service names are `LinxConsumer-<folder>` (e.g. `LinxConsumer-TDCS`).

> Per-feed example configs live in [appsettings-samples/](appsettings-samples/) — copy
> the matching one into a folder as `appsettings.json` when first setting it up, then
> edit in place (set the real `Mq.Host`, etc.).

## Prerequisites

- **Run elevated** (Administrator) — the script writes to the Service Control Manager.
- **NSSM** on `PATH`, or pass `-NssmPath`. Download: https://nssm.cc/download
- **AWS credentials**: services run as `LocalSystem`, which reaches the EC2 instance
  metadata endpoint, so the instance role is used for `PutRecords`. No keys in config.
- Each folder must already contain `appsettings.json` (the script warns if one is missing).

## Roll out a new build

Refresh just the exe (and pdb) in every instance folder — config untouched:

```powershell
.\Deploy-LinxConsumers.ps1 -Root C:\Codebase -ExeSource .\MqiptConsumer.exe
```

(Stops each service, copies the exe, restarts.) Omit `-ExeSource` to (re)register
services against the exe already in each folder.

## Install / register services

```powershell
# Discover every subfolder of C:\Codebase containing MqiptConsumer.exe and register each:
.\Deploy-LinxConsumers.ps1 -Root C:\Codebase

# Target a specific subset:
.\Deploy-LinxConsumers.ps1 -Root C:\Codebase -Folders TDCS,VSCS
```

Each service is set to: auto-start on boot, **restart on exit** (throttled to ≥10s so
a deliberate fatal-config exit can't hot-loop), with rotating per-feed logs.

| Parameter | Default | Purpose |
| --------- | ------- | ------- |
| `-Root` | *(required)* | parent folder of the instance directories |
| `-Folders` | *(auto-discover)* | explicit folder names instead of discovery |
| `-ExeSource` | *(none)* | refresh the exe in each folder before registering |
| `-ServicePrefix` | `LinxConsumer-` | service name prefix |
| `-NssmPath` | `nssm` | path to `nssm.exe` |
| `-Remove` | — | remove the services (folders/configs left in place) |

## Remove

```powershell
.\Deploy-LinxConsumers.ps1 -Root C:\Codebase -Remove
```

## Operate

```powershell
# Status of all consumers:
Get-Service LinxConsumer-* | Format-Table Name, Status, StartType

# Per service (via NSSM or sc):
nssm status  LinxConsumer-TDCS
nssm restart LinxConsumer-TDCS
nssm stop    LinxConsumer-TDCS
nssm start   LinxConsumer-TDCS
```

To change a feed's behaviour, edit that folder's `appsettings.json` and restart its
service — no re-deploy needed:

```powershell
notepad C:\Codebase\TDCS\appsettings.json
nssm restart LinxConsumer-TDCS
```

## Logs

Per-feed rotating logs (10 MB rotation) under each instance folder:

```
C:\Codebase\TDCS\logs\out.log    # [stats] throughput lines (per-message detail at LogLevel=Debug)
C:\Codebase\TDCS\logs\err.log    # reconnect warnings, fatal errors, Kinesis publish/backout
```

## What restarts vs. what reconnects

- **Reconnects in-process** (no restart): `MQRC_CONNECTION_BROKEN` (2009),
  queue-manager unavailable/quiescing, host/channel temporarily unavailable, etc.
  Backoff is exponential, capped ~60s, with jitter.
- **Exits → NSSM restarts**: genuinely fatal config errors — not authorised (2035),
  unknown queue (2085), wrong channel (2540), wrong QM name (2058) — and any
  unexpected crash. NSSM restarts after ≥10s. If a service keeps cycling, check
  `err.log`: a fatal code means the **config is wrong**, not the connection.

## Note on Kinesis being enabled

A folder whose `appsettings.json` has no `Kinesis` section (or an empty `StreamArn`)
runs with **publishing disabled** — it consumes from the queue and discards the
messages (nothing is sent to Kinesis or stored). Add the stream ARN to enable it:

```json
"Kinesis": { "StreamArn": "arn:aws:kinesis:eu-west-2:093711202389:stream/TDCSLEI" }
```

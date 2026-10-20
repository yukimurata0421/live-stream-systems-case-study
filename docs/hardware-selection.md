# Hardware Selection and Workload Separation

Recorded: 2026-10-14 JST. This is a retrospective of the operator's choices,
cross-checked against manufacturer specifications and retained measurement
summaries. The recording date is not the purchase or migration date.

The hardware followed the constraint at each stage: receiver processing,
competition with video encoding, cooling uncertainty, home uplink capacity,
and the cost of interrupting a working stream. The resulting system uses a
ProDesk for reception and observation, a Dell workstation for video production,
and a Pi for public status publication. See [physical topology](physical-topology.md)
for component responsibilities.

## Why Reception Moved From Pi to ProDesk

The operator reports sample loss on a Raspberry Pi 5 after increasing the
Airspy Mini's `airspy_adsb` setting from 12 to 20 MSPS. A processing thread
could fall behind even when aggregate CPU usage looked low. This led to moving
`airspy_adsb` and `readsb` to an x86 HP ProDesk instead of treating memory
capacity as the first constraint. The original logs were not independently
reviewed for this retrospective, so this remains the operator's diagnosis.

These are ADS-B software settings. The configuration project documents both
values and explicitly says that its 20 MSPS setting is not officially supported on the Mini;
it also describes additional cooling precautions. They should not be confused
with the manufacturer's general I/Q output specification or taken as proof of
stable reception. The product sheet separately lists a 20 MSPS ADC and
10/6/3 MSPS I/Q output. [Airspy configuration notes](https://github.com/wiedehopf/airspy-conf#changing-airspy_adsb-options),
[Airspy Mini specifications](https://airspy.com/airspy-mini/).

Reception, browser rendering, encoding, and monitoring then competed on the
ProDesk. Moving video production to the Dell separated their compute resources.
Separate hosts still share the LAN, power environment, uplink, and receiver data
dependency. More cores alone do not establish better single-thread performance.

## Why the Dell Uses an E5-2695 v4

The Precision Tower 5810 was evaluated as a complete workstation, including its
OEM cooling, airflow, and power options. Easy replacement with a retail CPU
cooler was not assumed. Dell's sheet lists selected E5-2600 v4 configurations
up to 22 cores and several power supplies; it does not qualify every proposed
component combination. [Dell 5810 specification sheet](https://i.dell.com/sites/csdocuments/Shared-Content_data-Sheets_Documents/es/es/Dell_Precision_Tower_5000_Series_5810_SpecSheet.pdf).

| Specification | Selected: E5-2695 v4 | Considered: E5-2699 v4 |
| --- | ---: | ---: |
| Cores / threads | 18 / 36 | 22 / 44 |
| Base frequency | 2.10 GHz | 2.20 GHz |
| Maximum turbo frequency | 3.30 GHz | 3.60 GHz |
| TDP | 120 W | 145 W |

Values were checked against [Intel's E5-2695 v4 specifications](https://www.intel.com/content/www/us/en/products/sku/91316/intel-xeon-processor-e52695-v4-45m-cache-2-10-ghz/specifications.html)
and [E5-2699 v4 specifications](https://www.intel.com/content/www/us/en/products/sku/91317/intel-xeon-processor-e52699-v4-55m-cache-2-20-ghz/specifications.html).
Maximum turbo is not a sustained all-core frequency. TDP is not measured wall
power or a noise rating.

The operator chose the 2695 for its lower TDP, price, and expected compute
headroom while cooling and noise were uncertain. CPU and memory were ordered
before the chassis arrived to shorten assembly time. The installed 825 W PSU
was discovered after arrival, and noise was subjectively lower than expected.
Those later observations were not information available at CPU selection.
Purchase prices, sound levels, wall power, and a 2699 comparison are unmeasured
in this record. The PSU rating alone does not establish CPU cooling headroom.

A read-only inventory on 2026-10-14 JST confirmed a Precision Tower 5810 with
one E5-2695 v4, 18 cores, and 36 logical threads. It did not benchmark the
alternative CPU or establish that this is the optimum configuration.

## Why a GTX 1070 Does Not Mean Every Workload Uses GPU

The GTX 1070 8 GB was selected for NVENC video encoding and room for future GPU
work. Earlier ARENA experiments on a GTX 1060 6 GB influenced that choice, but
they also showed why GPU execution needs workload-specific evidence:

| Historical ARENA workload | GTX 1060 | Core i7-8700K |
| --- | ---: | ---: |
| Bayesian phase comparison: 4 chains, 3000 draws | 658 s | 17 s |
| Multi change-point K=3: 1 chain, 4500 draws | 748 s | 15 s |

These are retained results from the earlier machine, not benchmarks of the
Dell or NVENC. They do not show that changing to a GTX 1070 accelerates those
models. [ARENA benchmark record](https://github.com/yukimurata0421/arena-eval-engine/blob/main/docs/principles/statistical-assumptions-and-limitations.md#gpu-vs-cpu-performance-on-small-datasets).
ARENA's [execution policy](https://github.com/yukimurata0421/arena-eval-engine/blob/main/docs/facts/performance/mcmc-parallelism-and-gpu-policy.md)
and [selection code](https://github.com/yukimurata0421/arena-eval-engine/blob/main/src/arena/lib/platform_setup.py)
prefer CPU for small models and distinguish physical GPU presence from an
available JAX CUDA backend. The June CPU fallback record is historical, not a
fresh inspection of that Python environment.

The October hardware inventory confirmed a GTX 1070 with 8192 MiB. NVENC video
encoding and general GPU computation remain separate capabilities and decisions.

## Encoding Freed CPU; Delivery Still Needed Measurement

Moving from `libx264` to `h264_nvenc` was intended to free CPU resources. The
retained encoder trials show why configured video bitrate alone was insufficient:
audio, multiplexing, rate control, and transport also affect upload volume.
The May 31 trial selected 5 fps / 3400k CBR, with upload p95 of 4.905 Mbps and
good YouTube input health during the short trial. This was a historical direct
route result, not a baseline for the changed uplink or OCI relay.
[Encoder upload case study](v3/encoder-upload-case-study.md),
[dated FPS comparison](v3/encoder-fps-tuning-2026-05-31.md).

The Pi's publication to GCS and Cloudflare moves public status reads off the
home uplink. OCI fanout addresses the cost of sending media separately to
YouTube and Twitch. These solve different constraints; static publication is
not a video relay, and a shared relay adds a dependency. Recovery deployment
and qualification are covered by the [OCI delivery record](v3/oci-relay-and-twitch.md),
not established by this hardware retrospective.

## Maintenance Cost and Remaining Options

The operator reports adding UPS protection after a brief power outage, including
the router. Battery runtime, the complete protected-device list, and upstream
network resilience were not verified here.

A 2699 replacement is currently a low priority: it requires a shutdown,
disassembly, and validation for an unmeasured gain. A separate used dual-CPU
rack server is a possible future route to parallel validation while retaining
the Dell for rollback. It has not been purchased or qualified. Workload benefit,
cooling, noise, power, GPU compatibility, and migration cost would need review
before that decision.

This update records the selection reasoning. It changes no hardware, driver,
service, delivery route, or recovery policy, and adds no new performance or
availability claim.

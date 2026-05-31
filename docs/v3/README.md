# stream_v3 Documentation

`stream_v3` is the current architecture described by this public repository.

## Documents

- `current-runtime-contract.md`
- `runtime-state-and-evidence.md`
- `sli-and-dashboard.md`
- `encoder-fps-tuning-2026-05-31.md`
- `runbooks.md`
- `decisions.md`
- `program-map.md`
- `open-followups.md`
- `../sli-methodology.md`

## Core Claim

The system is easier to operate when delivery and observation are split:
delivery keeps video and audio moving; observation keeps evidence, SLI, and
recovery decisions coherent.

The public topology also names the production data flow: Airspy on HP ProDesk
feeds `airspy_adsb`, ProDesk readsb, Dell readsb, Dell modified tar1090, and
then the `stream_v3` k3s delivery workload. The HP ProDesk is also the
observability host.

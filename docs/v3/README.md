# stream_v3 Documentation

`stream_v3` is the current architecture described by this public repository.

## Documents

- `current-runtime-contract.md`
- `runtime-state-and-evidence.md`
- `sli-and-dashboard.md`
- `runbooks.md`
- `decisions.md`
- `program-map.md`
- `open-followups.md`

## Core Claim

The system is easier to operate when delivery and observation are split:
delivery keeps video and audio moving; observation keeps evidence, SLI, and
recovery decisions coherent.

The public topology also names the physical split: Dell workstation for k3s
delivery, HP ProDesk for observability, and Raspberry Pi for ADS-B edge/source
data.

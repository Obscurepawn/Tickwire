# Run Log (`L0_OrsSessionRouting`)

Verified with `./scripts/check-tla.sh strategy_intake` (`TLC 2.19`, `-coverage 60`):

```bash
./scripts/check-tla.sh strategy_intake
```

| Configuration | Generated States | Distinct States | Diameter | Next Actions Covered | Wall Time | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `L0_OrsSessionRouting.cfg` (`MaxSlots=2, MaxMsgSeq=2, MaxOrdSeq=1`) | 230,651 | 21,488 | 12 | 11 / 14 | 6s | PASS |
| `L0_OrsSessionRouting_restart.cfg` (`MaxSlots=1, MaxOrdSeq=2, MaxRestarts=1, MaxAdminOps=2`) | 891,287 | 73,856 | 13 | 14 / 14 | 15s | PASS |

Combined across the two configurations, all 14 `Next` actions (`AdvanceClock`, `DopLogout`, `DopLogin`, `SubmitNewOrderAdmit`, `RejectUnauthorizedAccount`, `RejectMsgSeqGap`, `RejectDisabledStratNewOrder`, `RejectStaleNewOrder`, `SubmitStrategyCancel`, `AdminDisableAndStartSweep`, `AdminSweepNextSlot`, `AdminEnableStrat`, `VenueAckOrder`, `VenueTerminalRelease`) achieve nonzero firing counts with `0 states left on queue`.

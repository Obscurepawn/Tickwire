# Reachability witnesses

The two safety configurations have no counterexample. Two diagnostic configurations independently establish that the order can reach economic booking and worker delivery.

* In the [27-state trace](evidence/NoComplete.log), a covered quote reaches the worker and account, the worker's buy is admitted, and a single native write consumes one quota unit. The REST ACK is captured. The private trade reaches the account and the worker. A second delivery advances its input cursor while the account retains exactly one normalized base unit.
* In the [22-state trace](evidence/NoFillBeforeAck.log), the private trade books before an HTTP acknowledgement arrives. The hosted worker consumes the fill from the account history while the order session still awaits its response.

These probes use `NoComplete` and `NoFillBeforeAck`, which are negations of the desired witnesses. They deliberately reside outside the regression configurations.

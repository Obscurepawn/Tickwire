# v2 备注

实际Red显示第二次DeliveryBudget把Mono从0推进到2秒时，1秒请求仍以旧second放行，WSS对端接收219字节。最小修复在最后check_clock返回后再sample；核域、顺序、原请求fresh和本地截止后直接返回，保留原SignedTime。修后两项真实网络回归均为0字节/NoBytes，私有流41项及组合12项原生测试和Clippy通过。

原有回退分类保持，新增末次回退归Error::Clock，域不符归ClockDomain。修复证据见docs/validation/binance-private-clock-adapter/local-deadline；修前完整源码/证据见docs/validation/binance-private-session/pre-final-gate-v2。后续TimeClient网络及ClockAdapter组合模型独立规划。

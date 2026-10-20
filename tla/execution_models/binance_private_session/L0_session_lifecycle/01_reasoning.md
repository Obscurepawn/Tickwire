# L0 推理

每个可见状态对应 owner 同步调用边界，首次接受回调保留 Gate 与 Final 两个内部阶段：GateCallbacksReady 保存旧 second；GateBudgetExpires 在保留旧 second 的同时推进真实 Mono 到截止。FinalSampleExpired 使用末次样本拒绝，FirstAccept 和 GateBlocked 只能在末次样本仍有效时发生，其他API不可交错。时间跳转和 poll 的过期检查合并，避免在未调用poll时把wall clock变化误写成自动修改status。

字节接受度用0、1、2表示零、部分、全部，请求长度和具体协议字节由真实网络测试覆盖；属性只依赖这三个区别。历史记录保留每次请求的接受度、首次接受时刻、截止和结果，因而可以检查NoBytes请求不会后续接受、确认需要完整请求、旧请求不能改变新状态。Clock adapter的区间判断保留为真实必调用的准许/拒绝结果，首接受前后的时间检查具体建模，区间数学交给clock阶梯。

认证模式固定为Spot签名、Spot登录或USD-M。诊断status/subscriptions不纳入L0，登录/订阅/远端关闭和USD-M创建/续期/删除均保留。API请求成功和EOF同次返回时，动作在发布可见状态前撤销Receiving；已完整接收的事件保留原epoch，排空之前不允许重连。事件值用两个值表达顺序与身份见证，缓存容量取实际允许的1或2；不模拟JSON字符和WS碎片。

USD-M lease从请求开始而非回复时刻计算。renew的回复同时检查旧lease与request期限。远端DELETE开始后owner停止自动重建，接受后不确定结果保留remote_close_unknown。失败计数及指数退避保留；成功建立Receiving后清零。有限配置至多两个连接尝试，未触达30秒退避上限，不验证共享限流负载。

有限输入的时间上界由请求数与代次数限制：每个续期最多增加1,800,000ms，每次失败至多增加5,000ms或一个lease截止，所有配置小于100,000,000ms，低于TLC整数上限。未建模的心跳/轮换与服务器终止归入关闭缺口分支；精确帧调度、共享限流和账户恢复留在后续层。

事件回调拒绝对应独立 SinkFailure：真实 sink_failed 清理网络并永久 Stopped。Notice/State 回调的同类拒绝及其回调次数未单独枚举；本级不把它并入可重连的网络错误。独立审阅指出了初稿注释与 Fail 转移的这一不一致，修正只涉及模型。

v2 的 lateCallback 是历史见证：它只在第二次签名检查中的预算回调跨过本地截止时置位，该请求以后不得接受任何字节。两个内部阶段不允许 owner 的其他 API 重入；最终检查之后没有许可、预算或签名回调。签名区间、Instant 独立校验仍作为受信适配器/传输契约，未把这次 Mono 截止修复扩大为网卡发送时间保证。旧 GateExpires 对应更早回调导致的期限拒绝。

预算跨截止动作只允许 Signed 操作。USD-M key 管理没有 SignedTime，check_clock 直接返回，不执行 DeliveryBudget；普通无回调调度耗时没有单独展开。

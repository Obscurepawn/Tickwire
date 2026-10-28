#!/usr/bin/env python3
"""Regenerate the self-contained SVG architecture figures in this directory."""
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BG = '#f4f7fb'
INK = '#13263e'
MUTED = '#50647d'
BORDER = '#cbd7e5'
BLUE = '#2369b5'
TEAL = '#087f8c'
AMBER = '#a66b14'

class Diagram:
    def __init__(self, name, height, title, subtitle, desc):
        self.name = name
        self.parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="1440" height="{height}" viewBox="0 0 1440 {height}" role="img" aria-labelledby="diagram-title diagram-desc">
<title id="diagram-title">{escape(title)}</title>
<desc id="diagram-desc">{escape(desc)}</desc>
<defs>
  <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{BLUE}"/></marker>
  <marker id="muted-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{MUTED}"/></marker>
  <style>text {{font-family: 'Noto Sans CJK SC', 'Microsoft YaHei', 'PingFang SC', sans-serif;fill:{INK};}} .title {{font-weight:700;}} .muted {{fill:{MUTED};}}</style>
</defs>
<rect width="1440" height="{height}" rx="18" fill="{BG}"/>
''']
        self.text(48, 56, title, 30, weight=True)
        self.text(48, 91, subtitle, 18, color=MUTED)

    def text(self, x, y, text, size=20, color=INK, anchor='start', weight=False):
        self.parts.append(f'<text x="{x}" y="{y}" font-size="{size}" text-anchor="{anchor}" style="fill:{color}"' + (' class="title"' if weight else '') + f'>{escape(text)}</text>')

    def rect(self, x, y, w, h, fill='#ffffff', stroke=BORDER, radius=14):
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')

    def box(self, x, y, w, h, title, lines, accent=BLUE, fill='#ffffff', label=None):
        self.rect(x, y, w, h, fill)
        self.parts.append(f'<path d="M {x+16} {y+1} H {x+w-16}" stroke="{accent}" stroke-width="4"/>')
        if label:
            self.text(x+20, y+30, label, 15, color=accent, weight=True)
            offset=61
        else:
            offset=36
        self.text(x+20, y+offset, title, 23, weight=True)
        for i, line in enumerate(lines):
            self.text(x+20, y+offset+32+i*28, line, 18, color=MUTED)

    def arrow(self, points, muted=False, dash=False, both=False):
        d = 'M ' + ' L '.join(f'{x} {y}' for x,y in points)
        marker='muted-arrow' if muted else 'arrow'
        attrs = f' marker-end="url(#{marker})"'
        if both:
            attrs += f' marker-start="url(#{marker})"'
        if dash:
            attrs += ' stroke-dasharray="7 6"'
        self.parts.append(f'<path d="{d}" fill="none" stroke="{MUTED if muted else BLUE}" stroke-width="2.5" stroke-linejoin="round"{attrs}/>')

    def finish(self):
        (ROOT / self.name).write_text('\n'.join(self.parts) + '\n</svg>\n')


def architecture():
    d=Diagram('architecture.svg', 900, 'Tickwire · 多进程架构', '共享内存分发行情，执行进程按账户风险域隔离；控制面协调低频变更。',
              'Binance公共行情进入独立MD，经共享内存广播到独立策略。策略通过SPSC向执行账户进程发送意图并接收账户回报。执行进程连接Binance交易端，保存账户和输入WAL并异步复制。Supervisor持有子进程；控制Raft处理低频配置、发布和执行权；Guardian独立查询和撤单。')
    d.rect(32, 122, 1376, 172, '#e8eef7', '#d8e2ef')
    d.text(52, 151, '控制面', 17, color=MUTED, weight=True)
    d.box(56, 171, 360, 100, 'Supervisor', ['持有子进程 · 认证控制连接'], fill='#fdfefe')
    d.box(456, 171, 440, 100, 'Control Raft / 配置与发布', ['配置、发布、执行权的低频协调'], fill='#fdfefe')
    d.box(936, 171, 448, 100, 'Guardian', ['独立密钥 · 紧急查询 / 撤单 · 审计'], accent=AMBER, fill='#fdfefe')
    d.text(56, 335, '数据面', 17, color=MUTED, weight=True)
    d.box(56, 359, 240, 172, 'Binance 公共行情', ['Spot / USDⓈ-M', 'WSS 增量 + HTTPS 快照', 'JSON / Spot SBE'])
    d.box(360, 359, 240, 172, 'MD 进程', ['深度同步 · 规范化', '固定记录 + arena', '单写共享广播 ring'])
    d.box(664, 359, 304, 172, '策略进程 × N', ['每个实例独立运行', 'JOIN 快照 + 增量追平', '策略自有市场副本'])
    d.box(1032, 359, 352, 172, '执行 / 账户进程', ['每个账户风险域独立', '定点风控 · 订单与资金单写', '持久发送 owner · TLS'])
    for a,b in [(296,360),(600,664),(968,1032)]:
        d.arrow([(a,414),(b,414)])
    d.arrow([(1032,486),(968,486)])
    d.text(304, 395, '流', 15, color=BLUE)
    d.text(608, 395, 'SHM', 15, color=BLUE)
    d.text(976, 395, '意图', 15, color=BLUE)
    d.text(976, 514, '回报', 15, color=BLUE)
    d.arrow([(240,271),(240,310),(816,310),(816,359)], muted=True, dash=True)
    d.arrow([(676,271),(676,322),(1208,322),(1208,359)], muted=True, dash=True)
    d.box(1032, 660, 352, 140, 'Binance 交易端', ['签名 REST / WS', '私有事件 + 只读对账'])
    d.arrow([(1260,531),(1260,660)], both=True)
    d.text(1280, 594, '请求', 18, color=BLUE)
    d.text(1280, 622, '回报', 18, color=BLUE)
    d.box(664, 660, 304, 140, '持久化与恢复', ['账户 WAL · 输入 WAL', '不可变依赖对象闭包'], accent=TEAL)
    d.box(360, 660, 240, 140, '镜像进程', ['有序前缀 + 依赖对象', 'mTLS 持久复制'], accent=TEAL)
    d.arrow([(1100,531),(1100,592),(816,592),(816,660)])
    d.text(828, 582, '意图 / 经济事实 / 恢复依赖', 18, color=MUTED)
    d.arrow([(664,730),(600,730)])
    d.text(380, 635, '默认后台异步；显式支持 SyncReplica', 17, color=TEAL)
    d.arrow([(1384,220),(1400,220),(1400,730),(1384,730)], muted=True, dash=True)
    d.rect(56, 660, 240, 140, '#eaf4f4', '#c8e0e1')
    d.text(76, 695, '执行边界', 20, weight=True)
    for i,line in enumerate(['策略代码位于策略进程', '逐单路径使用本地门禁', '恢复完成后保持冻结']):
        d.text(76, 727+i*25, line, 16, color=MUTED)
    d.text(56, 854, '实线：行情、命令、回报与持久数据    虚线：监督、控制与紧急执行    IPC 中保留稳定业务标识', 18, color=MUTED)
    d.finish()


def order_flow():
    d=Diagram('order-flow.svg', 860, 'Tickwire · 从策略意图到经济事实', '发送前同步意图与依赖；收到事实后按输入 WAL → 账户 WAL → 状态发布推进。',
              '策略产生完整规范命令；执行端准入并预留风险；同步恢复依赖和Admit到账户WAL；首次输出前刷新授权、时钟、配额，TLS接受字节。Binance响应或私有事件先写输入WAL，再验证候选账户并同步账户WAL，最后发布状态和策略回报。默认复制后台异步，恢复保持冻结。')
    boxes=[
        (56,'01 · 策略意图',['完整命令与稳定身份','绑定行情触发和截止时间']),
        (392,'02 · 准入与预留',['当前授权 / 行情 / 风险','定点计算资金与订单责任']),
        (728,'03 · 本地持久完成',['恢复依赖先同步','Admit → 账户 WAL 同步']),
        (1064,'04 · 首次输出',['刷新租约 / 时钟 / 配额','TLS 接受字节后保留责任']),
    ]
    for x,title,lines in boxes:
        d.box(x,170,288,148,title,lines)
    for x in [344,680,1016]:
        d.arrow([(x,244),(x+48,244)])
    d.rect(56,350,960,70,'#eaf4f4','#c8e0e1')
    d.text(76,380,'默认模式：本地 durable intent 完成后发送，复制在后台推进。',19,color=TEAL)
    d.text(76,405,'SyncReplica 显式等待镜像持久 ACK；发送仍通过当前交易授权门禁。',17,color=MUTED)
    d.arrow([(1208,318),(1208,474)])
    d.text(1230,403,'网络',18,color=BLUE)
    reversed_boxes=[
        (1064,'05 · Binance',['请求响应 / 私有事件','只读订单、成交与账户查询']),
        (728,'06 · 输入 WAL',['认证原文 + 规范决定','先同步输入，再处理账户']),
        (392,'07 · 账户 WAL',['验证候选状态与去重','同步经济事实，随后发布']),
        (56,'08 · 策略回报',['有序账户记录与消费水位','成交 / 费用 / 终态可追溯']),
    ]
    for x,title,lines in reversed_boxes:
        d.box(x,474,288,148,title,lines,accent=TEAL)
    for x in [1064,728,392]:
        d.arrow([(x,548),(x-48,548)])
    d.text(56,680,'失败与恢复',18,color=MUTED,weight=True)
    notes=[
        (56,'零字节接受',['验证并同步 Aborted，','随后释放对应责任。']),
        (504,'已接受 / 结果不明',['保留预留和订单责任，','通过原身份查询与对账。']),
        (952,'进程恢复',['验证完整依赖，按原序重放；','保持冻结，重新对账与授权。']),
    ]
    for x,title,lines in notes:
        d.box(x,702,432,116,title,lines,accent=AMBER,fill='#fffaf1')
    d.finish()


def handover():
    d=Diagram('schema3-handover.svg', 1160, 'Tickwire · schema3 策略交接', '从固定 S 重放到实际 C；持久安装与独立回滚探测完成后，协调器推进 Commit / Release。',
              '旧策略保存S，新Shadow恢复S。协调器发送Prepare，Host停止旧转发并取得C，启动旧binary重放S到C。目标迁移C并应用新配置、持久安装。独立probe恢复安装输出并成功退出。执行端返回ReplayPrepared，协调器采纳后Commit，持久管理权移交，然后Release、旧实例退休和目标激活。')
    labels=['旧策略 Active','Host / 执行端','目标 Shadow','旧 binary 回放','独立回滚探针','协调器']
    xs=[130,366,602,838,1074,1310]
    for x,label in zip(xs,labels):
        d.rect(x-104,132,208,62,'#ffffff')
        d.text(x,171,label,21,anchor='middle',weight=True)
        d.parts.append(f'<path d="M {x} 194 V 1074" stroke="#cbd7e5" stroke-width="1.5" stroke-dasharray="5 6"/>')
    def msg(a,b,y,label,reply=False):
        d.arrow([(xs[a],y),(xs[b],y)],muted=reply,dash=reply)
        d.text((xs[a]+xs[b])/2,y-12,label,17,anchor='middle',color=MUTED if reply else INK)
    msg(0,1,240,'固定 S / 持久输入闭包')
    msg(1,2,286,'启动并恢复 S')
    msg(2,1,332,'READY：恢复阶段完成',True)
    msg(5,1,390,'签名 Prepare：绑定 operation / member / 当前挑战')
    msg(1,0,436,'停止旧转发，封口 C')
    msg(0,1,482,'实际 C + 完整 S → C 输入',True)
    msg(1,3,528,'启动旧 binary，按原输入顺序重放 S → C')
    msg(3,1,574,'真实回放完成 / C 检查点',True)
    msg(1,2,620,'迁移 C / ConfigApply / 安装')
    msg(2,1,666,'持久安装与实际消费证明',True)
    msg(1,4,712,'独立探针恢复 C 的迁移输出')
    msg(4,1,758,'完整输出重读 + 子进程成功退出',True)
    msg(1,5,804,'签名 ReplayPrepared：源回放、安装、回滚与当前 READY')
    msg(5,1,850,'Commit：绑定已采纳的 ReplayAdoption')
    d.rect(246,865,240,48,'#eaf4f4','#c8e0e1',8)
    d.text(366,895,'同步管理权移交记录',17,anchor='middle',color=TEAL,weight=True)
    msg(5,1,947,'Release：全部成员已 Committed')
    msg(1,0,993,'QUIESCE / DETACH / 退出')
    msg(1,2,1039,'旧进程已退出，再激活目标')
    d.rect(48,1090,1344,44,'#e8eef7','#d8e2ef',8)
    d.text(720,1119,'历史回放与 Shadow 阶段关闭命令输出；订单创建者保留，管理权移交后由新实例继续消费迟到成交。',18,anchor='middle',color=MUTED)
    d.finish()


def market_expansion():
    d=Diagram('market-expansion.svg', 1020, 'Tickwire · 运行中追加行情标的',
              '固定 catalog 内追加成员：先预热与批准，再退出旧 bank，最后完成真实 JOIN 和持久收据。',
              '受信launcher提交完整扩容计划。MD在旧流继续运行时预热新增来源，执行与策略验证当前身份并持久准备，返回Armed。MD撤销旧资格并核验真实QUIESCED，建立新bank完整基线，保存effective并开放JOIN。消费者以新身份追平激活并保存输入或effective，MD核对真实Active及全部收据后完成。退出前失败保留旧流，退出后失败保持关闭，重启不自动恢复授权。')
    xs=[156,514,914,1284]
    for x,label,width in [(xs[0],'受信 Launcher',248),(xs[1],'MD / Publisher',288),
                          (xs[2],'执行 / 策略消费者',300),(xs[3],'持久对象',216)]:
        d.rect(x-width/2,132,width,62)
        d.text(x,171,label,22,anchor='middle',weight=True)
        d.parts.append(f'<path d="M {x} 194 V 888" stroke="{BORDER}" stroke-width="1.5" stroke-dasharray="5 6"/>')
    def msg(a,b,y,label,reply=False):
        d.arrow([(xs[a],y),(xs[b],y)],muted=reply,dash=reply)
        d.text((xs[a]+xs[b])/2,y-12,label,18,anchor='middle',color=MUTED if reply else INK)
    msg(0,1,240,'完整 Plan：当前身份 / 目标集合 / 预算')
    d.rect(348,264,332,78,'#eaf4f4','#c8e0e1',10)
    d.text(514,294,'新 owner 经 WSS / REST 预热',18,anchor='middle',color=TEAL,weight=True)
    d.text(514,323,'旧 owner、连接和本地编号保留',17,anchor='middle',color=MUTED)
    msg(1,2,380,'Arm：精确旧配置 + 新 bank 模板')
    msg(2,3,422,'同步决定对象 / WAL')
    msg(2,1,464,'Armed：实际 PID 与批准摘要',True)
    msg(1,2,510,'暂停 JOIN，发送绑定操作的 QUIESCE')
    msg(2,1,554,'真实 cursor QUIESCED + 旧消费水位',True)
    d.rect(348,579,332,78,'#e8eef7','#d8e2ef',10)
    d.text(514,609,'封口旧 bank，提交完整新基线',18,anchor='middle',color=BLUE,weight=True)
    d.text(514,638,'同步 MD effective 后开放新 JOIN',17,anchor='middle',color=MUTED)
    msg(1,2,696,'Commit：同一精确新描述 / 重新 JOIN')
    msg(2,3,738,'实际激活；保存输入 / effective')
    msg(2,1,780,'Activated：新描述 + 实际消费水位',True)
    msg(1,0,834,'真实 Active 与全部收据核验完成',True)
    d.rect(48,910,1344,82,'#fffaf1','#e7d4b5',10)
    d.text(72,943,'退出前失败：丢弃候选，旧流继续。退出后失败：保持关闭，保留旧映射与历史证据。',19,color=AMBER,weight=True)
    d.text(72,973,'消费者先完成自己的持久边界才恢复回调；新增行情不增加账户资金或交易权限。进程重启后重新资格化。',18,color=MUTED)
    d.finish()

def rebuild_capture():
    d=Diagram('rebuild-capture.svg', 1080, 'Tickwire · 声明重建的输入采集',
              '先采集真实输入并封口，再固定 Plans 与发布操作；采集进程尚未创建用户策略。',
              'Host持有真实采集子进程并登记MD准入。CaptureOnly完成真实JOIN，接收执行端账户完整前缀，建立种子；仅用SDK固定观察器采集行情和账户记录。Host发起绑定合同和账户位置的封口请求，worker同步合同、种子、输入、WindowSeal和完成记录。Host重算整个窗口并核对实际PID、会话、报告游标与期限。同一子进程保持无READY和无交易输出，等待已批准的目标安装请求。')
    xs=[168,510,862,1252]
    for x,label,w in [(xs[0],'Host / 执行端',260),(xs[1],'MD 进程',248),
                      (xs[2],'CaptureOnly 子进程',304),(xs[3],'持久对象',236)]:
        d.rect(x-w/2,132,w,62)
        d.text(x,171,label,22,anchor='middle',weight=True)
        d.parts.append(f'<path d="M {x} 194 V 884" stroke="{BORDER}" stroke-width="1.5" stroke-dasharray="5 6"/>')
    def msg(a,b,y,label,reply=False):
        d.arrow([(xs[a],y),(xs[b],y)],muted=reply,dash=reply)
        d.text((xs[a]+xs[b])/2,y-12,label,18,anchor='middle',color=MUTED if reply else INK)
    msg(0,2,238,'启动固定 binary；Registry 持有实际 Child')
    msg(0,1,288,'登记实际 PID / 身份')
    msg(2,1,342,'认证 JOIN / 快照 + 增量追平')
    msg(0,2,396,'认证报告通道：基线 + 完整账户前缀')
    d.rect(690,416,344,74,'#eaf4f4','#c8e0e1',10)
    d.text(862,446,'以真实市场与账户视图建立种子',18,anchor='middle',color=TEAL,weight=True)
    d.text(862,475,'固定观察器 · 不执行用户工厂',17,anchor='middle',color=MUTED)
    msg(1,2,538,'实际行情记录 / 观察次序')
    msg(0,2,586,'有序账户记录')
    msg(0,2,640,'CaptureSeal：Contract / 账户位置 / 挑战 / 期限')
    msg(2,3,694,'合同、种子、输入、Seal')
    msg(2,3,740,'同步完成记录')
    msg(2,0,792,'认证回执：对象摘要 / 实际 PID / 报告游标',True)
    d.rect(48,816,1060,64,'#e8eef7','#d8e2ef',10)
    d.text(72,843,'Host 重读闭包并逐行重算窗口，核对当前子进程、会话和期限。',18,color=BLUE,weight=True)
    d.text(72,868,'封口成功保留事实；业务 READY 与交易输出仍关闭。',17,color=MUTED)
    d.rect(48,920,1344,130,'#fffaf1','#e7d4b5',10)
    d.text(72,954,'封口之后：目标安装与 schema4 发布',21,color=AMBER,weight=True)
    d.text(72,990,'固定窗口 → 全成员 Plans / 签名 Operation → 真实工厂重建 / 持久安装 → 独立回滚探测',18,color=MUTED)
    d.text(72,1025,'安装后取得真实 READY，再经协调器采纳、Commit / Release，完成窗口后的连续接续。',18,color=MUTED)
    d.finish()


def rebuild_installation():
    d=Diagram('rebuild-installation.svg', 1060, 'Tickwire · 声明重建与同进程安装',
              '固定输入窗口先于发布操作；预热后的同一策略对象从窗口末端继续运行。',
              '先用CaptureOnly采集真实窗口C，再固定Plans并签名Operation。执行端冻结旧策略准入并核对完整账户前缀，向原子进程发起安装。真实工厂执行一次预热，持久保存原起点、warm输入、检查点与HEAD，建立真实意图通道，同一PID消费C之后的输入。独立旧版本探针完成后，kind10同步回退闭包，新READY绑定安装和回退根。协调器采纳固定七根，再Commit和Release并使旧进程正常退出。')
    d.text(48,139,'01  固定发布输入',17,color=BLUE,weight=True)
    boxes=[(48,'采集并封口 C',['真实 MD JOIN + 账户报告','固定观察器 · 输出关闭']),
           (392,'Plans / Operation',['窗口、成员与配置根固定','封口完成后签名']),
           (736,'执行端准备',['冻结旧策略的新准入','核对真实账户 WAL / 动作']),
           (1080,'kind9 安装请求',['认证原 report 会话','绑定同一 PID 与窗口'])]
    for x,title,lines in boxes:
        d.box(x,161,312,130,title,lines)
    for left in [360,704,1048]:d.arrow([(left,226),(left+32,226)])
    d.rect(48,332,1344,332,'#eaf4f4','#b8d9d9',16)
    d.text(72,367,'02  同一个受监督的策略进程',22,color=TEAL,weight=True)
    d.box(80,397,348,176,'真实工厂执行一次',[
        '种子视图 + 完整 warm 行',
        '核对已接纳动作仍被消费',
        '保留预热后的策略对象'],accent=TEAL)
    d.box(548,397,348,176,'持久安装原输入链',[
        '起点 → genesis → warm commit',
        '同步 checkpoint / HEAD',
        '序号保留到 C，不重新归零'],accent=TEAL)
    d.box(1016,397,348,176,'接入持续 worker',[
        '原 MD / report 游标继续使用',
        '认证真实 intent 通道',
        '消费 C 之后的行情与账户输入'],accent=TEAL)
    d.arrow([(428,482),(548,482)])
    d.arrow([(896,482),(1016,482)])
    d.text(488,459,'同一对象',17,anchor='middle',color=TEAL)
    d.text(956,459,'同一 PID',17,anchor='middle',color=TEAL)
    d.text(72,614,'首次 READY 来自已同步的真实业务状态；后续输入继续持久记录并推进消费证明。',19,color=TEAL)
    d.text(72,645,'等待安装受会话期限与共享 ring 容量限制；缺口或容量失败会关闭本次尝试。',17,color=MUTED)
    d.arrow([(1236,291),(1236,318),(500,318),(500,380),(254,380),(254,397)],muted=True)
    d.text(48,713,'03  回退资格与发布',17,color=BLUE,weight=True)
    for x,title,lines in [(48,'独立旧版本探针',['真实旧 binary / 工厂执行','持久对象 + 成功退出']),
                          (392,'kind10 保存回退图',['原 worker 同步完成闭包','新 READY 绑定两类根']),
                          (736,'协调器采纳',['固定七根与真实 READY','Prepare / Commit 持久记录']),
                          (1080,'Release / 正常退出',['按账户风险域移交管理权','旧进程真实退订并退出'])]:
        d.box(x,737,312,140,title,lines,accent=TEAL if x<736 else BLUE)
    for left in [360,704,1048]:d.arrow([(left,808),(left+32,808)])
    d.rect(48,920,1344,102,'#e8eef7','#d8e2ef',12)
    d.text(72,956,'恢复依据',20,color=BLUE,weight=True)
    d.text(220,956,'HandoverSeal v4：原安装根 + 独立回退根 + 完整窗口后输入',20,color=BLUE)
    d.text(72,992,'安装与 READY 记录已完成的事实；实际交易仍经过执行端的账户、风控、授权和持久提交门禁。',18,color=MUTED)
    d.finish()


def config_planning():
    d=Diagram('config-planning.svg', 860, 'Tickwire · 配置计划与生效边界',
              '计划内容准备经过两次真实安全读；配置成功还需要持久提交和完整目标的双收据。',
              '已认证主体首先经唯一Raft owner的ReadIndex取得当前提交基线与角色。后台任务在固定资料包和信任上下文中检查候选、资格及完整新旧投影，生成确定性计划。第二次ReadIndex重新核对基线、权限、来源和期限后返回内部草案。运行实例签名观测按签发者和主机进程范围验证。当前登记头、实际采集、计划持久提交和运行时应用仍待接入，草案不授予发布权限。')
    d.text(48,141,'已实现 · 内部计划内容准备',18,color=TEAL,weight=True)
    steps=[(48,'01 · 提交基线',['mTLS 主体 + 当前角色','ReadIndex / 业务扫描','固定 revision + hash + ETag']),
           (392,'02 · 后台检查',['固定资料包与信任来源','候选、资格、旧新投影','完整目标集合 / 逐目标动作']),
           (736,'03 · 完成复核',['新的 ReadIndex','再次核对基线、权限与来源','期限内返回完整内部草案']),
           (1080,'04 · 计划草案',['固定内容摘要与交付闭包','不变投影也接收新版本','保留待核验项'])]
    for x,title,lines in steps:d.box(x,166,312,174,title,lines,accent=TEAL)
    for left in [360,704,1048]:d.arrow([(left,253),(left+32,253)])
    d.box(48,386,656,146,'运行实例观测验签',[
        '签发者限定 host / process；核对实例、boot 与当前投影',
        '完整基线目标集合、来源水位、期限与构建身份',
        '结果是签名观测；实际采集者和持久水位仍待接入'],accent=TEAL)
    d.box(736,386,656,146,'闭包与当前事实',[
        '候选资料核验与旧新投影来自同一有界后台 owner',
        '资源快照用于容量计算；当前登记头另行核对',
        '来源替换、证书撤销或基线变化使进行中的草案失效'],accent=BLUE)
    d.text(48,590,'待接入 · 发布与完整目标生效',18,color=AMBER,weight=True)
    later=[(48,'当前资格',['实际实例 / 资源登记头','运行时能力与继任条件']),
           (392,'多数持久提交',['计划 + revision + operation','caller-scoped 幂等结果']),
           (736,'逐目标应用',['desired 文件同步','实际运行时应用并持久记录']),
           (1080,'完整双收据',['effective 文件同步','全目标确认 → Succeeded'])]
    for x,title,lines in later:d.box(x,615,312,140,title,lines,accent=AMBER,fill='#fffaf1')
    for left in [360,704,1048]:d.arrow([(left,685),(left+32,685)],muted=True,dash=True)
    d.text(48,812,'当前 HTTP preview / plans / commits 仍拒绝未实现请求；内部草案不等于 Ready 或配置已生效。',18,color=MUTED)
    d.finish()


if __name__ == '__main__':
    architecture()
    order_flow()
    handover()
    market_expansion()
    rebuild_capture()
    rebuild_installation()
    config_planning()

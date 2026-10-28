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

if __name__ == '__main__':
    architecture()
    order_flow()
    handover()

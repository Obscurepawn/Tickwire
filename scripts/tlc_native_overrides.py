#!/usr/bin/env python3
"""Emit zero-dependency JVM bytecode overrides (`@TLAPlusOperator`) for TLC bottlenecks.

Preserves frozen `.tla` source files and SHA-256 digests byte-for-byte while replacing
interpreted TLA+ hot loops with canonical TLC `Value` constructors:
  - `L0_Calibration`: cached `Word` function-space descriptor (`[1..10 -> 0..999]`) plus
    10-limb base-1000 `Word` arithmetic (`Lt`, `Le`, `MinW`, `MaxW`, `Add`, `Sub`,
    `SatSub`, `Shift`, `Drop`, `CeilDrop`, `Half`, `Mid`, `Quantized`, `FractionCeil`,
    `Up`, `Down`, `Scaled`, `DriftPart`, `AdvanceTruth`).
  - `L0_DepthSync`: memoized `Events(t, g)` set enumeration keyed by `(MaxId, t, g)` plus
    native `Classify(b, e)`, `Apply(b, e)`, and iterative `Replay(b, queue, applied)`
    keyed by `-Dtickwire.tlc.policy=Spot|UsdM`.
"""
from __future__ import annotations

from pathlib import Path
import struct


VAL_CLS = "tlc2/value/impl/Value"
INT_CLS = "tlc2/value/impl/IntValue"
BOOL_CLS = "tlc2/value/impl/BoolValue"
STR_CLS = "tlc2/value/impl/StringValue"
INTV_CLS = "tlc2/value/impl/IntervalValue"
FCN_CLS = "tlc2/value/impl/FcnRcdValue"
SETFCN_CLS = "tlc2/value/impl/SetOfFcnsValue"
TUP_CLS = "tlc2/value/impl/TupleValue"
RCD_CLS = "tlc2/value/impl/RecordValue"
SET_CLS = "tlc2/value/impl/SetEnumValue"
USTR_CLS = "util/UniqueString"

L_VAL = f"L{VAL_CLS};"
ARR_VAL = f"[{L_VAL}"
OP_ANNOT = "Ltlc2/overrides/TLAPlusOperator;"


class ConstantPool:
    def __init__(self) -> None:
        self._entries: list[bytes] = []
        self._map: dict[tuple, int] = {}

    def _add(self, key: tuple, raw: bytes) -> int:
        idx = self._map.get(key)
        if idx is not None:
            return idx
        idx = len(self._entries) + 1
        self._entries.append(raw)
        self._map[key] = idx
        return idx

    def utf8(self, s: str) -> int:
        b = s.encode("utf-8")
        return self._add(("utf8", s), struct.pack(">BH", 1, len(b)) + b)

    def integer(self, v: int) -> int:
        return self._add(("int", v), struct.pack(">Bi", 3, v))

    def cls(self, internal_name: str) -> int:
        u = self.utf8(internal_name)
        return self._add(("cls", internal_name), struct.pack(">BH", 7, u))

    def string(self, s: str) -> int:
        u = self.utf8(s)
        return self._add(("str", s), struct.pack(">BH", 8, u))

    def nat(self, name: str, desc: str) -> int:
        n = self.utf8(name)
        d = self.utf8(desc)
        return self._add(("nat", name, desc), struct.pack(">BHH", 12, n, d))

    def fieldref(self, cls_name: str, name: str, desc: str) -> int:
        c = self.cls(cls_name)
        nt = self.nat(name, desc)
        return self._add(("field", cls_name, name, desc), struct.pack(">BHH", 9, c, nt))

    def methodref(self, cls_name: str, name: str, desc: str) -> int:
        c = self.cls(cls_name)
        nt = self.nat(name, desc)
        return self._add(("method", cls_name, name, desc), struct.pack(">BHH", 10, c, nt))

    def encode(self) -> bytes:
        return struct.pack(">H", len(self._entries) + 1) + b"".join(self._entries)


class CodeAsm:
    def __init__(self, cp: ConstantPool) -> None:
        self.cp = cp
        self.buf = bytearray()
        self.labels: dict[str, int] = {}
        self.fixups: list[tuple[int, int, str]] = []

    def label(self, name: str) -> None:
        self.labels[name] = len(self.buf)

    def emit(self, *opcodes: int) -> None:
        self.buf.extend(opcodes)

    def iconst(self, v: int) -> None:
        if -1 <= v <= 5:
            self.emit(0x03 + v)
        elif -128 <= v <= 127:
            self.buf.extend(struct.pack(">Bb", 0x10, v))
        elif -32768 <= v <= 32767:
            self.buf.extend(struct.pack(">Bh", 0x11, v))
        else:
            idx = self.cp.integer(v)
            self.buf.extend(struct.pack(">BH", 0x13, idx))

    def ldc_str(self, s: str) -> None:
        self.buf.extend(struct.pack(">BH", 0x13, self.cp.string(s)))

    def ldc_cls(self, internal_name: str) -> None:
        self.buf.extend(struct.pack(">BH", 0x13, self.cp.cls(internal_name)))

    def iload(self, slot: int) -> None:
        self.emit(0x15, slot)

    def aload(self, slot: int) -> None:
        self.emit(0x19, slot)

    def istore(self, slot: int) -> None:
        self.emit(0x36, slot)

    def astore(self, slot: int) -> None:
        self.emit(0x3A, slot)

    def iinc(self, slot: int, delta: int) -> None:
        if -128 <= delta <= 127 and slot < 256:
            self.buf.extend(struct.pack(">BBb", 0x84, slot, delta))
        else:
            self.buf.extend(struct.pack(">BBHh", 0xC4, 0x84, slot, delta))

    def aaload(self) -> None:
        self.emit(0x32)

    def aastore(self) -> None:
        self.emit(0x53)

    def dup(self) -> None:
        self.emit(0x59)

    def pop(self) -> None:
        self.emit(0x57)

    def iadd(self) -> None:
        self.emit(0x60)

    def isub(self) -> None:
        self.emit(0x64)

    def imul(self) -> None:
        self.emit(0x68)

    def ishl(self) -> None:
        self.emit(0x78)

    def ishr(self) -> None:
        self.emit(0x7A)

    def iand(self) -> None:
        self.emit(0x7E)

    def ior(self) -> None:
        self.emit(0x80)

    def ireturn(self) -> None:
        self.emit(0xAC)

    def areturn(self) -> None:
        self.emit(0xB0)

    def vreturn(self) -> None:
        self.emit(0xB1)

    def _branch(self, op: int, target: str) -> None:
        op_pos = len(self.buf)
        self.buf.extend(b"\x00\x00\x00")
        self.buf[op_pos] = op
        self.fixups.append((op_pos + 1, op_pos, target))

    def ifeq(self, target: str) -> None:
        self._branch(0x99, target)

    def ifne(self, target: str) -> None:
        self._branch(0x9A, target)

    def iflt(self, target: str) -> None:
        self._branch(0x9B, target)

    def ifge(self, target: str) -> None:
        self._branch(0x9C, target)

    def ifgt(self, target: str) -> None:
        self._branch(0x9D, target)

    def ifle(self, target: str) -> None:
        self._branch(0x9E, target)

    def if_icmpeq(self, target: str) -> None:
        self._branch(0x9F, target)

    def if_icmpne(self, target: str) -> None:
        self._branch(0xA0, target)

    def if_icmplt(self, target: str) -> None:
        self._branch(0xA1, target)

    def if_icmpge(self, target: str) -> None:
        self._branch(0xA2, target)

    def if_icmpgt(self, target: str) -> None:
        self._branch(0xA3, target)

    def if_icmple(self, target: str) -> None:
        self._branch(0xA4, target)

    def if_acmpeq(self, target: str) -> None:
        self._branch(0xA5, target)

    def if_acmpne(self, target: str) -> None:
        self._branch(0xA6, target)

    def ifnull(self, target: str) -> None:
        self._branch(0xC6, target)

    def ifnonnull(self, target: str) -> None:
        self._branch(0xC7, target)

    def goto(self, target: str) -> None:
        self._branch(0xA7, target)

    def getstatic(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB2, self.cp.fieldref(cls_name, name, desc)))

    def putstatic(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB3, self.cp.fieldref(cls_name, name, desc)))

    def getfield(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB4, self.cp.fieldref(cls_name, name, desc)))

    def invokevirtual(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB6, self.cp.methodref(cls_name, name, desc)))

    def invokespecial(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB7, self.cp.methodref(cls_name, name, desc)))

    def invokestatic(self, cls_name: str, name: str, desc: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xB8, self.cp.methodref(cls_name, name, desc)))

    def new(self, cls_name: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xBB, self.cp.cls(cls_name)))

    def anewarray(self, cls_name: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xBD, self.cp.cls(cls_name)))

    def checkcast(self, cls_name: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xC0, self.cp.cls(cls_name)))

    def instanceof(self, cls_name: str) -> None:
        self.buf.extend(struct.pack(">BH", 0xC1, self.cp.cls(cls_name)))

    def arraylength(self) -> None:
        self.emit(0xBE)

    def finish(self, max_stack: int = 12, max_locals: int = 16) -> bytes:
        for patch_pos, op_pos, target in self.fixups:
            rel = self.labels[target] - op_pos
            struct.pack_into(">h", self.buf, patch_pos, rel)
        code_bytes = bytes(self.buf)
        code_attr_name = self.cp.utf8("Code")
        body = struct.pack(">HHI", max_stack, max_locals, len(code_bytes)) + code_bytes + struct.pack(">HH", 0, 0)
        return struct.pack(">HI", code_attr_name, len(body)) + body


class ClassFileBuilder:
    """Java 5 (`49.0`) class file writer supporting annotations without `StackMapTable`."""

    def __init__(self, this_cls: str, super_cls: str = "java/lang/Object", interfaces: list[str] | None = None) -> None:
        self.cp = ConstantPool()
        self.this_idx = self.cp.cls(this_cls)
        self.super_idx = self.cp.cls(super_cls)
        self.iface_idxs = [self.cp.cls(i) for i in (interfaces or [])]
        self.fields: list[bytes] = []
        self.methods: list[bytes] = []

    def add_field(self, access: int, name: str, desc: str) -> None:
        self.fields.append(struct.pack(">HHHH", access, self.cp.utf8(name), self.cp.utf8(desc), 0))

    def _op_annot_attr(self, identifier: str, module: str) -> bytes:
        attr_name = self.cp.utf8("RuntimeVisibleAnnotations")
        type_idx = self.cp.utf8(OP_ANNOT)
        k_id = self.cp.utf8("identifier")
        v_id = self.cp.utf8(identifier)
        k_mod = self.cp.utf8("module")
        v_mod = self.cp.utf8(module)
        k_warn = self.cp.utf8("warn")
        v_false = self.cp.integer(0)
        annot = (
            struct.pack(">HH", type_idx, 3)
            + struct.pack(">HBH", k_id, ord("s"), v_id)
            + struct.pack(">HBH", k_mod, ord("s"), v_mod)
            + struct.pack(">HBH", k_warn, ord("Z"), v_false)
        )
        payload = struct.pack(">H", 1) + annot
        return struct.pack(">HI", attr_name, len(payload)) + payload

    def add_method(
        self,
        access: int,
        name: str,
        desc: str,
        code_attr: bytes,
        tla_op: tuple[str, str] | None = None,
    ) -> None:
        attrs = [code_attr]
        if tla_op is not None:
            attrs.append(self._op_annot_attr(tla_op[0], tla_op[1]))
        self.methods.append(
            struct.pack(">HHHH", access, self.cp.utf8(name), self.cp.utf8(desc), len(attrs))
            + b"".join(attrs)
        )

    def build(self) -> bytes:
        header = struct.pack(">IHH", 0xCAFEBABE, 0, 49)
        cp_bytes = self.cp.encode()
        cls_hdr = struct.pack(">HHHH", 0x0021, self.this_idx, self.super_idx, len(self.iface_idxs))
        ifaces = b"".join(struct.pack(">H", idx) for idx in self.iface_idxs)
        f_sec = struct.pack(">H", len(self.fields)) + b"".join(self.fields)
        m_sec = struct.pack(">H", len(self.methods)) + b"".join(self.methods)
        a_sec = struct.pack(">H", 0)
        return header + cp_bytes + cls_hdr + ifaces + f_sec + m_sec + a_sec


def _build_calib_overrides() -> bytes:
    ME = "tlc2/overrides/L0_CalibrationOverrides"
    MOD = "L0_Calibration"
    cb = ClassFileBuilder(ME)

    cb.add_field(0x0019, "DOM10", f"L{INTV_CLS};")
    cb.add_field(0x0019, "DIGITS", f"[L{INT_CLS};")
    cb.add_field(0x0019, "ZERO", L_VAL)
    cb.add_field(0x0019, "ONE", L_VAL)
    cb.add_field(0x0019, "WORD_SET", L_VAL)

    # Default <init>()V
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokespecial("java/lang/Object", "<init>", "()V")
    c.vreturn()
    cb.add_method(0x0001, "<init>", "()V", c.finish(2, 1))

    # <clinit>()V: initialize DOM10, DIGITS[0..999], ZERO, ONE
    c = CodeAsm(cb.cp)
    c.new(INTV_CLS)
    c.dup()
    c.iconst(1)
    c.iconst(10)
    c.invokespecial(INTV_CLS, "<init>", "(II)V")
    c.putstatic(ME, "DOM10", f"L{INTV_CLS};")

    c.iconst(1000)
    c.anewarray(INT_CLS)
    c.astore(0)
    c.iconst(0)
    c.istore(1)
    c.label("dig_loop")
    c.iload(1)
    c.iconst(1000)
    c.if_icmpge("dig_done")
    c.aload(0)
    c.iload(1)
    c.iload(1)
    c.invokestatic(INT_CLS, "gen", f"(I)L{INT_CLS};")
    c.aastore()
    c.iinc(1, 1)
    c.goto("dig_loop")
    c.label("dig_done")
    c.aload(0)
    c.putstatic(ME, "DIGITS", f"[L{INT_CLS};")

    # ZERO = FcnRcdValue(DOM10, [DIGITS[0]*10])
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(2)
    c.iconst(0)
    c.istore(1)
    c.label("z_loop")
    c.iload(1)
    c.iconst(10)
    c.if_icmpge("z_done")
    c.aload(2)
    c.iload(1)
    c.aload(0)
    c.iconst(0)
    c.aaload()
    c.aastore()
    c.iinc(1, 1)
    c.goto("z_loop")
    c.label("z_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(2)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.putstatic(ME, "ZERO", L_VAL)

    # ONE = FcnRcdValue(DOM10, [DIGITS[1], DIGITS[0]*9])
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(3)
    c.aload(3)
    c.iconst(0)
    c.aload(0)
    c.iconst(1)
    c.aaload()
    c.aastore()
    c.iconst(1)
    c.istore(1)
    c.label("o_loop")
    c.iload(1)
    c.iconst(10)
    c.if_icmpge("o_done")
    c.aload(3)
    c.iload(1)
    c.aload(0)
    c.iconst(0)
    c.aaload()
    c.aastore()
    c.iinc(1, 1)
    c.goto("o_loop")
    c.label("o_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(3)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.putstatic(ME, "ONE", L_VAL)

    # WORD_SET = SetOfFcnsValue(DOM10, IntervalValue(0, 999))
    c.new(SETFCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.new(INTV_CLS)
    c.dup()
    c.iconst(0)
    c.iconst(999)
    c.invokespecial(INTV_CLS, "<init>", "(II)V")
    c.invokespecial(SETFCN_CLS, "<init>", f"({L_VAL}{L_VAL})V")
    c.putstatic(ME, "WORD_SET", L_VAL)
    c.vreturn()
    cb.add_method(0x0008, "<clinit>", "()V", c.finish(8, 4))

    # Word(): cached SetOfFcnsValue([1..10 -> 0..999]) eliminating 9 allocs/state in TypeOK
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "WORD_SET", L_VAL)
    c.areturn()
    cb.add_method(0x0009, "Word", f"(){L_VAL}", c.finish(2, 0), ("Word", MOD))

    # private static Value[] elems(Value v): handles FcnRcdValue & TupleValue in O(1)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.instanceof(FCN_CLS)
    c.ifeq("not_fcn")
    c.aload(0)
    c.checkcast(FCN_CLS)
    c.getfield(FCN_CLS, "values", ARR_VAL)
    c.areturn()
    c.label("not_fcn")
    c.aload(0)
    c.instanceof(TUP_CLS)
    c.ifeq("fallback_fcn")
    c.aload(0)
    c.checkcast(TUP_CLS)
    c.getfield(TUP_CLS, "elems", ARR_VAL)
    c.areturn()
    c.label("fallback_fcn")
    c.aload(0)
    c.invokevirtual(VAL_CLS, "toFcnRcd", f"(){L_VAL}")
    c.checkcast(FCN_CLS)
    c.getfield(FCN_CLS, "values", ARR_VAL)
    c.areturn()
    cb.add_method(0x000A, "elems", f"({L_VAL}){ARR_VAL}", c.finish(3, 1))

    # private static int cmp(Value a, Value b): <0 if a<b, 0 if a==b, >0 if a>b
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(2)
    c.aload(1)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(3)
    c.iconst(9)
    c.istore(4)
    c.label("cmp_loop")
    c.iload(4)
    c.iflt("cmp_eq")
    c.aload(2)
    c.iload(4)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.aload(3)
    c.iload(4)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.isub()
    c.dup()
    c.ifeq("cmp_next")
    c.ireturn()
    c.label("cmp_next")
    c.pop()
    c.iinc(4, -1)
    c.goto("cmp_loop")
    c.label("cmp_eq")
    c.iconst(0)
    c.ireturn()
    cb.add_method(0x000A, "cmp", f"({L_VAL}{L_VAL})I", c.finish(4, 5))

    # Lt(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "cmp", f"({L_VAL}{L_VAL})I")
    c.ifge("lt_false")
    c.getstatic(BOOL_CLS, "ValTrue", f"L{BOOL_CLS};")
    c.areturn()
    c.label("lt_false")
    c.getstatic(BOOL_CLS, "ValFalse", f"L{BOOL_CLS};")
    c.areturn()
    cb.add_method(0x0009, "Lt", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(3, 2), ("Lt", MOD))

    # Le(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "cmp", f"({L_VAL}{L_VAL})I")
    c.ifgt("le_false")
    c.getstatic(BOOL_CLS, "ValTrue", f"L{BOOL_CLS};")
    c.areturn()
    c.label("le_false")
    c.getstatic(BOOL_CLS, "ValFalse", f"L{BOOL_CLS};")
    c.areturn()
    cb.add_method(0x0009, "Le", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(3, 2), ("Le", MOD))

    # MinW(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "cmp", f"({L_VAL}{L_VAL})I")
    c.ifgt("min_b")
    c.aload(0)
    c.areturn()
    c.label("min_b")
    c.aload(1)
    c.areturn()
    cb.add_method(0x0009, "MinW", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(3, 2), ("MinW", MOD))

    # MaxW(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "cmp", f"({L_VAL}{L_VAL})I")
    c.ifgt("max_a")
    c.aload(1)
    c.areturn()
    c.label("max_a")
    c.aload(0)
    c.areturn()
    cb.add_method(0x0009, "MaxW", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(3, 2), ("MaxW", MOD))

    # Add(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(2)
    c.aload(1)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(3)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.astore(4)
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(5)
    c.iconst(0)
    c.istore(6)  # carry
    c.iconst(0)
    c.istore(7)  # idx
    c.label("add_loop")
    c.iload(7)
    c.iconst(10)
    c.if_icmpge("add_done")
    c.aload(2)
    c.iload(7)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.aload(3)
    c.iload(7)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.iadd()
    c.iload(6)
    c.iadd()
    c.istore(8)  # sum
    c.iload(8)
    c.iconst(1000)
    c.if_icmplt("add_no_carry")
    c.iinc(8, -1000)
    c.iconst(1)
    c.istore(6)
    c.goto("add_store")
    c.label("add_no_carry")
    c.iconst(0)
    c.istore(6)
    c.label("add_store")
    c.aload(5)
    c.iload(7)
    c.aload(4)
    c.iload(8)
    c.aaload()
    c.aastore()
    c.iinc(7, 1)
    c.goto("add_loop")
    c.label("add_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(5)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.areturn()
    cb.add_method(0x0009, "Add", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(6, 9), ("Add", MOD))

    # Sub(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(2)
    c.aload(1)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(3)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.astore(4)
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(5)
    c.iconst(0)
    c.istore(6)  # borrow
    c.iconst(0)
    c.istore(7)  # idx
    c.label("sub_loop")
    c.iload(7)
    c.iconst(10)
    c.if_icmpge("sub_done")
    c.aload(2)
    c.iload(7)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.aload(3)
    c.iload(7)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.isub()
    c.iload(6)
    c.isub()
    c.istore(8)  # diff
    c.iload(8)
    c.ifge("sub_no_borrow")
    c.iload(8)
    c.iconst(1000)
    c.iadd()
    c.istore(8)
    c.iconst(1)
    c.istore(6)
    c.goto("sub_store")
    c.label("sub_no_borrow")
    c.iconst(0)
    c.istore(6)
    c.label("sub_store")
    c.aload(5)
    c.iload(7)
    c.aload(4)
    c.iload(8)
    c.aaload()
    c.aastore()
    c.iinc(7, 1)
    c.goto("sub_loop")
    c.label("sub_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(5)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.areturn()
    cb.add_method(0x0009, "Sub", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(6, 9), ("Sub", MOD))

    # SatSub(a, b)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "cmp", f"({L_VAL}{L_VAL})I")
    c.ifge("satsub_pos")
    c.getstatic(ME, "ZERO", L_VAL)
    c.areturn()
    c.label("satsub_pos")
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "Sub", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "SatSub", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 2), ("SatSub", MOD))

    # Shift(a, kv): [i \in 1..10 |-> IF i>k THEN a[i-k] ELSE 0]
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(2)
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(3)  # k
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.iconst(0)
    c.aaload()
    c.astore(4)  # IntValue(0)
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(5)
    c.iconst(0)
    c.istore(6)  # idx
    c.label("sh_loop")
    c.iload(6)
    c.iconst(10)
    c.if_icmpge("sh_done")
    c.iload(6)
    c.iload(3)
    c.if_icmplt("sh_zero")
    c.iload(6)
    c.iload(3)
    c.isub()
    c.istore(7)
    c.iload(7)
    c.iconst(10)
    c.if_icmpge("sh_zero")
    c.aload(5)
    c.iload(6)
    c.aload(2)
    c.iload(7)
    c.aaload()
    c.aastore()
    c.goto("sh_next")
    c.label("sh_zero")
    c.aload(5)
    c.iload(6)
    c.aload(4)
    c.aastore()
    c.label("sh_next")
    c.iinc(6, 1)
    c.goto("sh_loop")
    c.label("sh_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(5)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.areturn()
    cb.add_method(0x0009, "Shift", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(6, 8), ("Shift", MOD))

    # Drop(a, kv): [i \in 1..10 |-> IF i+k<=10 THEN a[i+k] ELSE 0]
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(2)
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(3)  # k
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.iconst(0)
    c.aaload()
    c.astore(4)  # IntValue(0)
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(5)
    c.iconst(0)
    c.istore(6)  # idx
    c.label("dr_loop")
    c.iload(6)
    c.iconst(10)
    c.if_icmpge("dr_done")
    c.iload(6)
    c.iload(3)
    c.iadd()
    c.istore(7)
    c.iload(7)
    c.iflt("dr_zero")
    c.iload(7)
    c.iconst(10)
    c.if_icmpge("dr_zero")
    c.aload(5)
    c.iload(6)
    c.aload(2)
    c.iload(7)
    c.aaload()
    c.aastore()
    c.goto("dr_next")
    c.label("dr_zero")
    c.aload(5)
    c.iload(6)
    c.aload(4)
    c.aastore()
    c.label("dr_next")
    c.iinc(6, 1)
    c.goto("dr_loop")
    c.label("dr_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(5)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.areturn()
    cb.add_method(0x0009, "Drop", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(6, 8), ("Drop", MOD))

    # CeilDrop(a, kv): Add(Drop(a, kv), IF \E i \in 1..k: a[i]#0 THEN One ELSE Zero)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "Drop", f"({L_VAL}{L_VAL}){L_VAL}")
    c.astore(2)  # dropped
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(3)
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(4)  # k
    c.iload(4)
    c.iconst(10)
    c.if_icmple("cd_k_ok")
    c.iconst(10)
    c.istore(4)
    c.label("cd_k_ok")
    c.iconst(0)
    c.istore(5)
    c.label("cd_loop")
    c.iload(5)
    c.iload(4)
    c.if_icmpge("cd_exact")
    c.aload(3)
    c.iload(5)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.ifne("cd_plus_one")
    c.iinc(5, 1)
    c.goto("cd_loop")
    c.label("cd_exact")
    c.aload(2)
    c.areturn()
    c.label("cd_plus_one")
    c.aload(2)
    c.getstatic(ME, "ONE", L_VAL)
    c.invokestatic(ME, "Add", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "CeilDrop", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 6), ("CeilDrop", MOD))

    # Half(a): [i \in 1..10 |-> (a[i] + (IF i<10 THEN 1000*(a[i+1]%2) ELSE 0)) \div 2]
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "elems", f"({L_VAL}){ARR_VAL}")
    c.astore(1)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.astore(2)
    c.iconst(10)
    c.anewarray(VAL_CLS)
    c.astore(3)
    c.iconst(0)
    c.istore(4)
    c.label("hf_loop")
    c.iload(4)
    c.iconst(10)
    c.if_icmpge("hf_done")
    c.aload(1)
    c.iload(4)
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(5)
    c.iload(4)
    c.iconst(9)
    c.if_icmpge("hf_store")
    c.aload(1)
    c.iload(4)
    c.iconst(1)
    c.iadd()
    c.aaload()
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.iconst(1)
    c.iand()
    c.ifeq("hf_store")
    c.iinc(5, 1000)
    c.label("hf_store")
    c.aload(3)
    c.iload(4)
    c.aload(2)
    c.iload(5)
    c.iconst(1)
    c.ishr()
    c.aaload()
    c.aastore()
    c.iinc(4, 1)
    c.goto("hf_loop")
    c.label("hf_done")
    c.new(FCN_CLS)
    c.dup()
    c.getstatic(ME, "DOM10", f"L{INTV_CLS};")
    c.aload(3)
    c.invokespecial(FCN_CLS, "<init>", f"(L{INTV_CLS};{ARR_VAL})V")
    c.areturn()
    cb.add_method(0x0009, "Half", f"({L_VAL}){L_VAL}", c.finish(6, 6), ("Half", MOD))

    # Mid(a, b): Add(a, Half(Sub(b, a)))
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.aload(0)
    c.invokestatic(ME, "Sub", f"({L_VAL}{L_VAL}){L_VAL}")
    c.invokestatic(ME, "Half", f"({L_VAL}){L_VAL}")
    c.invokestatic(ME, "Add", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Mid", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 2), ("Mid", MOD))

    # Quantized(a, k): Shift(Drop(a, k), k)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "Drop", f"({L_VAL}{L_VAL}){L_VAL}")
    c.aload(1)
    c.invokestatic(ME, "Shift", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Quantized", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 2), ("Quantized", MOD))

    # FractionCeil(a, d): IF d=0 THEN Zero ELSE CeilDrop(a, IF d=1 THEN 3 ELSE 1)
    c = CodeAsm(cb.cp)
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(2)
    c.iload(2)
    c.ifne("fc_nonzero")
    c.getstatic(ME, "ZERO", L_VAL)
    c.areturn()
    c.label("fc_nonzero")
    c.aload(0)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.iload(2)
    c.iconst(1)
    c.if_icmpne("fc_one")
    c.iconst(3)
    c.goto("fc_call")
    c.label("fc_one")
    c.iconst(1)
    c.label("fc_call")
    c.aaload()
    c.invokestatic(ME, "CeilDrop", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "FractionCeil", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 3), ("FractionCeil", MOD))

    # Up(a, d): Add(a, FractionCeil(a, d))
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "FractionCeil", f"({L_VAL}{L_VAL}){L_VAL}")
    c.invokestatic(ME, "Add", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Up", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 2), ("Up", MOD))

    # Down(a, d): Sub(a, FractionCeil(a, d))
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "FractionCeil", f"({L_VAL}{L_VAL}){L_VAL}")
    c.invokestatic(ME, "Sub", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Down", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 2), ("Down", MOD))

    # Scaled(a): Shift(a, 3)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.iconst(3)
    c.aaload()
    c.invokestatic(ME, "Shift", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Scaled", f"({L_VAL}){L_VAL}", c.finish(4, 1), ("Scaled", MOD))

    # DriftPart(a, d): IF d=0 THEN Zero ELSE Shift(a, IF d=1 THEN 0 ELSE 2)
    c = CodeAsm(cb.cp)
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(2)
    c.iload(2)
    c.ifne("dp_nonzero")
    c.getstatic(ME, "ZERO", L_VAL)
    c.areturn()
    c.label("dp_nonzero")
    c.aload(0)
    c.getstatic(ME, "DIGITS", f"[L{INT_CLS};")
    c.iload(2)
    c.iconst(1)
    c.if_icmpne("dp_two")
    c.iconst(0)
    c.goto("dp_call")
    c.label("dp_two")
    c.iconst(2)
    c.label("dp_call")
    c.aaload()
    c.invokestatic(ME, "Shift", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "DriftPart", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 3), ("DriftPart", MOD))

    # AdvanceTruth(a, d, rate)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokestatic(ME, "Scaled", f"({L_VAL}){L_VAL}")
    c.astore(3)
    c.aload(2)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(4)
    c.iload(4)
    c.ifne("at_ drifted")
    c.aload(3)
    c.areturn()
    c.label("at_ drifted")
    c.aload(0)
    c.aload(1)
    c.invokestatic(ME, "DriftPart", f"({L_VAL}{L_VAL}){L_VAL}")
    c.astore(5)
    c.iload(4)
    c.ifgt("at_plus")
    c.aload(3)
    c.aload(5)
    c.invokestatic(ME, "Sub", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    c.label("at_plus")
    c.aload(3)
    c.aload(5)
    c.invokestatic(ME, "Add", f"({L_VAL}{L_VAL}){L_VAL}")
    c.areturn()
    cb.add_method(
        0x0009,
        "AdvanceTruth",
        f"({L_VAL}{L_VAL}{L_VAL}){L_VAL}",
        c.finish(4, 6),
        ("AdvanceTruth", MOD),
    )

    return cb.build()


def _build_depth_sync_overrides() -> bytes:
    """Memoize `Events(t, g)` and compile `Classify`, `Apply`, and `Replay` for `L0_DepthSync`."""
    ME = "tlc2/overrides/L0_DepthSyncOverrides"
    MOD = "L0_DepthSync"
    L_RCD = f"L{RCD_CLS};"
    L_STR = f"L{STR_CLS};"
    L_USTR = f"L{USTR_CLS};"
    cb = ClassFileBuilder(ME)

    cb.add_field(0x004A, "READY", "I")
    cb.add_field(0x000A, "MAX_ID", "I")
    cb.add_field(0x000A, "IS_SPOT", "I")
    cb.add_field(0x000A, "CACHE", ARR_VAL)

    s_keys = [
        ("S_ID", "id"),
        ("S_STAMP", "stamp"),
        ("S_QTY", "qty"),
        ("S_PHASE", "phase"),
        ("S_GEN", "gen"),
        ("S_FIRST", "first"),
        ("S_LAST", "last"),
        ("S_PREV", "previous"),
        ("S_VALID", "valid"),
        ("S_STALE", "Stale"),
        ("S_ERROR", "Error"),
        ("S_APPLY", "Apply"),
        ("S_INVALID", "Invalid"),
        ("S_LIVE", "Live"),
    ]
    u_keys = [
        ("U_GEN", "gen"),
        ("U_ID", "id"),
        ("U_PHASE", "phase"),
        ("U_QTY", "qty"),
        ("U_STAMP", "stamp"),
        ("U_APPLIED", "applied"),
        ("U_BOOK", "book"),
        ("U_BRIDGING", "Bridging"),
        ("U_INVALID", "Invalid"),
    ]
    for fname, _ in s_keys:
        cb.add_field(0x000A, fname, L_STR)
    for fname, _ in u_keys:
        cb.add_field(0x000A, fname, L_USTR)

    # <init>()V
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokespecial("java/lang/Object", "<init>", "()V")
    c.vreturn()
    cb.add_method(0x0001, "<init>", "()V", c.finish(2, 1))

    # private static Value makeRec(int first, int last, int prev, int stamp, int gen, int qty, int valid)
    c = CodeAsm(cb.cp)
    field_names = ["first", "last", "previous", "stamp", "gen", "qty", "valid"]
    c.iconst(7)
    c.anewarray(USTR_CLS)
    c.astore(7)
    for idx, fname in enumerate(field_names):
        c.aload(7)
        c.iconst(idx)
        c.ldc_str(fname)
        c.invokestatic(USTR_CLS, "uniqueStringOf", f"(Ljava/lang/String;)L{USTR_CLS};")
        c.aastore()

    c.iconst(7)
    c.anewarray(VAL_CLS)
    c.astore(8)
    for idx in range(6):
        c.aload(8)
        c.iconst(idx)
        c.iload(idx)
        c.invokestatic(INT_CLS, "gen", f"(I)L{INT_CLS};")
        c.aastore()
    c.aload(8)
    c.iconst(6)
    c.iload(6)
    c.ifeq("rec_false")
    c.getstatic(BOOL_CLS, "ValTrue", f"L{BOOL_CLS};")
    c.goto("rec_store_bool")
    c.label("rec_false")
    c.getstatic(BOOL_CLS, "ValFalse", f"L{BOOL_CLS};")
    c.label("rec_store_bool")
    c.aastore()

    c.new(RCD_CLS)
    c.dup()
    c.aload(7)
    c.aload(8)
    c.iconst(0)
    c.invokespecial(RCD_CLS, "<init>", f"([L{USTR_CLS};{ARR_VAL}Z)V")
    c.invokevirtual(VAL_CLS, "normalize", f"(){L_VAL}")
    c.areturn()
    cb.add_method(0x000A, "makeRec", f"(IIIIIII){L_VAL}", c.finish(6, 9))

    # private static Value buildEvents(int maxId, int t, int g)
    # Total raw elements before SetEnumValue normalization: ((maxId+1)*(maxId+1) + 1) * 4
    c = CodeAsm(cb.cp)
    c.iload(0)
    c.iconst(1)
    c.iadd()
    c.dup()
    c.imul()
    c.iconst(1)
    c.iadd()
    c.iconst(4)
    c.imul()
    c.anewarray(VAL_CLS)
    c.astore(3)  # arr
    c.iconst(0)
    c.istore(4)  # out_pos

    # Loop i in 0..maxId, p in 0..maxId, q in 0..1, v in 0..1
    c.iconst(0)
    c.istore(5)  # i
    c.label("loop_i")
    c.iload(5)
    c.iload(0)
    c.if_icmpgt("done_i")
    c.iconst(0)
    c.istore(6)  # p
    c.label("loop_p")
    c.iload(6)
    c.iload(0)
    c.if_icmpgt("next_i")
    c.iconst(0)
    c.istore(7)  # q
    c.label("loop_q")
    c.iload(7)
    c.iconst(1)
    c.if_icmpgt("next_p")
    c.iconst(0)
    c.istore(8)  # v
    c.label("loop_v")
    c.iload(8)
    c.iconst(1)
    c.if_icmpgt("next_q")

    c.aload(3)
    c.iload(4)
    c.iload(5)
    c.iload(5)
    c.iload(6)
    c.iload(1)
    c.iload(2)
    c.iload(7)
    c.iload(8)
    c.invokestatic(ME, "makeRec", f"(IIIIIII){L_VAL}")
    c.aastore()
    c.iinc(4, 1)

    c.iinc(8, 1)
    c.goto("loop_v")
    c.label("next_q")
    c.iinc(7, 1)
    c.goto("loop_q")
    c.label("next_p")
    c.iinc(6, 1)
    c.goto("loop_p")
    c.label("next_i")
    c.iinc(5, 1)
    c.goto("loop_i")
    c.label("done_i")

    # Extra batch range: [first |-> 0, last |-> 1, previous |-> 0] for q in 0..1, v in 0..1
    c.iconst(0)
    c.istore(7)
    c.label("ex_q")
    c.iload(7)
    c.iconst(1)
    c.if_icmpgt("ex_done")
    c.iconst(0)
    c.istore(8)
    c.label("ex_v")
    c.iload(8)
    c.iconst(1)
    c.if_icmpgt("ex_next_q")
    c.aload(3)
    c.iload(4)
    c.iconst(0)
    c.iconst(1)
    c.iconst(0)
    c.iload(1)
    c.iload(2)
    c.iload(7)
    c.iload(8)
    c.invokestatic(ME, "makeRec", f"(IIIIIII){L_VAL}")
    c.aastore()
    c.iinc(4, 1)
    c.iinc(8, 1)
    c.goto("ex_v")
    c.label("ex_next_q")
    c.iinc(7, 1)
    c.goto("ex_q")
    c.label("ex_done")

    c.new(SET_CLS)
    c.dup()
    c.aload(3)
    c.iconst(0)
    c.invokespecial(SET_CLS, "<init>", f"({ARR_VAL}Z)V")
    c.dup()
    c.invokevirtual(VAL_CLS, "deepNormalize", "()V")
    c.invokevirtual(VAL_CLS, "normalize", f"(){L_VAL}")
    c.areturn()
    cb.add_method(0x000A, "buildEvents", f"(III){L_VAL}", c.finish(12, 9))

    # private static RecordValue asRcd(Value v)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.instanceof(RCD_CLS)
    c.ifeq("to_rcd")
    c.aload(0)
    c.checkcast(RCD_CLS)
    c.areturn()
    c.label("to_rcd")
    c.aload(0)
    c.invokevirtual(VAL_CLS, "toRcd", f"(){L_VAL}")
    c.checkcast(RCD_CLS)
    c.areturn()
    cb.add_method(0x000A, "asRcd", f"({L_VAL}){L_RCD}", c.finish(3, 1))

    # private static int selInt(RecordValue r, StringValue key)
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.aload(1)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.ireturn()
    cb.add_method(0x000A, "selInt", f"({L_RCD}{L_STR})I", c.finish(3, 2))

    # private static RecordValue makeBook(Value gen, Value id, Value phase, Value qty, Value stamp)
    c = CodeAsm(cb.cp)
    c.iconst(5)
    c.anewarray(USTR_CLS)
    c.astore(5)
    for idx, ufield in enumerate(("U_GEN", "U_ID", "U_PHASE", "U_QTY", "U_STAMP")):
        c.aload(5)
        c.iconst(idx)
        c.getstatic(ME, ufield, L_USTR)
        c.aastore()
    c.iconst(5)
    c.anewarray(VAL_CLS)
    c.astore(6)
    for idx in range(5):
        c.aload(6)
        c.iconst(idx)
        c.aload(idx)
        c.aastore()
    c.new(RCD_CLS)
    c.dup()
    c.aload(5)
    c.aload(6)
    c.iconst(0)
    c.invokespecial(RCD_CLS, "<init>", f"([L{USTR_CLS};{ARR_VAL}Z)V")
    c.invokevirtual(VAL_CLS, "normalize", f"(){L_VAL}")
    c.checkcast(RCD_CLS)
    c.areturn()
    cb.add_method(
        0x000A,
        "makeBook",
        f"({L_VAL}{L_VAL}{L_VAL}{L_VAL}{L_VAL}){L_RCD}",
        c.finish(6, 7),
    )

    # private static int classifyInt(RecordValue b, RecordValue e): 0=Stale, 1=Error, 2=Apply
    c = CodeAsm(cb.cp)
    c.aload(0)
    c.getstatic(ME, "S_ID", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.istore(2)  # b_id
    c.aload(1)
    c.getstatic(ME, "S_FIRST", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.istore(3)  # e_first
    c.aload(1)
    c.getstatic(ME, "S_LAST", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.istore(4)  # e_last

    c.getstatic(ME, "IS_SPOT", "I")
    c.ifeq("cls_usdm")

    # Spot: IF e.last <= b.id THEN Stale ELSE IF e.first > b.id+1 THEN Error ELSE Apply
    c.iload(4)
    c.iload(2)
    c.if_icmple("ret_stale")
    c.iload(3)
    c.iload(2)
    c.iconst(1)
    c.iadd()
    c.if_icmpgt("ret_error")
    c.iconst(2)
    c.ireturn()

    # UsdM: check b.phase == "Bridging" via UniqueString.equals (tok equality, safe across DiskStateQueue!)
    c.label("cls_usdm")
    c.aload(0)
    c.getstatic(ME, "S_PHASE", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.checkcast(STR_CLS)
    c.getfield(STR_CLS, "val", L_USTR)
    c.getstatic(ME, "U_BRIDGING", L_USTR)
    c.invokevirtual(USTR_CLS, "equals", f"(L{USTR_CLS};)Z")
    c.ifeq("usdm_not_bridging")

    # UsdM & Bridging: IF e.last < b.id THEN Stale ELSE IF e.first > b.id THEN Error ELSE Apply
    c.iload(4)
    c.iload(2)
    c.if_icmplt("ret_stale")
    c.iload(3)
    c.iload(2)
    c.if_icmpgt("ret_error")
    c.iconst(2)
    c.ireturn()

    # UsdM & !Bridging: IF e.last <= b.id THEN Stale ELSE IF e.previous # b.id THEN Error ELSE Apply
    c.label("usdm_not_bridging")
    c.iload(4)
    c.iload(2)
    c.if_icmple("ret_stale")
    c.aload(1)
    c.getstatic(ME, "S_PREV", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.iload(2)
    c.if_icmpne("ret_error")
    c.iconst(2)
    c.ireturn()

    c.label("ret_stale")
    c.iconst(0)
    c.ireturn()
    c.label("ret_error")
    c.iconst(1)
    c.ireturn()
    cb.add_method(0x000A, "classifyInt", f"({L_RCD}{L_RCD})I", c.finish(5, 5))

    # public static Value Classify(Value bv, Value ev)
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "READY", "I")
    c.ifne("cls_ready")
    c.invokestatic(ME, "initOnce", "()V")
    c.label("cls_ready")
    c.aload(0)
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.aload(1)
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.invokestatic(ME, "classifyInt", f"({L_RCD}{L_RCD})I")
    c.istore(2)
    c.iload(2)
    c.ifne("cls_not_stale")
    c.getstatic(ME, "S_STALE", L_STR)
    c.areturn()
    c.label("cls_not_stale")
    c.iload(2)
    c.iconst(1)
    c.if_icmpne("cls_apply")
    c.getstatic(ME, "S_ERROR", L_STR)
    c.areturn()
    c.label("cls_apply")
    c.getstatic(ME, "S_APPLY", L_STR)
    c.areturn()
    cb.add_method(0x0009, "Classify", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(4, 3), ("Classify", MOD))

    # private static RecordValue applyStep(RecordValue b, RecordValue e, int cls)
    c = CodeAsm(cb.cp)
    c.iload(2)
    c.ifne("ap_not_stale")
    c.aload(0)
    c.areturn()

    c.label("ap_not_stale")
    c.iload(2)
    c.iconst(1)
    c.if_icmpeq("ap_invalid")

    c.aload(1)
    c.getstatic(ME, "S_VALID", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.getstatic(BOOL_CLS, "ValFalse", f"L{BOOL_CLS};")
    c.invokevirtual("java/lang/Object", "equals", "(Ljava/lang/Object;)Z")
    c.ifne("ap_invalid")

    c.aload(0)
    c.getstatic(ME, "S_STAMP", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.istore(3)  # b_stamp
    c.iload(3)
    c.iconst(-1)
    c.if_icmpeq("ap_live")
    c.aload(1)
    c.getstatic(ME, "S_STAMP", L_STR)
    c.invokestatic(ME, "selInt", f"({L_RCD}{L_STR})I")
    c.iload(3)
    c.if_icmplt("ap_invalid")

    c.label("ap_live")
    c.aload(0)
    c.getstatic(ME, "S_GEN", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.aload(1)
    c.getstatic(ME, "S_LAST", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.getstatic(ME, "S_LIVE", L_STR)
    c.aload(1)
    c.getstatic(ME, "S_QTY", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.aload(1)
    c.getstatic(ME, "S_STAMP", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.invokestatic(ME, "makeBook", f"({L_VAL}{L_VAL}{L_VAL}{L_VAL}{L_VAL}){L_RCD}")
    c.areturn()

    c.label("ap_invalid")
    c.aload(0)
    c.getstatic(ME, "S_GEN", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.aload(0)
    c.getstatic(ME, "S_ID", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.getstatic(ME, "S_INVALID", L_STR)
    c.aload(0)
    c.getstatic(ME, "S_QTY", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.aload(0)
    c.getstatic(ME, "S_STAMP", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.invokestatic(ME, "makeBook", f"({L_VAL}{L_VAL}{L_VAL}{L_VAL}{L_VAL}){L_RCD}")
    c.areturn()
    cb.add_method(0x000A, "applyStep", f"({L_RCD}{L_RCD}I){L_RCD}", c.finish(8, 4))

    # public static Value Apply(Value bv, Value ev)
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "READY", "I")
    c.ifne("ap_ready")
    c.invokestatic(ME, "initOnce", "()V")
    c.label("ap_ready")
    c.aload(0)
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.astore(2)
    c.aload(1)
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.astore(3)
    c.aload(2)
    c.aload(3)
    c.aload(2)
    c.aload(3)
    c.invokestatic(ME, "classifyInt", f"({L_RCD}{L_RCD})I")
    c.invokestatic(ME, "applyStep", f"({L_RCD}{L_RCD}I){L_RCD}")
    c.areturn()
    cb.add_method(0x0009, "Apply", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(5, 4), ("Apply", MOD))

    # public static Value Replay(Value bv, Value qv, Value appv)
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "READY", "I")
    c.ifne("rp_ready")
    c.invokestatic(ME, "initOnce", "()V")
    c.label("rp_ready")
    c.aload(0)
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.astore(3)  # cur

    c.aload(1)
    c.instanceof(TUP_CLS)
    c.ifeq("rp_to_tup")
    c.aload(1)
    c.checkcast(TUP_CLS)
    c.goto("rp_got_tup")
    c.label("rp_to_tup")
    c.aload(1)
    c.invokevirtual(VAL_CLS, "toTuple", f"(){L_VAL}")
    c.checkcast(TUP_CLS)
    c.label("rp_got_tup")
    c.getfield(TUP_CLS, "elems", ARR_VAL)
    c.astore(4)  # elems

    c.aload(2)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(5)  # app

    c.aload(4)
    c.arraylength()
    c.istore(6)  # n
    c.iconst(0)
    c.istore(7)  # idx

    c.label("rp_loop")
    c.iload(7)
    c.iload(6)
    c.if_icmpge("rp_done")

    c.aload(4)
    c.iload(7)
    c.aaload()
    c.invokestatic(ME, "asRcd", f"({L_VAL}){L_RCD}")
    c.astore(8)  # e

    c.aload(3)
    c.aload(8)
    c.invokestatic(ME, "classifyInt", f"({L_RCD}{L_RCD})I")
    c.istore(9)  # cls

    c.aload(3)
    c.aload(8)
    c.iload(9)
    c.invokestatic(ME, "applyStep", f"({L_RCD}{L_RCD}I){L_RCD}")
    c.astore(3)  # cur = next

    c.aload(3)
    c.getstatic(ME, "S_PHASE", L_STR)
    c.invokevirtual(RCD_CLS, "select", f"({L_VAL}){L_VAL}")
    c.checkcast(STR_CLS)
    c.getfield(STR_CLS, "val", L_USTR)
    c.getstatic(ME, "U_INVALID", L_USTR)
    c.invokevirtual(USTR_CLS, "equals", f"(L{USTR_CLS};)Z")
    c.ifne("rp_done")

    c.iload(9)
    c.iconst(2)
    c.if_icmpne("rp_next")
    c.iinc(5, 1)
    c.label("rp_next")
    c.iinc(7, 1)
    c.goto("rp_loop")

    c.label("rp_done")
    c.iconst(2)
    c.anewarray(USTR_CLS)
    c.astore(10)
    c.aload(10)
    c.iconst(0)
    c.getstatic(ME, "U_APPLIED", L_USTR)
    c.aastore()
    c.aload(10)
    c.iconst(1)
    c.getstatic(ME, "U_BOOK", L_USTR)
    c.aastore()

    c.iconst(2)
    c.anewarray(VAL_CLS)
    c.astore(11)
    c.aload(11)
    c.iconst(0)
    c.iload(5)
    c.invokestatic(INT_CLS, "gen", f"(I)L{INT_CLS};")
    c.aastore()
    c.aload(11)
    c.iconst(1)
    c.aload(3)
    c.aastore()

    c.new(RCD_CLS)
    c.dup()
    c.aload(10)
    c.aload(11)
    c.iconst(0)
    c.invokespecial(RCD_CLS, "<init>", f"([L{USTR_CLS};{ARR_VAL}Z)V")
    c.invokevirtual(VAL_CLS, "normalize", f"(){L_VAL}")
    c.areturn()
    cb.add_method(
        0x0009,
        "Replay",
        f"({L_VAL}{L_VAL}{L_VAL}){L_VAL}",
        c.finish(8, 12),
        ("Replay", MOD),
    )

    # private static synchronized void initOnce()V: MUST run lazily on first operator
    # invocation AFTER TLC calls `UniqueString.initialize()` during spec loading, so
    # every interned `UniqueString` / `StringValue` / `RecordValue` shares TLC's active
    # `internTbl` and `tok` ordering!
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "READY", "I")
    c.ifeq("do_init")
    c.vreturn()
    c.label("do_init")
    for fname, lit in s_keys:
        c.new(STR_CLS)
        c.dup()
        c.ldc_str(lit)
        c.invokespecial(STR_CLS, "<init>", "(Ljava/lang/String;)V")
        c.putstatic(ME, fname, L_STR)
    for fname, lit in u_keys:
        c.ldc_str(lit)
        c.invokestatic(USTR_CLS, "uniqueStringOf", f"(Ljava/lang/String;)L{USTR_CLS};")
        c.putstatic(ME, fname, L_USTR)

    c.ldc_str("UsdM")
    c.ldc_str("tickwire.tlc.policy")
    c.ldc_str("Spot")
    c.invokestatic(
        "java/lang/System",
        "getProperty",
        "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
    )
    c.invokevirtual("java/lang/String", "equals", "(Ljava/lang/Object;)Z")
    c.ifne("policy_usdm")
    c.iconst(1)
    c.goto("store_policy")
    c.label("policy_usdm")
    c.iconst(0)
    c.label("store_policy")
    c.putstatic(ME, "IS_SPOT", "I")

    c.ldc_str("tickwire.tlc.maxId")
    c.iconst(-1)
    c.invokestatic("java/lang/Integer", "getInteger", "(Ljava/lang/String;I)Ljava/lang/Integer;")
    c.invokevirtual("java/lang/Integer", "intValue", "()I")
    c.dup()
    c.putstatic(ME, "MAX_ID", "I")
    c.istore(0)  # maxId
    c.iload(0)
    c.iflt("no_cache")

    c.iconst(256)
    c.anewarray(VAL_CLS)
    c.astore(1)
    c.iconst(0)
    c.istore(2)  # t
    c.label("cl_t")
    c.iload(2)
    c.iconst(16)
    c.if_icmpge("cl_done")
    c.iconst(0)
    c.istore(3)  # g
    c.label("cl_g")
    c.iload(3)
    c.iconst(16)
    c.if_icmpge("cl_next_t")
    c.aload(1)
    c.iload(2)
    c.iconst(4)
    c.ishl()
    c.iload(3)
    c.ior()
    c.iload(0)
    c.iload(2)
    c.iload(3)
    c.invokestatic(ME, "buildEvents", f"(III){L_VAL}")
    c.aastore()
    c.iinc(3, 1)
    c.goto("cl_g")
    c.label("cl_next_t")
    c.iinc(2, 1)
    c.goto("cl_t")
    c.label("cl_done")
    c.aload(1)
    c.putstatic(ME, "CACHE", ARR_VAL)
    c.label("no_cache")
    c.iconst(1)
    c.putstatic(ME, "READY", "I")
    c.vreturn()
    cb.add_method(0x002A, "initOnce", "()V", c.finish(8, 4))

    # public static Value Events(Value tv, Value gv)
    c = CodeAsm(cb.cp)
    c.getstatic(ME, "READY", "I")
    c.ifne("ev_ready")
    c.invokestatic(ME, "initOnce", "()V")
    c.label("ev_ready")
    c.aload(0)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(2)  # t
    c.aload(1)
    c.checkcast(INT_CLS)
    c.getfield(INT_CLS, "val", "I")
    c.istore(3)  # g
    c.getstatic(ME, "CACHE", ARR_VAL)
    c.astore(4)
    c.aload(4)
    c.ifnull("ev_miss")
    c.iload(2)
    c.iflt("ev_miss")
    c.iload(2)
    c.iconst(16)
    c.if_icmpge("ev_miss")
    c.iload(3)
    c.iflt("ev_miss")
    c.iload(3)
    c.iconst(16)
    c.if_icmpge("ev_miss")
    c.aload(4)
    c.iload(2)
    c.iconst(4)
    c.ishl()
    c.iload(3)
    c.ior()
    c.aaload()
    c.areturn()
    c.label("ev_miss")
    c.getstatic(ME, "MAX_ID", "I")
    c.iload(2)
    c.iload(3)
    c.invokestatic(ME, "buildEvents", f"(III){L_VAL}")
    c.areturn()
    cb.add_method(0x0009, "Events", f"({L_VAL}{L_VAL}){L_VAL}", c.finish(6, 5), ("Events", MOD))

    return cb.build()


def _build_tlc_overrides_registry(include_depth_sync: bool) -> bytes:
    ME = "tlc2/overrides/TLCOverrides"
    cb = ClassFileBuilder(ME, interfaces=["tlc2/overrides/ITLCOverrides"])

    c = CodeAsm(cb.cp)
    c.aload(0)
    c.invokespecial("java/lang/Object", "<init>", "()V")
    c.vreturn()
    cb.add_method(0x0001, "<init>", "()V", c.finish(2, 1))

    classes = ["tlc2/overrides/L0_CalibrationOverrides"]
    if include_depth_sync:
        classes.append("tlc2/overrides/L0_DepthSyncOverrides")

    c = CodeAsm(cb.cp)
    c.iconst(len(classes))
    c.anewarray("java/lang/Class")
    for idx, cls_name in enumerate(classes):
        c.dup()
        c.iconst(idx)
        c.ldc_cls(cls_name)
        c.aastore()
    c.areturn()
    cb.add_method(0x0001, "get", "()[Ljava/lang/Class;", c.finish(5, 1))

    return cb.build()


def emit_override_classes(target_dir: Path, include_depth_sync: bool = True) -> Path:
    """Write `tlc2/overrides/*.class` into `target_dir` and return `target_dir`."""
    pkg = target_dir / "tlc2" / "overrides"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "L0_CalibrationOverrides.class").write_bytes(_build_calib_overrides())
    if include_depth_sync:
        (pkg / "L0_DepthSyncOverrides.class").write_bytes(_build_depth_sync_overrides())
    (pkg / "TLCOverrides.class").write_bytes(_build_tlc_overrides_registry(include_depth_sync))
    return target_dir


if __name__ == "__main__":
    import sys

    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/tlc-overrides-test")
    emit_override_classes(out_dir, include_depth_sync=True)
    print(f"Wrote TLC native override classes to {out_dir}")

#![forbid(unsafe_code)]

use std::hint::black_box;

#[test]
#[should_panic(expected = "attempt to add with overflow")]
fn u64_add_overflow_panics_in_active_profile() {
    let _ = black_box(u64::MAX) + black_box(1u64);
}

#[test]
#[should_panic(expected = "attempt to subtract with overflow")]
fn u64_sub_overflow_panics_in_active_profile() {
    let _ = black_box(0u64) - black_box(1u64);
}

#[test]
#[should_panic(expected = "attempt to multiply with overflow")]
fn u64_mul_overflow_panics_in_active_profile() {
    let _ = black_box(u64::MAX / 2 + 2) * black_box(2u64);
}

#[test]
#[should_panic(expected = "attempt to add with overflow")]
fn i64_add_overflow_panics_in_active_profile() {
    let _ = black_box(i64::MAX) + black_box(1i64);
}

#[test]
#[should_panic(expected = "attempt to subtract with overflow")]
fn i64_sub_overflow_panics_in_active_profile() {
    let _ = black_box(i64::MIN) - black_box(1i64);
}

#[test]
#[should_panic(expected = "attempt to multiply with overflow")]
fn i64_mul_overflow_panics_in_active_profile() {
    let _ = black_box(i64::MIN) * black_box(-1i64);
}

#[test]
#[should_panic(expected = "attempt to add with overflow")]
fn i128_add_overflow_panics_in_active_profile() {
    let _ = black_box(i128::MAX) + black_box(1i128);
}

#[test]
#[should_panic(expected = "attempt to subtract with overflow")]
fn i128_sub_overflow_panics_in_active_profile() {
    let _ = black_box(i128::MIN) - black_box(1i128);
}

#[test]
#[should_panic(expected = "attempt to multiply with overflow")]
fn i128_mul_overflow_panics_in_active_profile() {
    let _ = black_box(i128::MAX / 2 + 2) * black_box(2i128);
}

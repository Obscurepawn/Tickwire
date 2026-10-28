use super::*;
use std::{fs::File, io::Read};

const STATUS_BYTES: usize = 64 * 1024;
const STAT_BYTES: usize = 8 * 1024;
fn bounded(path: &str, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > limit {
        return Err(Error::Capacity);
    }
    Ok(bytes)
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct KernelState {
    start_ticks: u64,
    uid: u32,
    gid: u32,
}
fn kernel(pid: u32) -> Result<KernelState> {
    let start_ticks = parse_start(&bounded(&format!("/proc/{pid}/stat"), STAT_BYTES)?, pid)?;
    let (uid, gid) = parse_ids(&bounded(&format!("/proc/{pid}/status"), STATUS_BYTES)?)?;
    Ok(KernelState {
        start_ticks,
        uid,
        gid,
    })
}
pub(super) fn current_process_identity() -> Result<ProcessIdentity> {
    let pid = std::process::id();
    stable_identity(pid, kernel(pid)?, kernel(pid)?)
}
fn stable_identity(pid: u32, before: KernelState, after: KernelState) -> Result<ProcessIdentity> {
    if pid == 0 || pid > i32::MAX as u32 || before != after {
        return Err(Error::Identity);
    }
    Ok(ProcessIdentity {
        pid,
        uid: after.uid,
        gid: after.gid,
    })
}
fn alive(entry: &mut Entry) -> Result<()> {
    if entry.exit.is_none() {
        entry.exit = entry.child.try_wait()?;
    }
    if entry.exit.is_some() {
        Err(Error::Exited)
    } else {
        Ok(())
    }
}
pub(super) fn observe(entry: &mut Entry) -> Result<LiveChildObservation> {
    let sampled_at = Instant::now();
    alive(entry)?;
    let pid = entry.child.id();
    let before = kernel(pid)?;
    if before.uid != entry.uid
        || before.gid != entry.gid
        || !entry.image.matches_process(pid).map_err(Error::Transport)?
    {
        return Err(Error::Identity);
    }
    let host_boot = parse_boot(&bounded("/proc/sys/kernel/random/boot_id", 64)?)?;
    let after = kernel(pid)?;
    if before != after || !entry.image.matches_process(pid).map_err(Error::Transport)? {
        return Err(Error::Identity);
    }
    alive(entry)?;
    Ok(LiveChildObservation {
        identity: entry.identity.clone(),
        process: ProcessIdentity {
            pid,
            uid: after.uid,
            gid: after.gid,
        },
        host_boot,
        start_ticks: after.start_ticks,
        image_bytes: entry.image.len(),
        connected_epoch: entry.connected.then_some(entry.last_control_epoch),
        sampled_at,
    })
}

fn number(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty()
        || bytes.len() > 20
        || !bytes.iter().all(u8::is_ascii_digit)
        || (bytes.len() > 1 && bytes[0] == b'0')
    {
        return Err(Error::Identity);
    }
    bytes.iter().try_fold(0u64, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(byte - b'0')))
            .ok_or(Error::Identity)
    })
}
fn parse_start(bytes: &[u8], pid: u32) -> Result<u64> {
    let begin = bytes
        .iter()
        .position(|b| *b == b'(')
        .ok_or(Error::Identity)?;
    if begin < 2 || bytes[begin - 1] != b' ' || number(&bytes[..begin - 1])? != u64::from(pid) {
        return Err(Error::Identity);
    }
    let end = bytes
        .iter()
        .rposition(|b| *b == b')')
        .filter(|end| *end >= begin)
        .ok_or(Error::Identity)?;
    let tail = bytes.get(end + 1..).ok_or(Error::Identity)?;
    if !tail.starts_with(b" ") {
        return Err(Error::Identity);
    }
    let fields: Vec<_> = tail
        .split(u8::is_ascii_whitespace)
        .filter(|s| !s.is_empty())
        .collect();
    // Linux since 3.5 has fields through exit_code (52); memfd targets are newer.
    if fields.len() < 50 || fields[0].len() != 1 || !b"RSDZTtXxKWPI".contains(&fields[0][0]) {
        return Err(Error::Identity);
    }
    number(fields[19])
}
fn quartet(bytes: &[u8]) -> Result<u32> {
    let mut fields = bytes
        .split(u8::is_ascii_whitespace)
        .filter(|s| !s.is_empty());
    let first: u32 = number(fields.next().ok_or(Error::Identity)?)?
        .try_into()
        .map_err(|_| Error::Identity)?;
    for _ in 0..3 {
        if number(fields.next().ok_or(Error::Identity)?)? != u64::from(first) {
            return Err(Error::Identity);
        }
    }
    if fields.next().is_some() {
        return Err(Error::Identity);
    }
    Ok(first)
}
fn parse_ids(bytes: &[u8]) -> Result<(u32, u32)> {
    let mut uid = None;
    let mut gid = None;
    for line in bytes.split(|b| *b == b'\n') {
        for (prefix, value) in [(b"Uid:".as_slice(), &mut uid), (b"Gid:", &mut gid)] {
            if let Some(tail) = line.strip_prefix(prefix) {
                if value.is_some() {
                    return Err(Error::Identity);
                }
                *value = Some(quartet(tail)?);
            }
        }
    }
    Ok((uid.ok_or(Error::Identity)?, gid.ok_or(Error::Identity)?))
}
fn parse_boot(bytes: &[u8]) -> Result<[u8; 16]> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    if bytes.len() != 36 {
        return Err(Error::Identity);
    }
    let mut output = [0; 16];
    let mut digits = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            if byte != b'-' {
                return Err(Error::Identity);
            }
            continue;
        }
        let value = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return Err(Error::Identity),
        };
        output[digits / 2] = output[digits / 2] * 16 + value;
        digits += 1;
    }
    if output == [0; 16] {
        return Err(Error::Identity);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn stat(comm: &[u8], ticks: &str) -> Vec<u8> {
        let mut bytes = b"123 (".to_vec();
        bytes.extend_from_slice(comm);
        bytes.extend_from_slice(b") S ");
        for _ in 0..18 {
            bytes.extend_from_slice(b"0 ");
        }
        bytes.extend_from_slice(ticks.as_bytes());
        for _ in 0..30 {
            bytes.extend_from_slice(b" 0");
        }
        bytes.push(b'\n');
        bytes
    }
    #[test]
    fn kernel_start_time_handles_opaque_comm_and_rejects_wrong_pid_or_bad_fields() {
        let row = stat(b"a)b ( c\n\xff", "987654321");
        assert_eq!(parse_start(&row, 123).unwrap(), 987654321);
        assert!(parse_start(&row, 124).is_err());
        for value in ["", "00", "+1", "-1", "18446744073709551616", "1x"] {
            assert!(parse_start(&stat(b"ok", value), 123).is_err(), "{value:?}");
        }
        assert_eq!(parse_start(&stat(b"ok", "0"), 123).unwrap(), 0);
        assert!(parse_start(b"123 (truncated) S 0", 123).is_err());
    }
    #[test]
    fn credentials_require_exact_current_kernel_fields_and_no_mixed_ids() {
        assert_eq!(
            parse_ids(b"Name:\t\xff\nUid:\t1000 1000 1000 1000\nGid:\t2000 2000 2000 2000\n")
                .unwrap(),
            (1000, 2000)
        );
        for body in [
            b"Uid: 1 1 1 1\n".as_slice(),
            b"Uid: 1 1 1 1\nUid: 1 1 1 1\nGid: 2 2 2 2\n",
            b"Uid: 1 1 1 0\nGid: 2 2 2 2\n",
            b"Uid: 1 1 1\nGid: 2 2 2 2\n",
            b"Uid: 1 1 1 1 1\nGid: 2 2 2 2\n",
            b"Uid: +1 +1 +1 +1\nGid: 2 2 2 2\n",
            b"Uid: 4294967296 4294967296 4294967296 4294967296\nGid: 2 2 2 2\n",
        ] {
            assert!(parse_ids(body).is_err());
        }
    }
    #[test]
    fn every_credential_slot_must_match_even_with_an_opaque_process_name() {
        for field in ["Uid", "Gid"] {
            for slot in 0..4 {
                let mut values = [1000; 4];
                values[slot] = 1001;
                let body = format!(
                    "{field}: {} {} {} {}\n{}: 1000 1000 1000 1000\n",
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    if field == "Uid" { "Gid" } else { "Uid" }
                );
                assert!(parse_ids(body.as_bytes()).is_err(), "{field} slot {slot}");
            }
        }
        for value in ["", "0 0 0", "0 0 0 0 0", "-1 -1 -1 -1", "01 01 01 01"] {
            assert!(parse_ids(format!("Uid: 0 0 0 0\nGid: {value}\n").as_bytes()).is_err());
        }
        assert_eq!(
            parse_ids(b"Name:\t(\xff)\nUid: 0 0 0 0\nGid: 0 0 0 0\n").unwrap(),
            (0, 0)
        );
    }
    #[test]
    fn current_identity_rejects_a_changed_kernel_sample() {
        let before = KernelState {
            start_ticks: 123,
            uid: 1000,
            gid: 2000,
        };
        assert_eq!(
            stable_identity(1234, before, before).unwrap(),
            ProcessIdentity {
                pid: 1234,
                uid: 1000,
                gid: 2000,
            }
        );
        for after in [
            KernelState {
                start_ticks: 124,
                ..before
            },
            KernelState {
                uid: 1001,
                ..before
            },
            KernelState {
                gid: 2001,
                ..before
            },
        ] {
            assert!(matches!(
                stable_identity(1234, before, after),
                Err(Error::Identity)
            ));
        }
        for pid in [0, (i32::MAX as u32) + 1] {
            assert!(stable_identity(pid, before, before).is_err());
        }
    }
    #[test]
    fn host_boot_is_the_exact_nonzero_kernel_uuid() {
        let expected = [0xab; 16];
        assert_eq!(
            parse_boot(b"abababab-abab-abab-abab-abababababab\n").unwrap(),
            expected
        );
        assert_eq!(
            parse_boot(b"abababab-abab-abab-abab-abababababab").unwrap(),
            expected
        );
        for body in [
            b"abababababab-abab-abab-abababababab".as_slice(),
            b"00000000-0000-0000-0000-000000000000",
            b"abababab-abab-abab-abab-abababababab\n\n",
            b"zbababab-abab-abab-abab-abababababab",
        ] {
            assert!(parse_boot(body).is_err());
        }
    }
}

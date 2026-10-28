use super::*;

pub(super) fn list(bytes: &[u8], maximum: u32) -> Result<Vec<u32>> {
    list_text(line(bytes)?, maximum)
}
fn list_text(text: &str, maximum: u32) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    for part in text.split(',') {
        let (start, end) = if let Some((a, b)) = part.split_once('-') {
            let a = decimal(a)?;
            let b = decimal(b)?;
            if a >= b {
                return Err(Error::InvalidResources);
            }
            (a, b)
        } else {
            let n = decimal(part)?;
            (n, n)
        };
        if end > u64::from(maximum) || out.last().is_some_and(|n| u64::from(*n) >= start) {
            return Err(Error::InvalidResources);
        }
        out.extend(start as u32..=end as u32);
    }
    Ok(out)
}
pub(super) fn finite(bytes: &[u8]) -> Result<u64> {
    decimal(line(bytes)?)
}
fn line(bytes: &[u8]) -> Result<&str> {
    if bytes.len() > 32_768 {
        return Err(Error::InvalidResources);
    }
    let bytes = bytes.strip_suffix(b"\n").ok_or(Error::InvalidResources)?;
    std::str::from_utf8(bytes).map_err(|_| Error::InvalidResources)
}
pub(super) fn decimal(text: &str) -> Result<u64> {
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::InvalidResources);
    }
    text.parse().map_err(|_| Error::InvalidResources)
}
pub(super) fn ids(bytes: &[u8]) -> Result<Vec<u32>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let text = line(bytes)?;
    let mut ids = Vec::new();
    for field in text.split('\n') {
        let id: u32 = decimal(field)?
            .try_into()
            .map_err(|_| Error::InvalidResources)?;
        if id == 0 || ids.len() == 4096 {
            return Err(Error::InvalidResources);
        }
        ids.push(id);
    }
    ids.sort_unstable();
    if ids.windows(2).any(|p| p[0] == p[1]) {
        return Err(Error::InvalidResources);
    }
    Ok(ids)
}
pub(super) fn no_descendants(bytes: &[u8]) -> Result<()> {
    let text = line(bytes)?;
    let mut keys = std::collections::BTreeSet::new();
    for field in text.split('\n') {
        let (key, value) = field.split_once(' ').ok_or(Error::InvalidResources)?;
        if key.is_empty() || !keys.insert(key) {
            return Err(Error::InvalidResources);
        }
        let value = decimal(value)?;
        if matches!(key, "nr_descendants" | "nr_dying_descendants") && value != 0 {
            return Err(Error::ResourceConflict);
        }
    }
    if !keys.contains("nr_descendants") || !keys.contains("nr_dying_descendants") {
        return Err(Error::InvalidResources);
    }
    Ok(())
}
pub(super) fn thread_status(bytes: &[u8], pid: u32, tid: u32, node: u32) -> Result<()> {
    if bytes.len() > 32_768 {
        return Err(Error::InvalidResources);
    }
    let bytes = bytes.strip_suffix(b"\n").ok_or(Error::InvalidResources)?;
    let mut tgid = None;
    let mut task = None;
    let mut mems = None;
    // Name is kernel-supplied opaque bytes, not necessarily UTF-8. Decode only
    // the ASCII fields whose semantics are needed for this resource sample.
    for field in bytes.split(|b| *b == b'\n') {
        for (key, slot) in [
            (b"Tgid:\t".as_slice(), &mut tgid),
            (b"Pid:\t".as_slice(), &mut task),
        ] {
            if let Some(value) = field.strip_prefix(key)
                && slot
                    .replace(decimal(
                        std::str::from_utf8(value).map_err(|_| Error::InvalidResources)?,
                    )?)
                    .is_some()
            {
                return Err(Error::InvalidResources);
            }
        }
        if let Some(value) = field.strip_prefix(b"Mems_allowed_list:\t") {
            let nodes = list_text(
                std::str::from_utf8(value).map_err(|_| Error::InvalidResources)?,
                31,
            )?;
            if mems.replace(nodes).is_some() {
                return Err(Error::InvalidResources);
            }
        }
    }
    if tgid != Some(u64::from(pid))
        || task != Some(u64::from(tid))
        || mems.as_deref() != Some(&[node])
    {
        return Err(Error::InvalidResources);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn thread_name_is_opaque_kernel_bytes() {
        thread_status(
            b"Name:\tworker(\xff)\nTgid:\t10\nPid:\t11\nMems_allowed_list:\t2\n",
            10,
            11,
            2,
        )
        .unwrap();
        assert!(
            thread_status(
                b"Tgid:\t1\xff\nPid:\t11\nMems_allowed_list:\t2\n",
                10,
                11,
                2
            )
            .is_err()
        );
    }
    #[test]
    fn kernel_cpu_lists_are_bounded_exact_sets() {
        assert_eq!(
            list(b"0-2,9,1024,4095\n", 4095).unwrap(),
            [0, 1, 2, 9, 1024, 4095]
        );
        for bad in [
            b"".as_slice(),
            b"\n",
            b"1,1\n",
            b"3-1\n",
            b"1,0\n",
            b"1-2,2-3\n",
            b"4096\n",
            b"0-4294967295\n",
            b"01\n",
            b"+1\n",
            b"1,\n",
            b"1 \n",
            b"1\n2\n",
        ] {
            assert!(list(bad, 4095).is_err(), "{bad:?}");
        }
    }
    #[test]
    fn finite_memory_rejects_unlimited_and_noncanonical_values() {
        assert_eq!(finite(b"1048576\n").unwrap(), 1_048_576);
        assert_eq!(finite(b"0\n").unwrap(), 0);
        for bad in [
            b"max\n".as_slice(),
            b"01\n",
            b"+1\n",
            b"-1\n",
            b"1 \n",
            b"18446744073709551616\n",
            b"1\n2\n",
        ] {
            assert!(finite(bad).is_err(), "{bad:?}");
        }
    }
    #[test]
    fn memberships_and_descendants_reject_ambiguity() {
        assert_eq!(ids(b"200\n100\n").unwrap(), [100, 200]);
        assert!(ids(b"").unwrap().is_empty());
        for bad in [
            b"0\n".as_slice(),
            b"2\n2\n",
            b"01\n",
            b"4294967296\n",
            b"\n",
            b"2",
        ] {
            assert!(ids(bad).is_err(), "{bad:?}");
        }
        no_descendants(b"nr_descendants 0\nnr_dying_descendants 0\nnr_subsys_cpuset 1\n").unwrap();
        for bad in [
            b"nr_descendants 0\n".as_slice(),
            b"nr_descendants 1\nnr_dying_descendants 0\n",
            b"nr_descendants 0\nnr_dying_descendants 1\n",
            b"nr_descendants 0\nnr_descendants 0\nnr_dying_descendants 0\n",
            b"nr_descendants max\nnr_dying_descendants 0\n",
        ] {
            assert!(no_descendants(bad).is_err(), "{bad:?}");
        }
    }
    #[test]
    fn every_thread_must_have_exact_process_and_memory_node() {
        let valid = b"Name:\tworker\nTgid:\t10\nPid:\t11\nMems_allowed_list:\t2\n";
        thread_status(valid, 10, 11, 2).unwrap();
        assert!(thread_status(valid, 10, 12, 2).is_err());
        assert!(thread_status(valid, 12, 11, 2).is_err());
        assert!(thread_status(valid, 10, 11, 3).is_err());
        for tail in [
            b"Mems_allowed_list:\t2\n".as_slice(),
            b"Pid:\t11\n",
            b"Tgid:\t10\n",
        ] {
            assert!(thread_status(&[valid.as_slice(), tail].concat(), 10, 11, 2).is_err());
        }
        assert!(
            thread_status(b"Tgid:\t10\nPid:\t11\nMems_allowed_list:\t2-3\n", 10, 11, 2).is_err()
        );
    }
}

//! Static topology and resource checks. Deployment evidence is checked separately.

use crate::{Error, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Resources {
    pub strategy_instances: usize,
    pub strategy_domain_pairs: usize,
    pub banks_and_cursors: u64,
    pub snapshots: u64,
    pub projection_objects: u64,
    pub total_shared_bytes: u64,
}

fn check_that(condition: bool, check: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Semantic(check))
    }
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str().ok_or(Error::Schema)
}
fn number(value: &Value, field: &str) -> Result<u64> {
    value[field].as_u64().ok_or(Error::Schema)
}
fn array<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    value[field]
        .as_array()
        .map(Vec::as_slice)
        .ok_or(Error::Schema)
}
fn id(value: &Value) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => n.as_u64().map(|n| n.to_string()).ok_or(Error::Schema),
        _ => Err(Error::Schema),
    }
}
fn keyed<'a>(rows: &'a [Value], field: &str) -> Result<BTreeMap<String, &'a Value>> {
    let mut result = BTreeMap::new();
    for row in rows {
        check_that(
            result.insert(id(&row[field])?, row).is_none(),
            "duplicate_id",
        )?;
    }
    Ok(result)
}
fn lookup<'a>(map: &BTreeMap<String, &'a Value>, key: &Value) -> Result<&'a Value> {
    map.get(&id(key)?)
        .copied()
        .ok_or(Error::Semantic("missing_reference"))
}
fn ids(values: &[Value]) -> Result<BTreeSet<String>> {
    let set = values.iter().map(id).collect::<Result<BTreeSet<_>>>()?;
    check_that(set.len() == values.len(), "duplicate_reference")?;
    Ok(set)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Error::Capacity)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or(Error::Capacity)
}
fn sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0, add)
}
pub(crate) fn decimal_le(left: &str, right: &str) -> Result<bool> {
    fn parts(s: &str) -> Result<(&str, &str)> {
        let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
        check_that(
            !whole.is_empty()
                && whole.bytes().all(|x| x.is_ascii_digit())
                && fraction.bytes().all(|x| x.is_ascii_digit()),
            "decimal",
        )?;
        Ok((
            whole.trim_start_matches('0'),
            fraction.trim_end_matches('0'),
        ))
    }
    let (a, af) = parts(left)?;
    let (b, bf) = parts(right)?;
    let order = a.len().cmp(&b.len()).then(a.cmp(b));
    if !order.is_eq() {
        return Ok(order.is_lt());
    }
    let length = af.len().max(bf.len());
    Ok(af
        .bytes()
        .chain(std::iter::repeat(b'0'))
        .take(length)
        .cmp(bf.bytes().chain(std::iter::repeat(b'0')).take(length))
        .is_le())
}

pub(crate) fn check(c: &Value) -> Result<Resources> {
    let hosts = keyed(array(c, "hosts")?, "id")?;
    let procs = keyed(array(c, "processes")?, "id")?;
    let accounts = keyed(array(c, "accounts")?, "id")?;
    let venues = keyed(array(c, "venues")?, "id")?;
    let policies = keyed(array(c, "risk_policies")?, "id")?;
    let ipc = &c["ipc"];
    let topics = keyed(array(ipc, "topics")?, "id")?;
    let mut domains = BTreeSet::new();
    let mut owners = BTreeSet::new();
    let mut host_hot_cpus: BTreeMap<&str, BTreeSet<u64>> = BTreeMap::new();
    let mut host_cold_cpus: BTreeMap<&str, BTreeSet<u64>> = BTreeMap::new();
    let mut memory: BTreeMap<&str, u64> = BTreeMap::new();
    for h in hosts.values() {
        let hid = string(h, "id")?;
        let hk = cpu_list(array(h, "housekeeping_cpus")?, true)?;
        host_cold_cpus.insert(hid, hk);
    }
    // Schema bounds OS CPU and NUMA IDs. Actual existence, sparse numbering
    // and SMT topology require the inventory, not counts used as ID ranges.
    for p in procs.values() {
        let h = lookup(&hosts, &p["host_id"])?;
        let host = string(h, "id")?;
        let role = string(p, "role")?;
        let process_cpus = cpu_list(array(p, "cpus")?, false)?;
        let proc_cold = if p.get("housekeeping_cpus").is_some() {
            Some(cpu_list(array(p, "housekeeping_cpus")?, true)?)
        } else {
            None
        };
        if let Some(ref cold) = proc_cold {
            host_cold_cpus
                .entry(host)
                .or_default()
                .extend(cold.iter().copied());
        }
        validate_process_cpu_and_ors(p, role, &process_cpus, proc_cold.as_ref(), &accounts)?;
        if role != "supervisor" {
            let mut proc_hot = process_cpus;
            for field in ["req_cpu", "resp_cpu", "reactor_cpu", "worker_cpu"] {
                if let Some(val) = p.get(field) {
                    proc_hot.insert(val.as_u64().ok_or(Error::Schema)?);
                }
            }
            if let Some(ref cold) = proc_cold {
                check_that(proc_hot.is_disjoint(cold), "hot_housekeeping_overlap")?;
            }
            check_that(
                proc_hot.is_disjoint(&host_cold_cpus[host]),
                "hot_housekeeping_overlap",
            )?;
            let used = host_hot_cpus.entry(host).or_default();
            for n in proc_hot {
                check_that(used.insert(n), "cpu_sharing")?;
            }
        }
        let total = memory.entry(host).or_default();
        *total = add(*total, number(p, "memory_limit_bytes")?)?;
    }
    for (host, hot) in &host_hot_cpus {
        if let Some(cold) = host_cold_cpus.get(host) {
            check_that(hot.is_disjoint(cold), "hot_housekeeping_overlap")?;
        }
    }
    for h in hosts.values() {
        check_that(
            add(
                *memory.get(string(h, "id")?).unwrap_or(&0),
                number(h, "shm_budget_bytes")?,
            )? <= number(h, "memory_bytes")?,
            "host_memory",
        )?;
    }
    for venue in venues.values() {
        check_that(
            venue["environment"] == c["identity"]["environment"],
            "venue_environment",
        )?;
    }
    for a in accounts.values() {
        check_that(
            domains.insert(id(&a["risk_domain_id"])?),
            "risk_domain_owner",
        )?;
        let venue = lookup(&venues, &a["venue_id"])?;
        let policy = lookup(&policies, &a["risk_policy_id"])?;
        check_that(
            owners.insert((
                string(venue, "venue")?,
                string(venue, "product")?,
                string(a, "account_identity_ref")?,
            )),
            "duplicate_account_owner",
        )?;
        let owner = lookup(&procs, &a["exec_process_id"])?;
        check_that(string(owner, "role")? == "exec", "executor_role")?;
        let spot = string(venue, "product")? == "Spot";
        for field in ["position_mode", "margin_mode", "asset_mode"] {
            check_that(
                (string(a, field)? == "NotApplicable") == spot,
                "account_mode",
            )?;
        }
        for contract in array(a, "instrument_overrides")? {
            check_that(
                (string(contract, "contract_kind")? == "Spot") == spot,
                "contract_product",
            )?;
            check_that(
                array(a, "allowed_symbols")?.contains(&contract["symbol"]),
                "contract_symbol",
            )?;
            check_that(
                (number(contract, "expiry_epoch_ms")? > 0)
                    == (string(contract, "contract_kind")? == "Delivery"),
                "contract_expiry",
            )?;
        }
        check_that(
            decimal_le(
                string(policy, "max_order_quote")?,
                string(policy, "max_gross_quote")?,
            )?,
            "policy_order_budget",
        )?;
    }
    let mut states = BTreeSet::new();
    let mut processes = BTreeSet::new();
    let mut consumers: BTreeMap<String, u64> = BTreeMap::new();
    let mut pairs = 0u64;
    for strategy in array(c, "strategies")? {
        let p = lookup(&procs, &strategy["process_id"])?;
        check_that(string(p, "role")? == "strategy", "strategy_role")?;
        check_that(
            states.insert(id(&strategy["logical_strategy_id"])?)
                && processes.insert(string(strategy, "process_id")?),
            "strategy_identity",
        )?;
        let account_ids = ids(array(strategy, "account_ids")?)?;
        let budgets = keyed(array(strategy, "risk_budgets")?, "account_id")?;
        check_that(
            account_ids == budgets.keys().cloned().collect(),
            "budget_accounts",
        )?;
        for account in &account_ids {
            let a = accounts
                .get(account)
                .ok_or(Error::Semantic("account_reference"))?;
            let policy = lookup(&policies, &a["risk_policy_id"])?;
            let budget = budgets[account];
            check_that(
                budget["quote_asset"] == policy["quote_asset"],
                "budget_currency",
            )?;
            check_that(
                decimal_le(
                    string(budget, "max_order_quote")?,
                    string(budget, "max_gross_quote")?,
                )? && decimal_le(
                    string(budget, "max_order_quote")?,
                    string(policy, "max_order_quote")?,
                )?,
                "strategy_order_budget",
            )?;
        }
        pairs = add(pairs, account_ids.len() as u64)?;
        for topic in ids(array(strategy, "topic_ids")?)? {
            check_that(topics.contains_key(&topic), "topic_reference")?;
            let count = consumers.entry(topic).or_default();
            *count = add(*count, 1)?;
        }
        parameters(&strategy["params"], 16)?;
    }
    for p in procs.values() {
        check_that(
            string(p, "role")? != "strategy" || processes.contains(string(p, "id")?),
            "strategy_payload",
        )?;
    }
    for (key, topic) in &topics {
        let publisher = lookup(&procs, &topic["publisher_process_id"])?;
        check_that(string(publisher, "role")? == "md", "publisher_role")?;
        check_that(
            lookup(&venues, &topic["venue_id"])?["product"] == topic["product"],
            "topic_product",
        )?;
        check_that(
            add(*consumers.get(key).unwrap_or(&0), accounts.len() as u64)?
                <= number(topic, "consumer_limit")?,
            "consumer_capacity",
        )?;
    }
    let resources = resources(ipc, &topics, pairs, accounts.len() as u64, processes.len())?;
    for h in hosts.values() {
        if string(h, "role")? == "trading" {
            check_that(
                resources.total_shared_bytes <= number(h, "shm_budget_bytes")?,
                "shm_capacity",
            )?;
        }
    }
    let control = &c["control"];
    for (object, fields) in [
        (ipc, &["resource_heartbeat_ms", "resource_session_ms"][..]),
        (
            control,
            &["strategy_progress_interval_ms", "strategy_suspect_after_ms"],
        ),
        (&c["timeouts"], &["reconnect_min_ms", "reconnect_max_ms"]),
    ] {
        for field in fields.windows(2) {
            check_that(
                number(object, field[0])? < number(object, field[1])?,
                "timeout_order",
            )?;
        }
    }
    for binding in array(control, "role_bindings")? {
        for account in array(binding, "account_ids")? {
            lookup(&accounts, account)?;
        }
    }
    if string(&c["identity"], "mode")? == "live" {
        live_references(c, &mut Vec::new())?;
    }
    Ok(resources)
}

fn resources(
    ipc: &Value,
    topics: &BTreeMap<String, &Value>,
    pairs: u64,
    account_count: u64,
    strategy_instances: usize,
) -> Result<Resources> {
    for field in ["market_capacity", "intent_capacity", "report_capacity"] {
        check_that(number(ipc, field)?.is_power_of_two(), "ring_capacity")?;
    }
    check_that(
        number(ipc, "ring_high_watermark_records")? < number(ipc, "ring_stop_claiming_records")?
            && number(ipc, "ring_stop_claiming_records")? < number(ipc, "market_capacity")?,
        "ring_watermarks",
    )?;
    let classes = keyed(array(ipc, "market_arena_classes")?, "class_id")?;
    let mut arena_bytes = 0;
    for class in classes.values() {
        let block = number(class, "block_bytes")?;
        check_that(
            block >= 128 && block % 128 == 0 && number(class, "payload_limit")? <= block - 128,
            "arena_geometry",
        )?;
        arena_bytes = add(arena_bytes, mul(block, number(class, "block_count")?)?)?;
    }
    check_that(
        arena_bytes == number(ipc, "market_arena_bytes_per_bank")?,
        "arena_capacity",
    )?;
    let market_banks = mul(topics.len() as u64, number(ipc, "market_banks")?)?;
    let market_cursors = mul(
        sum(topics
            .values()
            .map(|t| number(t, "consumer_limit"))
            .collect::<Result<Vec<_>>>()?)?,
        number(ipc, "market_banks")?,
    )?;
    let market_bytes = sum([
        8192,
        mul(
            number(ipc, "market_capacity")?,
            number(ipc, "market_record_bytes")?,
        )?,
        arena_bytes,
    ])?;
    let intent_bytes = add(
        8192,
        mul(
            number(ipc, "intent_capacity")?,
            number(ipc, "intent_record_bytes")?,
        )?,
    )?;
    let report_bytes = add(
        8192,
        mul(
            number(ipc, "report_capacity")?,
            number(ipc, "report_record_bytes")?,
        )?,
    )?;
    let pair_bytes = add(
        mul(number(ipc, "intent_banks")?, add(intent_bytes, 8192)?)?,
        mul(number(ipc, "report_banks")?, add(report_bytes, 8192)?)?,
    )?;
    let banks_and_cursors = sum([
        mul(market_banks, market_bytes)?,
        mul(market_cursors, 8192)?,
        mul(pairs, pair_bytes)?,
    ])?;
    let snapshots = mul(
        mul(
            add(topics.len() as u64, account_count)?,
            number(ipc, "snapshots_per_owner")?,
        )?,
        number(ipc, "snapshot_max_bytes")?,
    )?;
    let projection_objects = mul(
        number(ipc, "config_projection_max_objects")?,
        add(4096, number(ipc, "config_projection_max_bytes")?)?,
    )?;
    Ok(Resources {
        strategy_instances,
        strategy_domain_pairs: pairs as usize,
        banks_and_cursors,
        snapshots,
        projection_objects,
        total_shared_bytes: sum([
            banks_and_cursors,
            snapshots,
            projection_objects,
            number(ipc, "relay_budget_bytes")?,
        ])?,
    })
}

fn cpu_list(values: &[Value], require_ascending: bool) -> Result<BTreeSet<u64>> {
    check_that(!values.is_empty(), "empty_cpu_list")?;
    let mut set = BTreeSet::new();
    let mut prev: Option<u64> = None;
    for v in values {
        let cpu = v.as_u64().ok_or(Error::Schema)?;
        check_that(cpu < 4096, "cpu_range")?;
        if require_ascending && let Some(last) = prev {
            check_that(last < cpu, "housekeeping_cpu_order")?;
        }
        check_that(set.insert(cpu), "cpu_duplicate")?;
        prev = Some(cpu);
    }
    Ok(set)
}

fn decimal_positive(s: &str) -> Result<bool> {
    Ok(!decimal_le(s, "0")?)
}

fn valid_ip(s: &str) -> bool {
    s.parse::<IpAddr>().is_ok_and(|ip| !ip.is_unspecified())
}

fn validate_process_cpu_and_ors(
    p: &Value,
    role: &str,
    process_cpus: &BTreeSet<u64>,
    proc_cold: Option<&BTreeSet<u64>>,
    accounts: &BTreeMap<String, &Value>,
) -> Result<()> {
    let exec_only = [
        "req_cpu",
        "resp_cpu",
        "epoll_wait_ms",
        "max_order_age_ns",
        "enabled_accounts",
        "order_manager",
        "rms",
    ];
    for field in exec_only {
        if p.get(field).is_some() {
            check_that(role == "exec", "process_role_field")?;
        }
    }
    if p.get("reactor_cpu").is_some() {
        check_that(role == "md", "process_role_field")?;
    }
    if p.get("worker_cpu").is_some() {
        check_that(role == "strategy", "process_role_field")?;
    }
    let req_cpu = p
        .get("req_cpu")
        .map(|v| v.as_u64().ok_or(Error::Schema))
        .transpose()?;
    let resp_cpu = p
        .get("resp_cpu")
        .map(|v| v.as_u64().ok_or(Error::Schema))
        .transpose()?;
    let reactor_cpu = p
        .get("reactor_cpu")
        .map(|v| v.as_u64().ok_or(Error::Schema))
        .transpose()?;
    let worker_cpu = p
        .get("worker_cpu")
        .map(|v| v.as_u64().ok_or(Error::Schema))
        .transpose()?;
    for pin in [req_cpu, resp_cpu, reactor_cpu, worker_cpu]
        .into_iter()
        .flatten()
    {
        check_that(pin < 4096, "cpu_range")?;
        if let Some(cold) = proc_cold {
            check_that(!cold.contains(&pin), "hot_housekeeping_overlap")?;
        }
    }
    if let (Some(req), Some(resp)) = (req_cpu, resp_cpu) {
        check_that(req != resp, "req_resp_cpu_equal")?;
    }
    for pin in [req_cpu, resp_cpu, reactor_cpu, worker_cpu]
        .into_iter()
        .flatten()
    {
        check_that(process_cpus.contains(&pin), "hot_cpu_binding")?;
    }
    if let Some(wait_ms) = p.get("epoll_wait_ms") {
        check_that(
            wait_ms.as_u64().ok_or(Error::Schema)? <= 1000,
            "epoll_wait_ms",
        )?;
    }
    if let Some(age_ns) = p.get("max_order_age_ns") {
        let age = age_ns.as_u64().ok_or(Error::Schema)?;
        check_that((1..=1_000_000_000).contains(&age), "max_order_age_ns")?;
    }
    if let Some(om) = p.get("order_manager") {
        let slots = number(om, "slots_per_strat")?;
        check_that(
            (1..=1_048_576).contains(&slots) && slots.is_power_of_two(),
            "slots_per_strat",
        )?;
        let exp = number(om, "fill_map_exponent")?;
        check_that((2..=22).contains(&exp), "fill_map_exponent")?;
        let mem_s = number(om, "fill_memory_s")?;
        check_that((1..=86_400).contains(&mem_s), "fill_memory_s")?;
        check_that(number(om, "max_query_count")? <= 1024, "max_query_count")?;
    }
    if let Some(ea_list) = p.get("enabled_accounts") {
        let rows = ea_list.as_array().ok_or(Error::Schema)?;
        check_that((1..=255).contains(&rows.len()), "enabled_accounts_count")?;
        let mut refs = BTreeSet::new();
        let mut bound_ids = BTreeSet::new();
        for ea in rows {
            let aref = number(ea, "account_ref")?;
            check_that(
                (1..=255).contains(&aref) && refs.insert(aref),
                "account_ref",
            )?;
            if let Some(aid_val) = ea.get("account_id") {
                let aid = aid_val.as_str().ok_or(Error::Schema)?;
                check_that(bound_ids.insert(aid), "duplicate_enabled_account")?;
                let acc = accounts
                    .get(aid)
                    .ok_or(Error::Semantic("enabled_account_reference"))?;
                check_that(acc["exec_process_id"] == p["id"], "enabled_account_owner")?;
            }
            let strats = array(ea, "strategies")?;
            check_that(
                !strats.is_empty() && strats.len() <= 255,
                "enabled_strategies",
            )?;
            let mut seen_strats = BTreeSet::new();
            for s in strats {
                let sid = s.as_u64().ok_or(Error::Schema)?;
                check_that(sid <= 254 && seen_strats.insert(sid), "enabled_strategies")?;
            }
            if let Some(ip_val) = ea.get("local_ip") {
                let ip = ip_val.as_str().ok_or(Error::Schema)?;
                check_that(valid_ip(ip), "enabled_account_ip")?;
            }
            let urls = ea["urls"].as_object().ok_or(Error::Schema)?;
            check_that(!urls.is_empty(), "enabled_account_urls")?;
            for pool in urls.values() {
                let tuples = pool.as_array().ok_or(Error::Schema)?;
                check_that(!tuples.is_empty(), "enabled_account_urls")?;
                for tuple in tuples {
                    let pair = tuple.as_array().ok_or(Error::Schema)?;
                    check_that(pair.len() == 2, "enabled_account_urls")?;
                    let ip = pair[0].as_str().ok_or(Error::Schema)?;
                    let count = pair[1].as_u64().ok_or(Error::Schema)?;
                    check_that(
                        valid_ip(ip) && (1..=64).contains(&count),
                        "enabled_account_urls",
                    )?;
                }
            }
            if let Some(rl) = ea.get("rate_limit") {
                let map = rl.as_object().ok_or(Error::Schema)?;
                check_that(!map.is_empty(), "rate_limit")?;
                if let (Some(o10), Some(omin)) = (rl.get("orders_10s"), rl.get("orders_min")) {
                    check_that(
                        o10.as_u64().ok_or(Error::Schema)? <= omin.as_u64().ok_or(Error::Schema)?,
                        "rate_limit_order",
                    )?;
                }
            }
        }
    }
    if let Some(rms) = p.get("rms") {
        let max_order = string(rms, "max_order_notional")?;
        let max_pos = string(rms, "max_pos_notional")?;
        let ff = string(rms, "fat_finger_ratio")?;
        check_that(
            decimal_positive(max_order)?
                && decimal_positive(max_pos)?
                && decimal_le(max_order, max_pos)?,
            "rms_notional_order",
        )?;
        check_that(
            decimal_positive(ff)? && decimal_le(ff, "1")?,
            "rms_fat_finger_ratio",
        )?;
        let w1 = &rms["1s"];
        let w5 = &rms["5s"];
        let w10 = &rms["10s"];
        for w in [w1, w5, w10] {
            check_that(
                number(w, "max_new_orders")? >= 1 && decimal_positive(string(w, "max_order_qty")?)?,
                "rms_window",
            )?;
        }
        check_that(
            number(w1, "max_new_orders")? <= number(w5, "max_new_orders")?
                && number(w5, "max_new_orders")? <= number(w10, "max_new_orders")?
                && decimal_le(string(w1, "max_order_qty")?, string(w5, "max_order_qty")?)?
                && decimal_le(string(w5, "max_order_qty")?, string(w10, "max_order_qty")?)?,
            "rms_window_order",
        )?;
    }
    Ok(())
}

fn parameters(value: &Value, depth: usize) -> Result<()> {
    match value {
        Value::Object(map) => {
            let next = depth.checked_sub(1).ok_or(Error::Depth)?;
            for (key, value) in map {
                check_that(
                    !["api_key", "private_key", "password", "token", "secret"]
                        .contains(&key.to_ascii_lowercase().as_str()),
                    "secret_parameter",
                )?;
                parameters(value, next)?;
            }
        }
        Value::Array(values) => {
            let next = depth.checked_sub(1).ok_or(Error::Depth)?;
            for v in values {
                parameters(v, next)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn live_references(value: &Value, path: &mut Vec<String>) -> Result<()> {
    if path.len() == 3 && path[0] == "strategies" && path[2] == "params" {
        // Parameter values are governed by the pinned parameter schema.
        // Projection separately enforces every embedded secret:// reference.
        return Ok(());
    }
    match value {
        Value::String(s) => {
            check_that(!s.contains("REQUIRED"), "live_reference")?;
            let field = path.last().map(String::as_str).unwrap_or("");
            if field.ends_with("_ref") || field.ends_with("_uri") || field.ends_with("_dir") {
                check_that(
                    !s.contains(".invalid")
                        && !s.starts_with("artifact://fixture/")
                        && !s.starts_with("secret://fixture/"),
                    "live_reference",
                )?;
            }
            if path.len() == 3 && path[0] == "venues" && field == "transport" {
                check_that(s != "mock", "live_reference")?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for (key, v) in map {
                path.push(key.clone());
                live_references(v, path)?;
                path.pop();
            }
            Ok(())
        }
        Value::Array(values) => {
            for (index, v) in values.iter().enumerate() {
                path.push(index.to_string());
                live_references(v, path)?;
                path.pop();
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

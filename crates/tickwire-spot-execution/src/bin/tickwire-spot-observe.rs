//! Reading the exchange never submits commands or changes local account state.
#[path = "tickwire-spot-observe/watch.rs"]
mod watch;
use serde_json::Value;
use std::{
    env,
    io::{self, Write},
    net::ToSocketAddrs,
    time::Duration,
};
#[cfg(test)]
use std::{
    io::{BufReader, Read},
    sync::Arc,
};
#[cfg(test)]
use tickwire_spot_execution::Credentials;
#[cfg(test)]
use tickwire_spot_execution::network::get_at;
use tickwire_spot_execution::{
    SpotVenue, authority,
    network::{OwnedCredentials, Result, get, roots, tls_at, venue_time},
    observe::Query,
};
#[cfg(test)]
use tickwire_transport::rustls;
use tickwire_venue::Environment;
const HELP: &str = "tickwire-spot-observe <bybit|okx> <test|production> <command>\n\nCommands\n  order SYMBOL CLIENT_ID\n  open-orders SYMBOL [CURSOR]\n  fills SYMBOL [CURSOR]\n  balance\n  scan EXPECTED_UID START_UTC_MS\n  watch EXPECTED_UID SECONDS (1..60)\n\nRead credentials from TICKWIRE_SPOT_KEY, TICKWIRE_SPOT_SECRET, plus TICKWIRE_SPOT_PASSPHRASE for OKX. Each variable may instead be supplied with its _FILE counterpart. Use read-only credentials. Set SSL_CERT_FILE to a trusted PEM CA bundle if the Linux system bundle is unavailable.\nSingle reads return one page. Scan follows standard spot order/fill pages for every instrument, checks the UID twice, and never authorizes trading. A query miss does not prove the order was never accepted.";
fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.as_slice() == ["--help"] || args.as_slice() == ["-h"] {
        println!("{HELP}");
        return Ok(());
    }
    let a: Vec<_> = args.iter().map(String::as_str).collect();
    let [venue, environment, rest @ ..] = a.as_slice() else {
        return Err(HELP.into());
    };
    let venue = match *venue {
        "bybit" => SpotVenue::Bybit,
        "okx" => SpotVenue::Okx,
        _ => return Err(HELP.into()),
    };
    let environment = match *environment {
        "test" => Environment::Test,
        "production" => Environment::Production,
        _ => return Err(HELP.into()),
    };
    enum Action<'a> {
        Page(Query<'a>),
        Scan(&'a str, u64),
        Watch(&'a str, u64),
    }
    let action = match rest {
        ["order", symbol, client_id] => Action::Page(Query::Order { symbol, client_id }),
        ["open-orders", symbol, rest @ ..] if rest.len() <= 1 => Action::Page(Query::OpenOrders {
            symbol: Some(symbol),
            cursor: rest.first().copied(),
        }),
        ["fills", symbol, rest @ ..] if rest.len() <= 1 => Action::Page(Query::Fills {
            symbol: Some(symbol),
            cursor: rest.first().copied(),
            window: None,
        }),
        ["balance"] => Action::Page(Query::Balance),
        ["scan", uid, start] => Action::Scan(
            uid,
            start.parse().map_err(|_| "invalid fill coverage start")?,
        ),
        ["watch", uid, seconds] => {
            let duration = seconds
                .parse::<u64>()
                .map_err(|_| "invalid watch duration")?;
            if !(1..=60).contains(&duration) {
                return Err("watch duration must be between 1 and 60 seconds".into());
            }
            Action::Watch(uid, duration)
        }
        ["new" | "cancel", ..] => {
            return Err("spot mutation requires a qualified private account frontier and an isolated live writer".into());
        }
        _ => return Err(HELP.into()),
    };
    if let Action::Page(query) = action {
        query.path(venue)?;
    }
    let owned = OwnedCredentials::load(venue)?;
    let credentials = owned.credentials();
    let host = authority(venue, environment);
    let tls = roots()?;
    let (ms, at) = venue_time(venue, host, tls.clone())?;
    let read = |query: Query<'_>| -> Result<Value> {
        if at.elapsed() > Duration::from_secs(120) {
            return Err("exchange clock sample expired during scan".into());
        }
        let stamp = ms
            .checked_add(at.elapsed().as_millis().try_into()?)
            .ok_or("venue timestamp overflow")?;
        let signed = query.signed_get(&credentials, environment, stamp)?;
        let response = get(host, tls.clone(), &signed)?;
        let status = response.status;
        query
            .evidence(venue, response)
            .map_err(|failure| format!("HTTP {status}, {failure}").into())
    };
    let mut page = match action {
        Action::Page(query) => read(query)?,
        Action::Watch(uid, seconds) => {
            let identity = read(Query::Identity)?;
            if identity["success"] != true {
                return Err(format!(
                    "identity HTTP {} venue code {}",
                    identity["http_status"], identity["venue_code"]
                )
                .into());
            }
            if identity["rows"][0]["uid"] != uid {
                return Err("authenticated user does not match expected venue UID".into());
            }
            let (host, path) = tickwire_spot_execution::private::endpoint(venue, environment);
            let stream = tls_at(host, (host, 443).to_socket_addrs()?, tls.clone())?;
            stream
                .sock
                .set_read_timeout(Some(Duration::from_millis(500)))?;
            let mut sink = io::stdout().lock();
            watch::capture(
                stream,
                host,
                path,
                &credentials,
                || ms.checked_add(at.elapsed().as_millis().try_into().ok()?),
                Duration::from_secs(seconds),
                |value| {
                    serde_json::to_writer(&mut sink, &value)?;
                    writeln!(sink)?;
                    sink.flush()?;
                    Ok(())
                },
            )?
        }
        Action::Scan(uid, start) => {
            tickwire_spot_execution::observe::scan(venue, uid, start, ms, read)?
        }
    };
    page["environment"] = serde_json::json!(if environment == Environment::Test {
        "test"
    } else {
        "production"
    });
    page["sampled_venue_ms"] = ms.into();
    let ok = page["success"] == true;
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &page)?;
    writeln!(out)?;
    if !ok {
        return Err("exchange did not return a successful observation".into());
    }
    Ok(())
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("spot observation: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::BufRead, net::TcpListener, thread};
    #[test]
    fn trusted_local_exchange_receives_a_signed_get_and_only_status_is_reported_on_denial() {
        for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
            let host = authority(venue, Environment::Test);
            let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
            let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
            let server = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap();
            let mut anchors = rustls::RootCertStore::empty();
            anchors.add(cert.cert.der().clone()).unwrap();
            let client = Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(anchors)
                    .with_no_client_auth(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let job = thread::spawn(move || {
                let (sock, _) = listener.accept().unwrap();
                sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let conn = rustls::ServerConnection::new(Arc::new(server)).unwrap();
                let mut wire = BufReader::new(rustls::StreamOwned::new(conn, sock));
                let mut head = String::new();
                loop {
                    let len = wire.read_line(&mut head).unwrap();
                    assert!(len > 0);
                    if head.ends_with("\r\n\r\n") {
                        break;
                    }
                }
                assert!(head.starts_with("GET "));
                assert!(head.contains(if venue == SpotVenue::Bybit {
                    "X-BAPI-SIGN:"
                } else {
                    "OK-ACCESS-SIGN:"
                }));
                wire.get_mut()
                    .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 6\r\n\r\nsecret")
                    .unwrap();
                wire.get_mut().flush().unwrap();
            });
            let q = Query::Balance;
            let cred = if venue == SpotVenue::Bybit {
                Credentials::bybit("key", b"secret")
            } else {
                Credentials::okx("key", b"secret", "pass")
            };
            let request = q
                .signed_get(&cred, Environment::Test, 1700000000123)
                .unwrap();
            let response = get_at(host, [addr], client, &request).unwrap();
            let report = q.evidence(venue, response).unwrap();
            assert_eq!(report["http_status"], 401);
            assert_eq!(report["success"], false);
            assert!(!report.to_string().contains("secret"));
            job.join().unwrap();
        }
    }
}

use super::*;
impl Owner {
    /// Starts the capture mode of the qualified SHM launcher.
    ///
    /// # Safety
    /// Approved build and qualification hashes identify qualified MD, strategy,
    /// and report SDKs, preserving immutable mappings and cursor ownership. The
    /// caller must establish this same contract as `RemoteHostedStrategy::launch`.
    #[allow(unsafe_code)]
    pub unsafe fn launch(mut config: Launch) -> Result<Self> {
        if config.spec.identity != config.bootstrap.market.endpoint.local.identity()
            || config.spec.identity != config.bootstrap.report.local.identity()
            || !config.spec.arguments.is_empty()
            || !config.spec.environment.is_empty()
            || config.parent_process.pid != std::process::id()
            || config.spec.uid != config.parent_process.uid
            || config.spec.gid != config.parent_process.gid
            || config.supervisor.role != 1
            || config.snapshot_id == [0; 16]
            || config.bootstrap.output_directory.exists()
        {
            return Err(Error::Invalid);
        }
        let mut clock = SystemClock::new(config.bootstrap.market.domain()?)?;
        let now = clock.sample()?.monotonic;
        config.bootstrap.validate(now)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_nanos(
                config
                    .bootstrap
                    .deadline_mono_ns
                    .checked_sub(now.nanos())
                    .ok_or(Error::Invalid)?,
            ))
            .ok_or(Error::Invalid)?;
        let directory = PrivateDirectory::create_in(&config.directory_parent, "tw-cap")?;
        config.bootstrap.report.path = directory.path().join("report.sock");
        config.bootstrap.market_admission_ready = directory.path().join("md-ready.json");
        config.bootstrap.report.peer_pid = config.parent_process.pid;
        config.bootstrap.report.peer_uid = config.parent_process.uid;
        config.bootstrap.report.peer_gid = config.parent_process.gid;
        let listener = ControlListener::bind(&config.bootstrap.report.path)?;
        let publisher = tickwire_shm::Publisher::<1>::create(ResourceHeader::decode(
            &config.bootstrap.report_header,
        )?)?;
        config.bootstrap.report_header = wire_bytes(publisher.header())?;
        let objects = tickwire_recovery::FileBlobStore::open(&config.bootstrap.report_directory)?;
        config.bootstrap.validate(clock.sample()?.monotonic)?;
        let bootstrap_file = directory.path().join("capture.json");
        write_json(&bootstrap_file, &config.bootstrap)?;
        config.spec.arguments = vec![
            "--capture-only".into(),
            bootstrap_file.to_str().ok_or(Error::Invalid)?.into(),
        ];
        let mut registry = Registry::new(1)?;
        let child = registry.spawn(config.spec)?;
        let admission = tickwire_md::server::Admission {
            client: config.bootstrap.market.clone(),
            pid: child.pid,
            uid: child.uid,
            gid: child.gid,
            supervisor: (&config.supervisor).into(),
            supervisor_pid: config.parent_process.pid,
            snapshot_id: config.snapshot_id,
        };
        tickwire_md::server::register_admission(&config.md_admission_directory, &admission)
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        write_json(&config.bootstrap.market_admission_ready, &true)?;
        let mut report = registry.accept(
            &listener,
            server_policy(&config.bootstrap.report, child),
            clock.sample()?.monotonic.nanos(),
            deadline,
        )?;
        let subscription = tickwire_shm::SubscriptionKey {
            bank: publisher.key(),
            instance: u128::from_le_bytes(config.bootstrap.identity.instance),
            generation: config.bootstrap.report_subscription_generation,
            cursor: u128::from_le_bytes(config.bootstrap.report_cursor_id),
        };
        // SAFETY: this qualified launcher owns the exact authenticated Registry child.
        let reports = unsafe {
            remote::connect_reports(
                publisher,
                objects,
                subscription,
                ReportIdentity {
                    logical_strategy_id: config.bootstrap.identity.logical_strategy,
                    instance_id: config.bootstrap.identity.instance,
                    strategy_generation: config.bootstrap.identity.generation,
                    risk_domain: config.bootstrap.identity.risk_domain,
                },
                &mut report,
                &mut clock,
                deadline,
            )?
        };
        let mut owner = Self {
            bootstrap: config.bootstrap,
            registry,
            child,
            report,
            reports,
            subscription,
            clock,
            request: None,
            outgoing: None,
            captured: None,
            failed: false,
            stopped: false,
            _directory: directory,
            _listener: listener,
        };
        owner.current()?;
        Ok(owner)
    }
}

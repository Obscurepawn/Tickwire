//! Authenticated requests executed by the actual consensus/application owner.
use crate::{
    Error, Hash, ReplicaControl, Result,
    api::{Endpoint, Request},
    auth::{AuthenticatedPrincipal, AuthenticatedStream, ServerTls},
    authorization::RoleBindings,
    bootstrap::{CommittedHead, HeadRead},
    deployment_owner::{Assessment, Job, Owner as DeploymentOwner},
};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::io::Write;
use tickwire_config::{
    Candidate, Format, Limits,
    changes::Authority,
    patch::{Head, ScopedPatch},
};
use tickwire_control_raft::ReadId;
use tickwire_journal::store::Store;

// A maximum-size canonical candidate still needs its bounded revision envelope.
const MAX_RESPONSE_BYTES: usize = crate::api::MAX_BODY_BYTES + 4096;
const PENDING_CHECKS: &[&str] = &[
    "artifact_closure",
    "parameter_schemas",
    "host_inventory",
    "retained_resources",
    "device_qualification",
    "account_modes",
    "credential_references",
    "runtime_targets",
];
pub struct PendingRequest {
    ticket: Option<ReadId>,
    principal: AuthenticatedPrincipal,
    request: Option<Request>,
    request_id: String,
    deployment: Option<PendingDeployment>,
}
struct PendingDeployment {
    base_etag: String,
    candidate_hash: Hash,
    stage: DeploymentStage,
}
enum DeploymentStage {
    Running(Job),
    Ready(Result<Box<Assessment>>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Reading,
    Validating,
    Completed,
}
impl PendingRequest {
    pub fn progress(&self) -> Progress {
        if self.request.is_none() {
            Progress::Completed
        } else if self.ticket.is_some() {
            Progress::Reading
        } else {
            Progress::Validating
        }
    }
}
pub struct Response {
    status: u16,
    body: Vec<u8>,
    etag: Option<String>,
    subject: String,
    leaf: Hash,
}
impl Response {
    pub fn status(&self) -> u16 {
        self.status
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn write(self, stream: &mut AuthenticatedStream) -> Result<()> {
        if stream.principal().subject() != self.subject
            || stream.principal().leaf_sha256() != self.leaf
        {
            return Err(Error::Authentication);
        }
        let reason = match self.status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            412 => "Precondition Failed",
            413 => "Content Too Large",
            422 => "Unprocessable Content",
            428 => "Precondition Required",
            429 => "Too Many Requests",
            503 => "Service Unavailable",
            _ => return Err(Error::Protocol),
        };
        let etag = self
            .etag
            .map_or_else(String::new, |value| format!("ETag: {value}\r\n"));
        write!(
            stream,
            "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\n{etag}Connection: close\r\n\r\n",
            self.status,
            self.body.len()
        )
        .and_then(|()| stream.write_all(&self.body))
        .and_then(|()| stream.flush())
        .map_err(|_| Error::Transport)
    }
    fn new(status: u16, value: Value, pending: &PendingRequest) -> Result<Self> {
        Self::from_value(status, value, &pending.principal)
    }
    fn from_value(status: u16, value: Value, principal: &AuthenticatedPrincipal) -> Result<Self> {
        let body = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(Error::Bounds);
        }
        Ok(Self {
            status,
            body,
            etag: None,
            subject: principal.subject().to_owned(),
            leaf: principal.leaf_sha256(),
        })
    }
    fn error(status: u16, code: &'static str, pending: &PendingRequest) -> Result<Self> {
        Self::error_with_id(status, code, &pending.principal, &pending.request_id)
    }
    pub(crate) fn error_for(
        status: u16,
        code: &'static str,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Self> {
        Self::error_with_id(status, code, principal, &request_id()?)
    }
    pub(crate) fn unavailable(pending: &PendingRequest) -> Result<Self> {
        Self::error(503, "ControlUnavailable", pending)
    }
    fn error_with_id(
        status: u16,
        code: &'static str,
        principal: &AuthenticatedPrincipal,
        request_id: &str,
    ) -> Result<Self> {
        Self::from_value(
            status,
            json!({
                "code": code,
                "message": match code {
                    "Unauthorized" => "客户端证书当前不可用。",
                    "PermissionDenied" => "当前角色不具备所需作用域权限。",
                    "VersionConflict" => "基础配置版本已变化。",
                    "RequestTooLarge" => "请求超过配置大小上限。",
                    "Malformed" => "请求格式无效。",
                    "NotFound" => "请求的配置版本不存在。",
                    "UnsupportedEndpoint" => "此接口的业务状态处理尚未实现。",
                    _ => "控制状态或所需处理能力尚未就绪。",
                },
                "request_id":request_id,
                "retryable":status==503,
                "field_errors":[],
            }),
            principal,
        )
    }
}
impl<S: Store> ReplicaControl<'_, S> {
    pub fn cancel_request(&mut self, pending: &mut PendingRequest) -> Result<bool> {
        if pending.request.is_none() {
            return Ok(false);
        }
        if let Some(ticket) = pending.ticket.take() {
            self.cancel_read(&ticket)?;
        }
        pending.request.take();
        pending.deployment.take();
        Ok(true)
    }
    pub fn begin_request(
        &mut self,
        trust: &ServerTls,
        principal: &AuthenticatedPrincipal,
        request: Request,
    ) -> Result<PendingRequest> {
        trust.revalidate(principal)?;
        let request_id = request_id()?;
        Ok(PendingRequest {
            ticket: Some(self.request_read()?),
            principal: principal.clone(),
            request: Some(request),
            request_id,
            deployment: None,
        })
    }
    pub fn poll_request(
        &mut self,
        trust: &ServerTls,
        pending: &mut PendingRequest,
    ) -> Result<Option<Response>> {
        if pending.request.is_none() {
            return Err(Error::NotReady);
        }
        if trust.revalidate(&pending.principal).is_err() {
            self.cancel_request(pending)?;
            return Response::error(401, "Unauthorized", pending).map(Some);
        }
        if let Some(work) = pending.deployment.as_mut()
            && let DeploymentStage::Running(job) = &mut work.stage
        {
            let outcome = match self.deployment_owner() {
                Some(owner) => owner.poll(job),
                None => Err(Error::NotReady),
            };
            let outcome = match outcome {
                Ok(None) => return Ok(None),
                Ok(Some(assessment)) => Ok(assessment),
                Err(error) => Err(error),
            };
            work.stage = DeploymentStage::Ready(outcome.map(Box::new));
            pending.ticket = Some(self.request_read()?);
            return Ok(None);
        }
        let head = match self.read_head(pending.ticket.as_ref().ok_or(Error::NotReady)?)? {
            HeadRead::Pending => return Ok(None),
            HeadRead::Ready(None) => {
                pending.request.take();
                pending.ticket.take();
                pending.deployment.take();
                return Response::error(503, "ControlUnavailable", pending).map(Some);
            }
            HeadRead::Ready(Some(head)) => head,
        };
        pending.ticket.take();
        let authority =
            RoleBindings::from_candidate(head.candidate())?.authorize(trust, &pending.principal);
        let request = pending.request.take().ok_or(Error::NotReady)?;
        let authority = match authority {
            Ok(authority) => authority,
            Err(Error::Authentication) => {
                return Response::error(401, "Unauthorized", pending).map(Some);
            }
            Err(Error::Authorization) => {
                return Response::error(403, "PermissionDenied", pending).map(Some);
            }
            Err(error) => return Err(error),
        };
        if authority
            .authorize_read(&Default::default(), false)
            .is_err()
        {
            return Response::error(403, "PermissionDenied", pending).map(Some);
        }
        let max_request_bytes = head.candidate().value()["control"]["max_request_bytes"]
            .as_u64()
            .ok_or(Error::Protocol)?;
        if request.body_bytes() as u64 > max_request_bytes {
            return Response::error(413, "RequestTooLarge", pending).map(Some);
        }
        if let Some(work) = pending.deployment.take() {
            if request
                .body()
                .is_some_and(|b| b["candidate"]["format"] != "scoped-patch")
                && authority.authorize_read(&Default::default(), true).is_err()
            {
                return Response::error(403, "PermissionDenied", pending).map(Some);
            }
            if head.etag() != work.base_etag {
                return Response::error(412, "VersionConflict", pending).map(Some);
            }
            let DeploymentStage::Ready(outcome) = work.stage else {
                return Err(Error::Protocol);
            };
            let completed = match outcome {
                Ok(assessment) => {
                    if assessment.content().candidate_hash() != work.candidate_hash {
                        return Err(Error::Protocol);
                    }
                    self.deployment_owner()
                        .is_some_and(|owner| owner.revalidate(&assessment).is_ok())
                }
                Err(_) => false,
            };
            return content_response(work.candidate_hash, completed, pending).map(Some);
        }
        let response = match request.endpoint() {
            Endpoint::Head => committed_response(&head, false, pending),
            Endpoint::Desired | Endpoint::Revision(_) => {
                if authority.authorize_read(&Default::default(), true).is_err() {
                    Response::error(403, "PermissionDenied", pending)
                } else if matches!(request.endpoint(), Endpoint::Revision(revision) if *revision != head.revision())
                {
                    Response::error(404, "NotFound", pending)
                } else {
                    committed_response(&head, true, pending)
                }
            }
            Endpoint::Schema => Response::new(
                200,
                serde_json::from_str(include_str!(
                    "../../../specs/contracts/system-config.schema.json"
                ))
                .map_err(|_| Error::Protocol)?,
                pending,
            ),
            Endpoint::Validate => {
                if request
                    .body()
                    .is_some_and(|b| b["validation_level"] == "deployment")
                    && let Some(owner) = self.deployment_owner()
                {
                    match start_content(&head, &request, &authority, pending, owner)? {
                        StartedContent::Response(response) => Ok(response),
                        StartedContent::Pending(work) => {
                            pending.deployment = Some(work);
                            pending.request = Some(request);
                            return Ok(None);
                        }
                    }
                } else {
                    validate(&head, &request, &authority, pending)
                }
            }
            Endpoint::Plan(_)
            | Endpoint::PlanDiff(_)
            | Endpoint::Effective(_)
            | Endpoint::Operation(_)
            | Endpoint::OperationByKey(_)
            | Endpoint::Watch => Response::error(503, "UnsupportedEndpoint", pending),
            _ => Response::error(503, "ControlUnavailable", pending),
        }?;
        Ok(Some(response))
    }
}
fn committed_response(
    head: &CommittedHead,
    include_config: bool,
    pending: &PendingRequest,
) -> Result<Response> {
    let hash = head
        .candidate()
        .hash()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut value = json!({
        "origin":"bootstrap",
        "revision":head.revision().to_string(),
        "config_hash":format!("sha256:{hash}"),
        "etag":head.etag(),
        "control_commit_ref":format!("raft:{}:{}",head.raft_term(),head.raft_index()),
    });
    if include_config {
        value["config"] = head.candidate().value().clone();
    }
    let mut response = Response::new(200, value, pending)?;
    response.etag = Some(head.etag());
    Ok(response)
}
enum StartedContent {
    Response(Response),
    Pending(PendingDeployment),
}
fn start_content(
    head: &CommittedHead,
    request: &Request,
    authority: &Authority,
    pending: &PendingRequest,
    owner: &mut DeploymentOwner,
) -> Result<StartedContent> {
    let body = request.body().ok_or(Error::Protocol)?;
    if body["candidate"]["format"] != "scoped-patch"
        && authority.authorize_read(&Default::default(), true).is_err()
    {
        return Response::error(403, "PermissionDenied", pending).map(StartedContent::Response);
    }
    let candidate = parse_candidate(head, request, authority).and_then(|candidate| {
        let candidate = candidate.normalized_configuration(Limits::default())?;
        candidate.validate_static()?;
        Ok(candidate)
    });
    let candidate = match candidate {
        Ok(candidate) => candidate,
        Err(_) => return validate(head, request, authority, pending).map(StartedContent::Response),
    };
    let candidate_hash = candidate.hash();
    match owner.start(candidate) {
        Ok(job) => Ok(StartedContent::Pending(PendingDeployment {
            base_etag: head.etag(),
            candidate_hash,
            stage: DeploymentStage::Running(job),
        })),
        Err(_) => Response::error(503, "ControlUnavailable", pending).map(StartedContent::Response),
    }
}
fn content_response(hash: Hash, completed: bool, pending: &PendingRequest) -> Result<Response> {
    let mut checks = vec![
        "strict_parse",
        "fixed_schema",
        "explicit_defaults",
        "static_semantics",
    ];
    if completed {
        checks.extend([
            "artifact_closure",
            "parameter_schemas",
            "projection_set",
            "host_inventory",
            "retained_capacity",
            "device_qualification",
            "account_modes",
            "credential_references",
        ]);
    }
    let pending_checks = if completed {
        vec!["retained_registry_head", "runtime_targets"]
    } else {
        PENDING_CHECKS.to_vec()
    };
    Response::new(
        200,
        json!({
            "valid":false,"validation_level":"deployment",
            "config_hash":format!("sha256:{}",hash.iter().map(|b|format!("{b:02x}")).collect::<String>()),
            "issues":[{"path":"","code":if completed{"DeploymentValidationPending"}else{"DeploymentContentFailed"},
                "severity":"error","message":if completed{"当前资源登记头与运行实例尚未完成核验。"}else{"实际资料未通过当前内容、来源或有效期核验。"}}],
            "checks_completed":checks,"checks_pending":pending_checks,
        }),
        pending,
    )
}

fn request_id() -> Result<String> {
    let mut identity = [0; 16];
    SystemRandom::new()
        .fill(&mut identity)
        .map_err(|_| Error::Transport)?;
    identity[6] = (identity[6] & 15) | 64;
    identity[8] = (identity[8] & 63) | 128;
    let hex: String = identity.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

fn validate(
    head: &CommittedHead,
    request: &Request,
    authority: &Authority,
    pending: &PendingRequest,
) -> Result<Response> {
    let body = request.body().ok_or(Error::Protocol)?;
    let format = body["candidate"]["format"]
        .as_str()
        .ok_or(Error::Protocol)?;
    let level = body["validation_level"].as_str().ok_or(Error::Protocol)?;
    if format != "scoped-patch" && authority.authorize_read(&Default::default(), true).is_err() {
        return Response::error(403, "PermissionDenied", pending);
    }
    let candidate = parse_candidate(head, request, authority).and_then(|candidate| {
        let candidate = candidate.normalized_configuration(Limits::default())?;
        candidate.validate_static()?;
        Ok(candidate)
    });
    let candidate = match candidate {
        Ok(candidate) => candidate,
        Err(tickwire_config::Error::Semantic("authorization")) => {
            return Response::error(403, "PermissionDenied", pending);
        }
        Err(tickwire_config::Error::Revision) => {
            return Response::error(412, "VersionConflict", pending);
        }
        Err(tickwire_config::Error::Capacity) => {
            return Response::error(413, "RequestTooLarge", pending);
        }
        Err(_) => {
            return Response::new(
                200,
                json!({
                    "valid":false,"validation_level":level,
                    "issues":[{"path":"","code":"StructuralValidationFailed","severity":"error","message":"候选未通过结构或静态语义校验。"}],
                    "checks_completed":[],"checks_pending":PENDING_CHECKS,
                }),
                pending,
            );
        }
    };
    let hash: String = candidate
        .hash()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Response::new(
        200,
        json!({
            "valid":level=="structural",
            "validation_level":level,
            "config_hash":format!("sha256:{hash}"),
            "issues":if level=="deployment" {
                json!([{"path":"","code":"DeploymentValidationPending","severity":"error","message":"实际部署依赖尚未完成核验。"}])
            } else {json!([])},
            "checks_completed":["strict_parse","fixed_schema","explicit_defaults","static_semantics"],
            "checks_pending":PENDING_CHECKS,
        }),
        pending,
    )
}
fn parse_candidate(
    head: &CommittedHead,
    request: &Request,
    authority: &Authority,
) -> tickwire_config::Result<Candidate> {
    let body = request.body().ok_or(tickwire_config::Error::Schema)?;
    let document = &body["candidate"]["document"];
    match body["candidate"]["format"].as_str() {
        Some("toml") => Candidate::parse(
            Format::Toml,
            document
                .as_str()
                .ok_or(tickwire_config::Error::Schema)?
                .as_bytes(),
            Limits::default(),
        ),
        Some("json") => Candidate::parse(
            Format::Json,
            &serde_json::to_vec(document).map_err(|_| tickwire_config::Error::Encoding)?,
            Limits::default(),
        ),
        Some("scoped-patch") => {
            let patch = ScopedPatch::parse(
                &serde_json::to_vec(document).map_err(|_| tickwire_config::Error::Encoding)?,
                Limits::default(),
            )?;
            patch.apply(
                &Head::new(head.revision(), head.candidate())?,
                patch.base_etag(),
                head.candidate(),
                authority,
            )
        }
        _ => Err(tickwire_config::Error::Schema),
    }
}

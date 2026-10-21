//! Bounded bootstrap HTTP acquisition. Exact decoded bodies are stored under the
//! native metadata roles; selected transport-provided headers are diagnostics,
//! not raw HTTP/TLS evidence or historical strategy availability.
use super::*;
use marketfeed_adapter_api::Environment;
use marketfeed_transport::{HttpMethod, HttpRequest, HttpTransport, ReqwestHttpTransport};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const ACQUISITION_ROLE: &str = "hyperliquid-bootstrap-http-acquisition-v1";
#[derive(Serialize)]
struct ResponseEvidence {
    role: &'static str,
    url: String,
    request_method: &'static str,
    request_body_sha256: String,
    status: u16,
    selected_response_headers: Vec<(String, String)>,
    decoded_body_bytes: u64,
    decoded_body_sha256: String,
    started_unix_ns: i64,
    observed_unix_ns: i64,
    elapsed_monotonic_ns: u64,
}
#[derive(Serialize)]
struct Acquisition {
    schema_version: u16,
    capture_request_sha256: String,
    body_scope: &'static str,
    availability_time_ns: Option<i64>,
    responses: Vec<ResponseEvidence>,
}
/// Only the concrete public client creates this value outside module tests.
/// There is no caller-supplied successful-response or HTTP receipt flag.
pub struct AcquiredDiscovery {
    spot: Vec<u8>,
    perps: Option<Vec<u8>>,
    request_sha256: String,
    evidence: PublicReproductionInputDto,
}
impl AcquiredDiscovery {
    pub fn prepare(&self, request: &CaptureRequest) -> Result<Vec<BootstrapDto>, BootstrapError> {
        let bytes = serde_json::to_vec(request).map_err(CodecError::from)?;
        if bootstrap_sha256(&bytes) != self.request_sha256 {
            return Err(BootstrapError::Unsupported(
                "capture request changed after discovery",
            ));
        }
        super::prepare(
            request,
            DiscoveryBodies {
                spot: &self.spot,
                perps: self.perps.as_deref(),
            },
            self.evidence.clone(),
        )
    }
}
fn wall() -> Result<i64, BootstrapError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .ok_or(BootstrapError::Unsupported(
            "bootstrap HTTP clock outside supported range",
        ))
}
pub async fn acquire_public(request: &CaptureRequest) -> Result<AcquiredDiscovery, BootstrapError> {
    // Refuse a disabled/unbound artifact before any public network operation.
    current_build()?;
    request.validate()?;
    let http = ReqwestHttpTransport::new_without_redirects()
        .map_err(|e| BootstrapError::Factory(e.to_string()))?;
    acquire(request, &http).await
}
async fn acquire<H: HttpTransport>(
    request: &CaptureRequest,
    http: &H,
) -> Result<AcquiredDiscovery, BootstrapError> {
    request.validate()?;
    let factory = HyperliquidFactory {
        spot: !request
            .markets
            .iter()
            .any(|m| m.market == HyperliquidMarketDto::Perps),
    };
    let specs = factory
        .instrument_requests(Environment::Production)
        .map_err(|e| BootstrapError::Factory(e.to_string()))?;
    if specs.is_empty() || specs.len() > 2 {
        return Err(BootstrapError::Unsupported("bootstrap HTTP plan capacity"));
    }
    let request_sha256 = bootstrap_sha256(&serde_json::to_vec(request).map_err(CodecError::from)?);
    let mut evidence = Acquisition {
        schema_version: 1,
        capture_request_sha256: request_sha256.clone(),
        body_scope: "exact decoded body; selected text headers from transport; no raw HTTP/TLS or availability attestation",
        availability_time_ns: None,
        responses: Vec::new(),
    };
    let mut bodies = Vec::new();
    for (index, spec) in specs.into_iter().enumerate() {
        if spec.url != "https://api.hyperliquid.xyz/info"
            || spec.method != marketfeed_adapter_api::HttpMethod::Post
        {
            return Err(BootstrapError::Unsupported(
                "unsupported public metadata endpoint/method",
            ));
        }
        let role = if index == 0 {
            HYPERLIQUID_SPOT_META_ROLE
        } else {
            HYPERLIQUID_PERP_METAS_ROLE
        };
        let started_unix_ns = wall()?;
        let start = Instant::now();
        let request_body_sha256 = bootstrap_sha256(spec.body.as_deref().unwrap_or_default());
        let url = spec.url.clone();
        let response = http
            .request(HttpRequest {
                method: HttpMethod::Post,
                url: spec.url,
                headers: spec.headers,
                body: spec.body,
                timeout_ms: 15_000,
                max_body_bytes: MAX_METADATA_BYTES,
            })
            .await
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        let observed_unix_ns = wall()?;
        if response.status != 200
            || response.body.is_empty()
            || response.body.len() > MAX_METADATA_BYTES
        {
            return Err(BootstrapError::Unsupported(
                "metadata HTTP status/body capacity",
            ));
        }
        if response.headers.len() > 128
            || response
                .headers
                .iter()
                .try_fold(0usize, |n, (k, v)| {
                    n.checked_add(k.len())?.checked_add(v.len())
                })
                .is_none_or(|n| n > 32 << 10)
        {
            return Err(BootstrapError::Unsupported(
                "metadata response header capacity",
            ));
        }
        let headers = response
            .headers
            .into_iter()
            .filter(|(name, _)| {
                matches!(
                    name.to_ascii_lowercase().as_str(),
                    "date" | "content-type" | "etag" | "last-modified"
                )
            })
            .collect::<Vec<_>>();
        if headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            > 8192
        {
            return Err(BootstrapError::Unsupported(
                "metadata diagnostic header capacity",
            ));
        }
        evidence.responses.push(ResponseEvidence {
            role,
            url,
            request_method: "post",
            request_body_sha256,
            status: response.status,
            selected_response_headers: headers,
            decoded_body_bytes: response.body.len() as u64,
            decoded_body_sha256: bootstrap_sha256(&response.body),
            started_unix_ns,
            observed_unix_ns,
            elapsed_monotonic_ns: u64::try_from(start.elapsed().as_nanos())
                .map_err(|_| BootstrapError::Unsupported("HTTP monotonic clock overflow"))?,
        });
        bodies.push(response.body.to_vec());
    }
    let bytes = serde_json::to_vec(&evidence).map_err(CodecError::from)?;
    if bytes.len() > 64 << 10 {
        return Err(BootstrapError::Unsupported(
            "bootstrap HTTP evidence capacity",
        ));
    }
    let spot = bodies.remove(0);
    Ok(AcquiredDiscovery {
        spot,
        perps: bodies.pop(),
        request_sha256,
        evidence: PublicReproductionInputDto::new(ACQUISITION_ROLE, &bytes),
    })
}

#[cfg(all(test, feature = "causal-journal"))]
mod tests {
    use super::*;
    use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};
    fn request() -> CaptureRequest {
        CaptureRequest {
            schema_version: 1,
            markets: vec![MarketSelection {
                market: HyperliquidMarketDto::Spot,
                coins: vec!["@12".into()],
                channels: vec![ChannelDto::Trades],
            }],
            duration_ms: 1000,
            source_max_bytes: 64 << 20,
            target_max_bytes: 64 << 20,
            projection_batch_groups: 1,
        }
    }
    const SPOT: &[u8] = br#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":[{"name":"@12","tokens":[7,0],"index":12}]}"#;
    #[tokio::test]
    async fn metadata_is_exact_bound_to_request_and_private_headers_are_not_copied() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "/info",
            HttpResponse {
                status: 200,
                headers: vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("Set-Cookie".into(), "synthetic-not-provenance".into()),
                ],
                body: SPOT.to_vec().into(),
            },
        );
        let request = request();
        let acquired = acquire(&request, &http).await.unwrap();
        let dtos = acquired.prepare(&request).unwrap();
        assert_eq!(
            dtos[0]
                .public_inputs
                .iter()
                .find(|p| p.role == HYPERLIQUID_SPOT_META_ROLE)
                .unwrap()
                .payload
                .bytes()
                .unwrap(),
            SPOT
        );
        let raw = acquired.evidence.payload.bytes().unwrap();
        let text = String::from_utf8(raw).unwrap();
        assert!(text.contains("application/json"));
        assert!(!text.contains("synthetic-not-provenance"));
        assert!(text.contains("\"availability_time_ns\":null"));
        let mut changed = request;
        changed.duration_ms += 1;
        assert!(acquired.prepare(&changed).is_err());
    }
    #[tokio::test]
    async fn non_success_redirect_and_oversize_metadata_fail_closed() {
        for (status, body) in [
            (302, SPOT.to_vec()),
            (500, SPOT.to_vec()),
            (200, Vec::new()),
            (200, vec![b'x'; MAX_METADATA_BYTES + 1]),
        ] {
            let http = ScriptedHttpTransport::new();
            http.push(
                "/info",
                HttpResponse {
                    status,
                    headers: Vec::new(),
                    body: body.into(),
                },
            );
            assert!(acquire(&request(), &http).await.is_err());
        }
    }
}

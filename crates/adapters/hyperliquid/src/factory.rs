use crate::{HyperliquidSession, instruments, specification::*};
use bytes::Bytes;
use marketfeed_adapter_api::*;
use marketfeed_model::*;
/// Perpetual discovery includes the native clearinghouse and builder-deployed
/// DEXs. Namespaced native symbols and per-DEX collateral remain distinct.
#[derive(Debug, Clone)]
pub struct HyperliquidFactory {
    pub spot: bool,
}
impl HyperliquidFactory {
    pub fn spot() -> Self {
        Self { spot: true }
    }
    pub fn perps() -> Self {
        Self { spot: false }
    }
}
impl VenueFactory for HyperliquidFactory {
    fn specification(&self) -> &'static VenueSpecification {
        if self.spot {
            &HYPERLIQUID_SPOT_SPEC
        } else {
            &HYPERLIQUID_PERPS_SPEC
        }
    }
    fn instrument_requests(&self, env: Environment) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        if env != Environment::Production {
            return Err(AdapterError::UnsupportedCapability(
                "Hyperliquid production only".into(),
            ));
        }
        let mut kinds = vec!["spotMeta"];
        if !self.spot {
            kinds.push("allPerpMetas");
        }
        Ok(kinds
            .into_iter()
            .enumerate()
            .map(|(i, kind)| HttpRequestSpec {
                id: i as u64 + 1,
                method: HttpMethod::Post,
                url: INFO_URL.into(),
                headers: vec![("Content-Type".into(), "application/json".into())],
                body: Some(Bytes::from(serde_json::json!({"type":kind}).to_string())),
            })
            .collect())
    }
    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(instruments::parse_instruments(responses, self.spot)?);
        Ok(())
    }
    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != self.specification().id {
            return Err(AdapterError::Catalog("wrong Hyperliquid venue".into()));
        }
        let wire = crate::session::subscriptions(request, catalog, self.spot)?;
        if wire.len() > 1000 {
            return Err(AdapterError::Subscription(
                "Hyperliquid subscription budget exceeded".into(),
            ));
        }
        if wire.is_empty() {
            return Err(AdapterError::Subscription(
                "empty Hyperliquid subscription plan".into(),
            ));
        }
        // Keep every native subscription in exactly one session. Shared asset
        // context requests (funding/mark/OI) must not fan out duplicate feeds.
        let mut groups: std::collections::BTreeMap<String, Vec<ConcreteSubscription>> =
            std::collections::BTreeMap::new();
        for item in &request.items {
            let inst = catalog
                .instruments
                .iter()
                .find(|i| i.id == item.instrument)
                .expect("validated catalog item");
            let key =
                crate::session::subscription(&item.channel, &inst.key.native_symbol, self.spot)?
                    .to_string();
            groups.entry(key).or_default().push(item.clone());
        }
        // Engine pending-write capacity is 256, including heartbeat/control.
        let groups: Vec<_> = groups.into_values().collect();
        Ok(groups
            .chunks(200)
            .map(|chunk| SessionSpec {
                endpoint_name: WS_URL.into(),
                subscriptions: ConcreteSubscriptionSet {
                    items: chunk.iter().flatten().cloned().collect(),
                },
            })
            .collect())
    }
    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        if spec.endpoint_name != WS_URL {
            return Err(AdapterError::Subscription(
                "unknown Hyperliquid endpoint".into(),
            ));
        }
        self.plan(&spec.subscriptions, &catalog)?;
        Ok(Box::new(HyperliquidSession::new(
            self.spot,
            spec.subscriptions,
            catalog,
        )?))
    }
}

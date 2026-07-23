//! Venue factory: instruments, planning, session construction.

use marketfeed_model::{CatalogView, InstrumentDefinition};

use crate::{
    AdapterError, ConcreteSubscriptionSet, Environment, HttpRequestSpec, HttpResponse,
    SessionMachine, SessionSpec, VenueSpecification,
};

/// Creates deterministic session machines for one venue family.
pub trait VenueFactory: Send + Sync + 'static {
    fn specification(&self) -> &'static VenueSpecification;

    fn instrument_requests(
        &self,
        environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError>;

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<InstrumentDefinition>,
    ) -> Result<(), AdapterError>;

    /// Optional N+1 detail requests after primary `parse_instruments`.
    ///
    /// Default: none. Factories that fan out must cap (no unbounded N+1).
    fn instrument_detail_requests(
        &self,
        _defs: &[InstrumentDefinition],
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(Vec::new())
    }

    /// Apply detail responses onto `defs` (match semantics are factory-owned).
    fn apply_instrument_details(
        &self,
        _responses: &[HttpResponse],
        _defs: &mut [InstrumentDefinition],
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError>;

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError>;
}

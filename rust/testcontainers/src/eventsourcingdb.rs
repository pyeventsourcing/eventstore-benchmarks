use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

const NAME: &str = "thenativeweb/eventsourcingdb";
const TAG: &str = "1.2.0";

/// Container port exposed by EventsourcingDB (HTTP).
pub const EVENTSOURCINGDB_PORT: ContainerPort = ContainerPort::Tcp(3000);

/// Default API token used for the benchmarking container.
pub const EVENTSOURCINGDB_API_TOKEN: &str = "secret";

#[derive(Debug, Clone)]
pub struct EventsourcingDb;

impl Default for EventsourcingDb {
    fn default() -> Self {
        Self
    }
}

impl Image for EventsourcingDb {
    fn name(&self) -> &str {
        NAME
    }
    fn tag(&self) -> &str {
        TAG
    }
    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }
    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec!["run", "--data-directory-temporary", "--https-enabled=false", "--http-enabled", "--api-token", EVENTSOURCINGDB_API_TOKEN]
    }
    fn expose_ports(&self) -> &[ContainerPort] {
        &[EVENTSOURCINGDB_PORT]
    }
}

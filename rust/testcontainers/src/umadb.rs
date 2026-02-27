use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

const NAME: &str = "umadb/umadb";
const TAG: &str = "latest";

/// Container port exposed by UmaDB (gRPC).
pub const UMADB_PORT: ContainerPort = ContainerPort::Tcp(50051);

#[derive(Debug, Clone, Default)]
pub struct UmaDb;

impl Image for UmaDb {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[UMADB_PORT]
    }
}

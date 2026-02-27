use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

const NAME: &str = "docker.kurrent.io/kurrent-latest/kurrentdb";
const TAG: &str = "25.1.0-x64-8.0-bookworm-slim";

/// Container port exposed by KurrentDB (HTTP/gRPC).
pub const KURRENTDB_PORT: ContainerPort = ContainerPort::Tcp(2113);

#[derive(Debug, Clone)]
pub struct KurrentDb {
    env_vars: Vec<(String, String)>,
}

impl Default for KurrentDb {
    fn default() -> Self {
        Self {
            env_vars: vec![
                ("KURRENTDB_INSECURE".to_string(), "true".to_string()),
                ("KURRENTDB_RUN_PROJECTIONS".to_string(), "All".to_string()),
                (
                    "KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP".to_string(),
                    "true".to_string(),
                ),
            ],
        }
    }
}

impl Image for KurrentDb {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<std::borrow::Cow<'_, str>>, impl Into<std::borrow::Cow<'_, str>>)>
    {
        self.env_vars
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[KURRENTDB_PORT]
    }
}

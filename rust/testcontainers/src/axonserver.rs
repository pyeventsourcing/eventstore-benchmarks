use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::Image;

const NAME: &str = "axoniq/axonserver";
const TAG: &str = "latest";

/// gRPC API port exposed by Axon Server.
pub const AXONSERVER_GRPC_PORT: ContainerPort = ContainerPort::Tcp(8124);

/// HTTP/Dashboard port exposed by Axon Server.
pub const AXONSERVER_HTTP_PORT: ContainerPort = ContainerPort::Tcp(8024);

#[derive(Debug, Clone)]
pub struct AxonServer {
    env_vars: Vec<(String, String)>,
}

impl Default for AxonServer {
    fn default() -> Self {
        Self {
            env_vars: vec![
                (
                    "AXONIQ_AXONSERVER_NAME".to_string(),
                    "bench-axon-server".to_string(),
                ),
                (
                    "AXONIQ_AXONSERVER_HOSTNAME".to_string(),
                    "bench-axon-server".to_string(),
                ),
                (
                    "AXONIQ_AXONSERVER_STANDALONE_DCB".to_string(),
                    "true".to_string(),
                ),
            ],
        }
    }
}

impl Image for AxonServer {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Started AxonServer")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<
        Item = (
            impl Into<std::borrow::Cow<'_, str>>,
            impl Into<std::borrow::Cow<'_, str>>,
        ),
    > {
        self.env_vars.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[AXONSERVER_GRPC_PORT, AXONSERVER_HTTP_PORT]
    }
}

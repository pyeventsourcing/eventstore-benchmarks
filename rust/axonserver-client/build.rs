fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &["protos/dcb.proto", "protos/axonclient/protos/common.proto"],
            &["protos"],
        )?;
    Ok(())
}

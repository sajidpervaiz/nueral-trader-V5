fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = manifest_dir.join("../../proto");
    let proto_file = proto_dir.join("bridge.proto");

    tonic_build::configure().compile(&[proto_file], &[proto_dir])?;
    Ok(())
}

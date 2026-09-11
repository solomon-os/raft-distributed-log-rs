fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/raft.proto")?;
    tonic_prost_build::compile_protos("proto/client.proto")?;
    Ok(())
}

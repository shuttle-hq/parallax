use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::Builder::new()
        .prefix("parallax-worker-build")
        .tempdir()?;
    let tmp_output_path = tmp.path().join("parallax.pb");

    println!("generating protobuf descriptor");
    let mut protoc = Command::new("protoc");
    let output = protoc
        .arg("-I../../third-party/googleapis")
        .arg("-I../api/proto/")
        .arg("--include_imports")
        .arg("--include_source_info")
        .arg("--descriptor_set_out")
        .arg(&tmp_output_path)
        .arg("../api/proto/parallax/type/error/v1/error.proto")
        .arg("../api/proto/parallax/service/resource/v1/resource.proto")
        .arg("../api/proto/parallax/service/job/v1/job.proto")
        .arg("../api/proto/parallax/config/resource/v1/resource.proto")
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr).unwrap_or("<not utf-8>".to_string());
        panic!(
            "failed to generate descriptor set for third-party: {}",
            stderr
        );
    }

    let output_path =
        PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR to be set")).join("parallax.pb");
    std::fs::copy(tmp_output_path, output_path)?;

    Ok(())
}

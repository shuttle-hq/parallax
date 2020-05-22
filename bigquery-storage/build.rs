use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "../third-party/googleapis/google/cloud/bigquery/storage/v1/arrow.proto",
            "../third-party/googleapis/google/cloud/bigquery/storage/v1/avro.proto",
            "../third-party/googleapis/google/cloud/bigquery/storage/v1/storage.proto",
            "../third-party/googleapis/google/cloud/bigquery/storage/v1/stream.proto",
        ],
        &["proto/", "../third-party/googleapis"],
    )?;
    Ok(())
}

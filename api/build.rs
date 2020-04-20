macro_rules! with_type_attributes {
    {
        transparent: [$($pid:tt,)*],
        block: [$($bid:tt,)*],
        label: [$($lid:tt,)*],
        try_into: [$($tid:tt,)*],
        from_primitive: [$($fpid:tt,)*],
        serde: [$($sid:tt,)*]
    } => {
        tonic_build::configure()
            .format(false)
            $(
                .type_attribute(
                    $pid,
                    "#[serde(transparent)]"
                )
            )*
            $(
                .type_attribute(
                    $bid,
                    "#[derive(swamp::Block)]"
                )
            )*
            $(
                .type_attribute(
                    $lid,
                    "#[derive(swamp::Label)]"
                )
            )*
            $(
                .type_attribute(
                    $tid,
                    "#[derive(derive_more::TryInto, derive_more::From)]"
                )
            )*
            $(
                .type_attribute(
                    $fpid,
                    "#[derive(num_derive::FromPrimitive)]"
                )
            )*
            $(
                .type_attribute(
                    $sid,
                    "#[derive(serde::Serialize, serde::Deserialize)]"
                )
                .type_attribute(
                    $sid,
                    "#[serde(rename_all = \"snake_case\")]"
                )
            )*
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // TODO: will get eaten by swamp-build when ready
    let config = with_type_attributes! {
        transparent: [
            "State",
            "Resource",
            "Backend",
            "Data",
            "Policy",
        ],
        block: [
            "Policy",
            "Resource",
            "Data",
        ],
        label: [
            ".parallax.config.resource.v1",
        ],
        try_into: [
            ".parallax.config.resource.v1.Resource.resource",
            ".parallax.type.error.v1.Error.details",
        ],
        from_primitive: [
            ".parallax.type.error.v1.AccessError.AccessErrorKind",
            ".parallax.type.error.v1.TypeError.TypeErrorKind",
            ".parallax.type.error.v1.BackendError.BackendErrorKind",
        ],
        serde: [
            ".parallax.config.resource",
            ".parallax.service.resource",
            ".parallax.service.job",
            ".parallax.type.error",
        ]
    };
    config.compile(
        &[
            "proto/parallax/config/resource/v1/resource.proto",
            "proto/parallax/service/job/v1/job.proto",
            "proto/parallax/service/resource/v1/resource.proto",
            "proto/parallax/type/error/v1/error.proto",
        ],
        &["proto/", "../../third-party/googleapis"],
    )?;
    Ok(())
}

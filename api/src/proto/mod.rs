pub mod parallax {
    pub mod service {
        pub mod job {
            pub mod v1 {
                tonic::include_proto!("parallax.service.job.v1");
            }
        }
        pub mod resource {
            pub mod v1 {
                tonic::include_proto!("parallax.service.resource.v1");
            }
        }
    }
    pub mod config {
        pub mod resource {
            pub mod v1 {
                use swamp::Label;
                tonic::include_proto!("parallax.config.resource.v1");
            }
        }
    }
    pub mod r#type {
        pub mod error {
            pub mod v1 {
                tonic::include_proto!("parallax.r#type.error.v1");
            }
        }
    }
}

mod impls;

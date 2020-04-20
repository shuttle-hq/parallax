use super::super::parallax::service::job::v1 as job_v1;
use job_v1::*;

use super::super::parallax::r#type::error::v1 as error_v1;
use error_v1::Error;

impl Job {
    pub fn is_done(&self) -> bool {
        self.status
            .as_ref()
            .map(|status| status.state == 3)
            .unwrap_or(false)
    }
    pub fn into_result(self) -> Result<Self, Error> {
        if !self.is_done() {
            return Err(Error::new("job has not terminated yet"));
        }
        if let Some(job_error) = (&self.status)
            .as_ref()
            .and_then(|status| status.final_error.as_ref())
        {
            Err(job_error.clone())
        } else {
            Ok(self)
        }
    }
}

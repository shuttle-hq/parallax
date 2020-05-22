# BigQuery Storage API

A rust implementation of the BigQuery Storage API client V1.

## Running tests

Tests will run using your GCP acc and credentials:

*you will get charged*

We have chosen a small dataset (20 Mb) on purpose for the test suite, so it's not going to be too expensive.


To run tests:

```bash
$ GOOGLE_APPLICATION_CREDENTIALS=[path_to_service_account_key] cargo test
```

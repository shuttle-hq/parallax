use tonic::codec::Streaming;
use tonic::codegen::Stream;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Error as TransportError};
use tonic::Request;
use tonic::Status;

use std::sync::mpsc::channel;

use derive_more::From;

use crate::proto::bigquery_storage::big_query_read_client::*;
use crate::proto::bigquery_storage::read_rows_response::*;
use crate::proto::bigquery_storage::read_session::*;
use crate::proto::bigquery_storage::*;
use tonic::metadata::errors::InvalidMetadataValue;

static DOMAIN: &'static str = "bigquerystorage.googleapis.com";
static SCHEME: &'static str = "https";
static MAX_STREAMS: i32 = 10;
static BIGQUERY_SCOPE: &'static str = "https://www.googleapis.com/auth/bigquery";

use futures_01::Future;
use yup_oauth2::GetToken;

use std::sync::{Arc, Mutex};

// we need a token provider here
pub struct Client<T> {
    connector: Arc<Mutex<T>>,
}

#[derive(Debug, From)]
pub enum Error {
    Status(Status),
    Transport(TransportError),
    Io(std::io::Error),
    Metadata(InvalidMetadataValue),
}

fn table_uri(project_id: &str, dataset_id: &str, table_id: &str) -> String {
    format!(
        "projects/{}/datasets/{}/tables/{}",
        project_id, dataset_id, table_id
    )
}

impl<C> Client<C>
where
    C: GetToken,
{
    pub fn new(connector: C) -> Self {
        Self {
            connector: Arc::new(Mutex::new(connector)),
        }
    }

    pub async fn create_read_session(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<ReadSession, Error> {
        let table_uri = table_uri(project_id, dataset_id, table_id);
        let read_session = ReadSession {
            data_format: DataFormat::Arrow as i32,
            table: table_uri.clone(),
            ..Default::default()
        };

        let create_read_session_request = CreateReadSessionRequest {
            parent: format!("projects/{}", project_id),
            read_session: Some(read_session),
            max_stream_count: MAX_STREAMS, // up to 1000
        };

        let x_goog_request_params = format!("read_session.table={}", table_uri);
        let mut client = self.get_client().await?;
        let req = self.prepare_request(create_read_session_request, &x_goog_request_params)?;
        Ok(client.create_read_session(req).await?.into_inner())
    }

    pub async fn read_rows(
        &self,
        read_row_request: ReadRowsRequest,
    ) -> Result<Streaming<ReadRowsResponse>, Error> {
        let x_goog_request_params = format!("read_stream={}", read_row_request.read_stream);
        let mut client = self.get_client().await?;
        let req = self.prepare_request(read_row_request, &x_goog_request_params)?;
        Ok(client.read_rows(req).await?.into_inner())
    }

    async fn get_channel(&self) -> Result<Channel, Error> {
        let tls_config = ClientTlsConfig::new().domain_name(DOMAIN);
        let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
            .tls_config(tls_config)
            .connect()
            .await?;
        Ok(channel)
    }

    fn prepare_request<T>(&self, t: T, params: &str) -> Result<Request<T>, Error> {
        let bearer_token = self.get_bearer_token()?;

        let mut req = Request::new(t);
        let meta = req.metadata_mut();
        meta.insert("authorization", bearer_token);
        meta.insert("x-goog-request-params", MetadataValue::from_str(params)?);

        Ok(req)
    }

    async fn get_client(&self) -> Result<BigQueryReadClient<Channel>, Error> {
        let channel = self.get_channel().await?;
        let client = BigQueryReadClient::new(channel);
        Ok(client)
    }

    fn get_bearer_token(&self) -> Result<MetadataValue<Ascii>, Error> {
        let mut connector = self.connector.lock().unwrap();
        let (tx, rx) = channel::<_>();
        hyper::rt::run(
            connector
                .token(&[BIGQUERY_SCOPE.to_string()])
                .map(|token| {
                    let bearer_token = format!("Bearer {}", token.access_token);
                    let bearer_meta = MetadataValue::from_str(&bearer_token)?;
                    Ok(bearer_meta)
                })
                .map(move |bearer_meta| {
                    tx.send(bearer_meta);
                })
                .map_err(|_| ()),
        );

        let bearer_meta = rx.recv().map_err(|_| {
            let msg = "could not get access token".to_string();
            std::io::Error::new(std::io::ErrorKind::Other, msg)
        })?;

        bearer_meta
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::ipc::reader::*;
    use std::borrow::BorrowMut;
    use std::io::{Cursor, Read};
    use tokio::runtime::Runtime;

    struct ArrowReader {
        schema_bytes: Vec<u8>,
        record_batches: Vec<Vec<u8>>,
    }

    impl ArrowReader {
        fn new(mut schema_bytes: Vec<u8>, record_batches: Vec<Vec<u8>>) -> Self {
            Self {
                schema_bytes,
                record_batches,
            }
        }

        // Stripping first 4 bytes
        fn to_cursor(&self) -> Cursor<Vec<u8>> {
            let mut bytes: Vec<u8> = vec![];

            bytes.extend_from_slice(&self.schema_bytes.clone()[4..]);

            for rb in self.record_batches.clone() {
                if rb[0..4] != [255, 255, 255, 255] {
                    continue;
                }
                bytes.extend_from_slice(&rb.clone()[4..]);
            }

            Cursor::new(bytes)
        }
    }

    use yup_oauth2::{service_account_key_from_file, ServiceAccountAccess};

    #[test]
    fn read_test() {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
            let project_id = "openquery-dev".to_string();
            let dataset_id = "yelp".to_string();
            let table_id = "business".to_string();

            let sa_key =
                service_account_key_from_file(env!("GOOGLE_APPLICATION_CREDENTIALS")).unwrap();
            let connector = ServiceAccountAccess::new(sa_key).build();

            let client = Client::new(connector);

            // Creates a read session
            let read_session = client
                .create_read_session(&project_id, &dataset_id, &table_id)
                .await
                .unwrap();

            let read_row_requests = read_session
                .streams
                .iter()
                .map(|stream| ReadRowsRequest {
                    read_stream: stream.name.clone(),
                    offset: 0,
                })
                .collect::<Vec<ReadRowsRequest>>();

            let mut row_responses: Vec<ReadRowsResponse> = vec![];
            let mut sum = 0;

            // Retrieves rows from streams
            for read_row_request in read_row_requests {
                let mut row_response_stream = client.read_rows(read_row_request).await.unwrap();

                while let Some(row_response) = row_response_stream.message().await.unwrap() {
                    sum += row_response.row_count;
                    row_responses.push(row_response);
                }
            }

            // At this stage we have a bunch of serialized record batches
            let record_batches_bytes = row_responses
                .into_iter()
                .map(|rr| rr.rows.unwrap())
                .map(|row| match row {
                    Rows::AvroRows(_) => panic!("Expected arrow record batches"),
                    Rows::ArrowRecordBatch(arrow_record_batch) => {
                        return arrow_record_batch.serialized_record_batch;
                    }
                })
                .collect::<Vec<Vec<u8>>>();

            let schema_bytes = match read_session.schema.unwrap() {
                Schema::AvroSchema(_) => panic!("Expected arrow schema"),
                Schema::ArrowSchema(arrow_schema) => arrow_schema.serialized_schema,
            };

            // Put the arrow bytes in a reader
            let arrow_reader = ArrowReader::new(schema_bytes, record_batches_bytes);

            // Try to parse these into a record batch
            let mut stream_reader = StreamReader::try_new(arrow_reader.to_cursor()).unwrap();

            let mut nrows = 0;
            // Iterate over the result
            while let Ok(Some(record_batch)) = stream_reader.next() {
                nrows += record_batch.num_rows();
            }

            assert_eq!(nrows, 192609);
        });
    }
}

use crate::common::*;

use crate::node::{Access, AccessProvider};
use crate::opt::{Context, TableMeta};

#[derive(Debug)]
pub enum CatalogError {
    MalformedCatalog(String),
}

impl CatalogError {
    fn malformed(msg: String) -> Self {
        CatalogError::MalformedCatalog(msg)
    }
}

impl From<CatalogError> for Status {
    fn from(catalog_error: CatalogError) -> Self {
        match catalog_error {
            CatalogError::MalformedCatalog(msg) => Status::internal(msg),
        }
    }
}

/// NaiveCatalog is naive because it does't have any policy context
#[async_trait]
pub trait Catalog {
    async fn get_virtual_datasets(&self) -> Result<Vec<VirtualDataset>, CatalogError>;
    async fn get_virtual_tables(&self, dataset_id: &str)
        -> Result<Vec<VirtualTable>, CatalogError>;
    async fn get_virtual_table_schema(
        &self,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<Vec<VirtualColumn>, CatalogError>;
}

#[tonic::async_trait]
impl<A> CatalogService for CatalogServiceImpl<A>
where
    A: AccessProvider + 'static,
{
    async fn get_virtual_datasets(
        &self,
        req: Request<GetVirtualDatasetsRequest>,
    ) -> Result<Response<GetVirtualDatasetsResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let virtual_datasets = access.context().await?.get_virtual_datasets().await?;

        Ok(Response::new(GetVirtualDatasetsResponse {
            virtual_datasets,
        }))
    }

    async fn get_virtual_tables(
        &self,
        req: Request<GetVirtualTablesRequest>,
    ) -> Result<Response<GetVirtualTablesResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let dataset_id = &req.get_ref().dataset_id;
        let virtual_tables = access
            .context()
            .await?
            .get_virtual_tables(dataset_id)
            .await?;

        Ok(Response::new(GetVirtualTablesResponse { virtual_tables }))
    }

    async fn get_virtual_table_schema(
        &self,
        req: Request<GetVirtualTableSchemaRequest>,
    ) -> Result<Response<GetVirtualTableSchemaResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let dataset_id = &req.get_ref().dataset_id;
        let table_id = &req.get_ref().table_id;
        let virtual_columns = access
            .context()
            .await?
            .get_virtual_table_schema(dataset_id, table_id)
            .await?;

        Ok(Response::new(GetVirtualTableSchemaResponse {
            virtual_columns,
        }))
    }
}

/// The Catalog here is a 'naive' Catalog because it doesn't have the policy context.
#[async_trait]
impl Catalog for Context<TableMeta> {
    async fn get_virtual_datasets(&self) -> Result<Vec<VirtualDataset>, CatalogError> {
        let mut virtual_datasets = HashSet::new();
        for (key, _) in self.iter() {
            let dataset_name = key.root().ok_or_else(|| {
                CatalogError::malformed(format!("Context key {} has no root", key))
            })?;
            let virtual_dataset = VirtualDataset {
                name: dataset_name.to_string(),
            };
            virtual_datasets.insert(virtual_dataset);
        }
        Ok(virtual_datasets.into_iter().collect())
    }

    // Is a HashSet necessary here?
    async fn get_virtual_tables(
        &self,
        dataset_id: &str,
    ) -> Result<Vec<VirtualTable>, CatalogError> {
        let mut virtual_tables = HashSet::new();
        for (key, _) in self.iter().filter(|(key, _)| match key.root() {
            Some(dataset_name) => dataset_name == dataset_id,
            None => false,
        }) {
            let table_name = key.at_depth(0).ok_or_else(|| {
                CatalogError::malformed(format!("Context key {} has no root", key))
            })?;
            let virtual_table = VirtualTable {
                name: table_name.to_string(),
            };
            virtual_tables.insert(virtual_table);
        }
        Ok(virtual_tables.into_iter().collect())
    }

    // Is a HashSet necessary here?
    async fn get_virtual_table_schema(
        &self,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<Vec<VirtualColumn>, CatalogError> {
        let mut virtual_columns = HashSet::new();
        for (_, table_meta) in self
            .iter()
            .filter(|(key, _)| match key.root() {
                Some(dataset_name) => dataset_name == dataset_id,
                None => false,
            })
            .filter(|(key, _)| match key.at_depth(0) {
                Some(table_name) => table_name == table_id,
                None => false,
            })
        {
            for (key, expr_meta) in table_meta.columns.iter() {
                let virtual_column = VirtualColumn {
                    mode: expr_meta.mode.to_string(),
                    name: key.at_depth(0).unwrap_or("undefined").to_string(),
                    ty: expr_meta.ty.to_string(),
                };
                virtual_columns.insert(virtual_column);
            }
        }
        Ok(virtual_columns.into_iter().collect())
    }
}

pub struct CatalogServiceImpl<A> {
    access: A,
}

impl<A> CatalogServiceImpl<A> {
    pub fn new(access: A) -> Self {
        Self { access }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::node::state::tests::read_manifest;
    use crate::node::tests::mk_node;
    use crate::opt::Taint;
    use crate::opt::{validate::Validator, Relation};

    use tokio::runtime::Runtime;

    #[test]
    fn get_virtual_datasets() {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new().unwrap().block_on(async move {
            let ctx = access.context().await.unwrap();
            let v_datasets = ctx.get_virtual_datasets().await.unwrap();
            assert_eq!(1, v_datasets.len());
            assert_eq!("patient_data", v_datasets.get(0).unwrap().name);
        })
    }

    #[test]
    fn get_virtual_tables() {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new().unwrap().block_on(async move {
            let ctx = access.context().await.unwrap();
            let v_tables = ctx
                .get_virtual_tables("patient_data")
                .await
                .unwrap()
                .iter()
                .map(|v_table| v_table.name.clone())
                .collect::<HashSet<_>>();
            assert_eq!(4, v_tables.len());
            assert_eq!(
                HashSet::from_iter(vec![
                    "person".to_string(),
                    "care_site".to_string(),
                    "location".to_string(),
                    "vocabulary".to_string()
                ]),
                v_tables
            );
        })
    }

    #[test]
    fn get_virtual_table_schema() {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new().unwrap().block_on(async move {
            let ctx = access.context().await.unwrap();
            let v_columns = ctx
                .get_virtual_table_schema("patient_data", "person")
                .await
                .unwrap();
            let id_column = v_columns
                .iter()
                .find(|col| col.name == "person_id")
                .unwrap();
            assert_eq!(v_columns.len(), 18);
            assert_eq!(
                id_column,
                &VirtualColumn {
                    name: "person_id".to_string(),
                    mode: "Nullable".to_string(),
                    ty: "Integer".to_string(),
                }
            );
        })
    }
}

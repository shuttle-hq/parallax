use anyhow::{Error, Result};
use parallax_api::client::Client;
use parallax_api::{
    GetVirtualDatasetsRequest, GetVirtualDatasetsResponse, GetVirtualTableSchemaRequest,
    GetVirtualTableSchemaResponse, GetVirtualTablesRequest, GetVirtualTablesResponse,
};
use prettytable::{Cell, Row, Table};

/// No string lists datasets
/// String with no dots lists tables in dataset
/// String with 1 dot lists columns in table
/// More than that is rejected
pub async fn ls(client: &mut Client, maybe_pattern: Option<String>) -> Result<()> {
    match maybe_pattern {
        Some(pattern) => {
            let substrings: Vec<_> = pattern.split('.').collect();
            match substrings.len() {
                1 => {
                    let dataset = substrings.get(0).unwrap().to_string();
                    get_virtual_tables(client, dataset).await?;
                }
                2 => {
                    let dataset = substrings.get(0).unwrap().to_string();
                    let table = substrings.get(1).unwrap().to_string();
                    get_virtual_table_schema(client, dataset, table).await?;
                }
                _ => {
                    return Err(Error::msg(
                        "Pattern should be of the form <dataset> or <dataset>.<table>",
                    ));
                }
            }
        }
        None => {
            get_virtual_datasets(client).await?;
        }
    }
    Ok(())
}

async fn get_virtual_datasets(client: &mut Client) -> Result<()> {
    let req = GetVirtualDatasetsRequest {};
    let virtual_dataset_resp = client.get_virtual_datasets(req).await?;
    let virtual_datasets = virtual_dataset_resp.into_inner().virtual_datasets;

    let mut table = Table::new();
    table.add_row(Row::new(vec![Cell::new("DATASET_ID")]));
    for virtual_dataset in virtual_datasets {
        table.add_row(Row::new(vec![Cell::new(&virtual_dataset.name)]));
    }
    table.printstd();
    Ok(())
}

async fn get_virtual_tables(client: &mut Client, dataset: String) -> Result<()> {
    let req = GetVirtualTablesRequest {
        dataset_id: dataset,
    };
    let virtual_tables_resp = client.get_virtual_tables(req).await?;
    let virtual_tables = virtual_tables_resp.into_inner().virtual_tables;

    let mut table = Table::new();
    table.add_row(Row::new(vec![Cell::new("TABLE_ID")]));
    for virtual_table in virtual_tables {
        table.add_row(Row::new(vec![Cell::new(&virtual_table.name)]));
    }
    table.printstd();
    Ok(())
}

async fn get_virtual_table_schema(
    client: &mut Client,
    dataset: String,
    table: String,
) -> Result<()> {
    let req = GetVirtualTableSchemaRequest {
        dataset_id: dataset,
        table_id: table,
    };
    let virtual_table_schema_resp = client.get_virtual_table_schema(req).await?;
    let virtual_columns = virtual_table_schema_resp.into_inner().virtual_columns;

    let mut table = Table::new();
    table.add_row(Row::new(vec![
        Cell::new("COLUMN_ID"),
        Cell::new("TYPE"),
        Cell::new("MODE"),
    ]));
    for virtual_column in virtual_columns {
        table.add_row(Row::new(vec![
            Cell::new(&virtual_column.name),
            Cell::new(&virtual_column.ty),
            Cell::new(&virtual_column.mode),
        ]));
    }
    table.printstd();
    Ok(())
}

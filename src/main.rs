use std::time::Instant;

use extractor::extractor_manager::extract_data;
use loader::loader_manager::load;
use model::data_model::AllDatasets;
use transformer::normalize::{convert_eu_to_usd, join_all_datasets, normalize_data, rename_columns};

use crate::model::data_model::DatasetWithConfig;

mod extractor;
mod transformer;
mod loader;
mod tests;
mod datasets_config;
mod model;

// TODO: Add logger instead of println! statements
// TODO: Remove unused dependencies

#[tokio::main]
async fn main() {
    let start = Instant::now();

    let all_datasets: AllDatasets = extract_data().await;

    let all_normalized_datasets: Vec<DatasetWithConfig> = normalize_data(all_datasets);

    let renamed_datasets = rename_columns(all_normalized_datasets);

    let converted_datasets = convert_eu_to_usd(renamed_datasets);

    let result_dataframe = join_all_datasets(converted_datasets);

    load(result_dataframe);

    let duration = start.elapsed();
    println!("Execution time: {:?}", duration);

}
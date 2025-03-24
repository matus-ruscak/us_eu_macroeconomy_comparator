mod extractor;
use extractor::extractor_manager::extract_data;
use model::data_model::AllDatasets;
use transformer::normalize::normalize_data;
use crate::model::data_model::DatasetWithConfig;
use std::time::Instant;

mod transformer;
mod loader;
mod utils;
mod tests;
mod datasets_config;
mod model;

#[tokio::main]
async fn main() {
    let start = Instant::now();

    let all_datasets: AllDatasets = extract_data().await;

    let all_normalized_datasets: Vec<DatasetWithConfig> = normalize_data(all_datasets);

    // Debug export
    for dataset in all_normalized_datasets {
        let name = dataset.name;
        println!("name is {name}");
        let dataframe = dataset.dataframe;
        println!("{dataframe}");
    }

    let duration = start.elapsed();
    println!("Execution time: {:?}", duration);

}
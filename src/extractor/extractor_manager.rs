use crate::extractor::{csv, fred, ecb};
use crate::datasets_config::datasets_config::{DatasetConfig, get_all_datasets_configs};
use crate::model::data_model::{DatasetWithConfig, AllDatasets};

use tokio::task::JoinHandle;

pub async fn extract_data() -> AllDatasets {
    let all_datasets_configs = get_all_datasets_configs();

    let mut handles: Vec<JoinHandle<DatasetWithConfig>> = vec![];

    for dataset_config in all_datasets_configs {
        let handle: JoinHandle<DatasetWithConfig> = tokio::spawn(async move {
            retrieve_dataset(dataset_config).await
        });

        handles.push(handle);
    }

    let mut all_datasets_vector: Vec<DatasetWithConfig> = vec![];

    for handle in handles {
        match handle.await {
            Ok(dataset_with_config) => all_datasets_vector.push(dataset_with_config),
            Err(e) => {
                eprintln!("Task failed to join: {:?}", e);
            }
        }
    }

    let all_datasets = AllDatasets::new(
        all_datasets_vector[0].clone(),
        all_datasets_vector[1].clone(),
        all_datasets_vector[2].clone(),
        all_datasets_vector[3].clone(),
        all_datasets_vector[4].clone(),
        all_datasets_vector[5].clone(),
        all_datasets_vector[6].clone(),
        all_datasets_vector[7].clone(),
    );

    all_datasets
}

async fn retrieve_dataset(dataset_config: DatasetConfig) -> DatasetWithConfig {
    let source = dataset_config.source;
    let identifier = dataset_config.identifier;
    let dataset_name = dataset_config.name;

    let data_frame = match source {
        "csv" => {
            csv::get_data(identifier).await.unwrap()
        },
        "fred" => {
            fred::get_data(identifier, None).await.unwrap()
        },
        "ecb" => {
            ecb::get_data(identifier, None).await.unwrap()
        }
        _ => {
            panic!("Unknown source type: {}", source);
        }
    };

    let result_dataset = DatasetWithConfig::new(dataset_name, data_frame, dataset_config);

    result_dataset
}

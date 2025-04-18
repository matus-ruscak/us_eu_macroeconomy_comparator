use crate::extractor::{csv, fred, ecb};
use crate::extractor::fred::get_fred_api_key;
use crate::datasets_config::datasets_config::{DatasetConfig, get_all_datasets_configs};
use crate::model::data_model::{DatasetWithConfig, AllDatasets};
use tokio::task::JoinHandle;
use polars::prelude::DataFrame;
use std::future::Future;

type DynError = Box<dyn std::error::Error + Send + Sync>;

pub async fn extract_data() -> AllDatasets {
    let all_datasets_configs = get_all_datasets_configs();

    let mut handles: Vec<JoinHandle<DatasetWithConfig>> = vec![];

    for dataset_config in all_datasets_configs {
        let handle: JoinHandle<DatasetWithConfig> = tokio::spawn(async move {
            retrieve_dataset(
                dataset_config,
                |id| csv::get_data(id),
                |id, _opt, get_key| fred::get_data(id, None, get_key),
                |id, _opt| ecb::get_data(id, None),
            ).await
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

pub async fn retrieve_dataset<
    CsvFn,
    CsvFut,
    FredFn,
    FredFut,
    EcbFn,
    EcbFut,
>(
    dataset_config: DatasetConfig,
    get_csv_data: CsvFn,
    get_fred_data: FredFn,
    get_ecb_data: EcbFn,
) -> DatasetWithConfig
where
    CsvFn: Fn(String) -> CsvFut + Send + Sync,
    CsvFut: Future<Output = Result<DataFrame, DynError>> + Send,

    FredFn: Fn(String, Option<()>, fn() -> String) -> FredFut + Send + Sync,
    FredFut: Future<Output = Result<DataFrame, DynError>> + Send,

    EcbFn: Fn(String, Option<()>) -> EcbFut + Send + Sync,
    EcbFut: Future<Output = Result<DataFrame, DynError>> + Send,
{
    let source = dataset_config.source;
    let identifier = dataset_config.identifier;
    let dataset_name = dataset_config.name;

    let data_frame = match source {
        "csv" => {
            get_csv_data(identifier.to_string()).await.unwrap()
        },
        "fred" => {
            get_fred_data(identifier.to_string(), None, get_fred_api_key).await.unwrap()
        },
        "ecb" => {
            get_ecb_data(identifier.to_string(), None).await.unwrap()
        },
        _ => {
            panic!("Unknown source type: {}", source);
        }
    };

    DatasetWithConfig::new(dataset_name, data_frame, dataset_config)
}


// Version only for testing


#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use crate::model::data_model::DatasetWithConfig;
    use crate::datasets_config::datasets_config::DatasetConfig;

    fn dummy_dataframe() -> DataFrame {
        df!["col" => &[1, 2, 3]].unwrap()
    }

    fn sample_config(source: &'static str) -> DatasetConfig {
        DatasetConfig {
            source,
            identifier: "test_id",
            name: "test_dataset",
            quarterly_avg_required: true,
            quarterly_average_config: None
        }
    }

    pub async fn extract_data_with<F1, F2, F3>(
        configs: Vec<DatasetConfig>,
        get_csv: F1,
        get_fred: F2,
        get_ecb: F3,
    ) -> AllDatasets
    where
        F1: Fn(String) -> JoinHandle<DatasetWithConfig> + Send + Sync + Copy + 'static,
        F2: Fn(String) -> JoinHandle<DatasetWithConfig> + Send + Sync + Copy + 'static,
        F3: Fn(String) -> JoinHandle<DatasetWithConfig> + Send + Sync + Copy + 'static,
    {
        let mut handles = vec![];

        for cfg in configs {
            let source = cfg.source;
            let handle = match source {
                "csv" => get_csv(cfg.identifier.to_string()),
                "fred" => get_fred(cfg.identifier.to_string()),
                "ecb" => get_ecb(cfg.identifier.to_string()),
                _ => panic!("Unknown source"),
            };

            handles.push(handle);
        }

        let mut all = vec![];
        for h in handles {
            if let Ok(data) = h.await {
                all.push(data);
            }
        }

        AllDatasets::new(
            all[0].clone(), all[1].clone(), all[2].clone(), all[0].clone(),
            all[1].clone(), all[2].clone(), all[0].clone(), all[1].clone(),
        )
    }

    #[tokio::test]
    async fn test_retrieve_dataset_csv() {
        let config = sample_config("csv");

        let result = retrieve_dataset(
            config.clone(),
            |id| async move {
                assert_eq!(id, "test_id");
                Ok(dummy_dataframe())
            },
            |_id, _opt, _key_fn| async { panic!("should not be called in csv test") },
            |_id, _opt| async { panic!("should not be called in csv test") },
        ).await;

        assert_eq!(result.name, "test_dataset");
        assert_eq!(result.dataset_config.source, "csv");
        assert_eq!(result.dataframe.shape(), (3, 1));
    }

    #[tokio::test]
    async fn test_retrieve_dataset_fred() {
        let config = sample_config("fred");

        let result = retrieve_dataset(
            config.clone(),
            |_id| async { panic!("should not be called in fred test") },
            |id, _opt, key_fn| async move {
                assert_eq!(id, "test_id");
                assert_eq!(key_fn(), get_fred_api_key());
                Ok(dummy_dataframe())
            },
            |_id, _opt| async { panic!("should not be called in fred test") },
        ).await;

        assert_eq!(result.name, "test_dataset");
        assert_eq!(result.dataset_config.source, "fred");
    }

    #[tokio::test]
    async fn test_retrieve_dataset_ecb() {
        let config = sample_config("ecb");

        let result = retrieve_dataset(
            config.clone(),
            |_id| async { panic!("should not be called in ecb test") },
            |_id, _opt, _key_fn| async { panic!("should not be called in ecb test") },
            |id, _opt| async move {
                assert_eq!(id, "test_id");
                Ok(dummy_dataframe())
            },
        ).await;

        assert_eq!(result.name, "test_dataset");
        assert_eq!(result.dataset_config.source, "ecb");
    }


    #[tokio::test]
    async fn test_extract_data_with_mocked_sources() {
        let dataset_config_csv = DatasetConfig {
            source: "csv",
            identifier: "id_1",
            name: "dataset_1",
            quarterly_avg_required: false,
            quarterly_average_config: None
        };
        let dataset_config_fred = DatasetConfig {
            source: "fred",
            identifier: "id_2",
            name: "dataset_2",
            quarterly_avg_required: false,
            quarterly_average_config: None
        };
        let dataset_config_ecb = DatasetConfig {
            source: "ecb",
            identifier: "id_3",
            name: "dataset_3",
            quarterly_avg_required: false,
            quarterly_average_config: None
        };

        let configs: Vec<DatasetConfig> = vec![dataset_config_csv, dataset_config_fred, dataset_config_ecb];

        let get_csv = |_id: String| {
            tokio::spawn(async move {
                DatasetWithConfig::new(
                    "csv_1",
                    dummy_dataframe(),
                    sample_config("csv"),
                )
            })
        };

        let get_fred = |_id: String| {
            tokio::spawn(async move {
                DatasetWithConfig::new(
                    "fred_1",
                    dummy_dataframe(),
                    sample_config("fred"),
                )
            })
        };

        let get_ecb = |_id: String| {
            tokio::spawn(async move {
                DatasetWithConfig::new(
                    "ecb_1",
                    dummy_dataframe(),
                    sample_config("ecb"),
                )
            })
        };

        let result = extract_data_with(configs, get_csv, get_fred, get_ecb).await;

        assert_eq!(result.items().len(), 8);
    }
}
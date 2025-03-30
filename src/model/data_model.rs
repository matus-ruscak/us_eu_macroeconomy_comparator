use polars::frame::DataFrame;
use crate::datasets_config::datasets_config::DatasetConfig;
#[derive(Clone)]
pub struct DatasetWithConfig {
    pub name: &'static str,
    pub dataframe: DataFrame,
    pub dataset_config: DatasetConfig,
}

impl DatasetWithConfig {
    pub fn new(name: &'static str,
               dataframe: DataFrame,
               dataset_config: DatasetConfig) -> Self {
        DatasetWithConfig {
            name,
            dataframe,
            dataset_config,
        }
    }
}

pub struct AllDatasets {
    pub fx_rates_df: DatasetWithConfig,
    pub fred_sp500_df: DatasetWithConfig,
    pub fred_us_gdp_df: DatasetWithConfig,
    pub fred_us_total_public_debt_df: DatasetWithConfig,
    pub fred_us_inflation_df: DatasetWithConfig,
    pub ecb_government_debt_df: DatasetWithConfig,
    pub ecb_gdp_df: DatasetWithConfig,
    pub ecb_inflation_df: DatasetWithConfig,
}

impl AllDatasets {
    pub fn items(self) -> Vec<DatasetWithConfig> {
        vec![
            self.fx_rates_df,
            self.fred_sp500_df,
            self.fred_us_gdp_df,
            self.fred_us_total_public_debt_df,
            self.fred_us_inflation_df,
            self.ecb_government_debt_df,
            self.ecb_gdp_df,
            self.ecb_inflation_df,
        ]
    }
}

impl AllDatasets {
    pub fn new(fx_rates_df: DatasetWithConfig,
               fred_sp500_df: DatasetWithConfig,
               fred_us_gdp_df: DatasetWithConfig,
               fred_us_total_public_debt_df: DatasetWithConfig,
               fred_us_inflation_df: DatasetWithConfig,
               ecb_government_debt_df: DatasetWithConfig,
               ecb_gdp_df: DatasetWithConfig,
               ecb_inflation_df: DatasetWithConfig) -> Self {
        AllDatasets {
            fx_rates_df,
            fred_sp500_df,
            fred_us_gdp_df,
            fred_us_total_public_debt_df,
            fred_us_inflation_df,
            ecb_government_debt_df,
            ecb_gdp_df,
            ecb_inflation_df,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasets_config::datasets_config::DatasetConfig;

    fn mock_dataframe() -> DataFrame {
        DataFrame::empty()
    }

    fn mock_dataset_config() -> DatasetConfig {
        DatasetConfig::new("test_dataset", "test_source", "test_identifier", false, None)
    }

    #[test]
    fn test_dataset_with_config_creation() {
        let df = mock_dataframe();
        let config = mock_dataset_config();

        let dataset = DatasetWithConfig::new("test_dataset", df.clone(), config.clone());

        assert_eq!(dataset.name, "test_dataset");
        assert_eq!(dataset.dataset_config.name, "test_dataset");
    }

    #[test]
    fn test_all_datasets_creation_and_items() {
        let dataset = DatasetWithConfig::new("test", mock_dataframe(), mock_dataset_config());

        let all_datasets = AllDatasets::new(
            dataset.clone(), dataset.clone(), dataset.clone(), dataset.clone(),
            dataset.clone(), dataset.clone(), dataset.clone(), dataset.clone()
        );

        let items = all_datasets.items();

        assert_eq!(items.len(), 8);

        for ds in &items {
            assert_eq!(ds.name, "test");
        }
    }
}
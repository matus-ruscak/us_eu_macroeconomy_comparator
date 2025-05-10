#[derive(Clone, Copy)]
pub struct QuarterlyAverageConfig {
    pub date_column_name: &'static str,
    pub target_column_name: &'static str,
    pub target_column_alias: &'static str,
    pub date_format_mask: &'static str
}

impl QuarterlyAverageConfig {
    pub fn new(date_column_name: &'static str,
               target_column_name: &'static str,
               target_column_alias: &'static str,
               date_format_mask: &'static str
    ) -> Self {
        QuarterlyAverageConfig {
            date_column_name,
            target_column_name,
            target_column_alias,
            date_format_mask
        }
    }
}

#[derive(Clone, Copy)]
pub struct DatasetConfig {
    pub name: &'static str,
    pub source: &'static str,
    pub identifier: &'static str,
    pub quarterly_avg_required: bool,
    pub quarterly_average_config: Option<QuarterlyAverageConfig>,
}

impl DatasetConfig {
    pub fn new(name: &'static str,
               source: &'static str,
               identifier: &'static str,
               quarterly_avg_required: bool,
               quarterly_average_config: Option<QuarterlyAverageConfig>) -> Self {
        DatasetConfig {
            name,
            source,
            identifier,
            quarterly_avg_required,
            quarterly_average_config
        }
    }
}

pub fn get_all_datasets_configs() -> Vec<DatasetConfig> {
    let all_datasets_configs: Vec<DatasetConfig> = vec![
        DatasetConfig::new("fx_rates",
                           "csv",
                           "csv_data/DEXUSEU.csv",
                           true,
                           Some(QuarterlyAverageConfig::new(
                               "observation_date",
                               "DEXUSEU",
                               "avg_fx_rate",
                               "%Y-%m-%d"))),
        DatasetConfig::new("sp500",
                           "fred",
                           "SP500",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "sp500_usd",
                                                            "%Y-%m-%d"))),
        DatasetConfig::new("us_gdp",
                           "fred",
                           "GDP",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_gdp_usd",
                                                            "%Y-%m-%d"))),
        DatasetConfig::new("us_total_public_debt",
                           "fred",
                           "GFDEBTN",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_total_debt_usd",
                                                            "%Y-%m-%d"))),
        DatasetConfig::new("us_inflation",
                           "fred",
                           "CORESTICKM159SFRBATL",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_inflation_usd",
                                                            "%Y-%m-%d"))),
        DatasetConfig::new("eu_government_debt",
                           "ecb",
                           "GFS/Q.N.I9.W0.S13.S1.C.L.LE.GD.T._Z.XDC._T.F.V.N._T",
                           false,
                           None),
        DatasetConfig::new("eu_gdp",
                           "ecb",
                           "MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.N",
                           false,
                           None),
        DatasetConfig::new("eu_inflation",
                           "ecb",
                           "ICP/M.U2.N.XEF000.4.ANR",
                           true,
                           Some(QuarterlyAverageConfig::new("quarter",
                                                            "value",
                                                            "value",
                                                            "%Y-%m"))),
    ];

    all_datasets_configs
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quarterly_average_config_creation() {
        let config = QuarterlyAverageConfig::new("date", "value", "avg_value", "%Y-%m");

        assert_eq!(config.date_column_name, "date");
        assert_eq!(config.target_column_name, "value");
        assert_eq!(config.target_column_alias, "avg_value");
        assert_eq!(config.date_format_mask, "%Y-%m");
    }

    #[test]
    fn test_dataset_config_creation() {
        let quarterly_config = QuarterlyAverageConfig::new("date", "value", "avg_value", "%Y-%m");

        let dataset = DatasetConfig::new(
            "test_dataset",
            "test_source",
            "test_identifier",
            true,
            Some(quarterly_config),
        );

        assert_eq!(dataset.name, "test_dataset");
        assert_eq!(dataset.source, "test_source");
        assert_eq!(dataset.identifier, "test_identifier");
        assert!(dataset.quarterly_avg_required);
        assert!(dataset.quarterly_average_config.is_some());

        let qa_config = dataset.quarterly_average_config.unwrap();
        assert_eq!(qa_config.date_column_name, "date");
        assert_eq!(qa_config.target_column_name, "value");
        assert_eq!(qa_config.target_column_alias, "avg_value");
        assert_eq!(qa_config.date_format_mask, "%Y-%m");
    }

    #[test]
    fn test_get_all_datasets_configs() {
        let configs = get_all_datasets_configs();

        assert!(!configs.is_empty());

        let sp500_config = configs.iter().find(|c| c.name == "sp500").expect("sp500 config missing");

        assert_eq!(sp500_config.source, "fred");
        assert_eq!(sp500_config.identifier, "SP500");
        assert!(sp500_config.quarterly_avg_required);
        assert!(sp500_config.quarterly_average_config.is_some());

        let qa_config = sp500_config.quarterly_average_config.unwrap();
        assert_eq!(qa_config.date_column_name, "date");
        assert_eq!(qa_config.target_column_name, "value");
        assert_eq!(qa_config.target_column_alias, "sp500_usd");
    }
}
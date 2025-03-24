#[derive(Clone, Copy)]
pub struct QuarterlyAverageConfig {
    pub date_column_name: &'static str,
    pub target_column_name: &'static str,
    pub target_column_alias: &'static str,
}

impl QuarterlyAverageConfig {
    pub fn new(date_column_name: &'static str,
               target_column_name: &'static str,
               target_column_alias: &'static str,
    ) -> Self {
        QuarterlyAverageConfig {
            date_column_name,
            target_column_name,
            target_column_alias,
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
                               "avg_fx_rate"))),
        DatasetConfig::new("sp500",
                           "fred",
                           "SP500",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "sp500_usd"))),
        DatasetConfig::new("us_gdp",
                           "fred",
                           "FYGDP",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_gdp_usd"))),
        DatasetConfig::new("us_total_public_debt",
                           "fred",
                           "GFDEBTN",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_total_debt_usd"))),
        DatasetConfig::new("us_inflation",
                           "fred",
                           "CORESTICKM159SFRBATL",
                           true,
                           Some(QuarterlyAverageConfig::new("date",
                                                            "value",
                                                            "us_inflation_usd"))),
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
                           false,
                           None),
    ];

    all_datasets_configs
}

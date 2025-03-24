use crate::extractor::extractor_manager::Dataset;
use polars::frame::DataFrame;

pub struct DatasetsConfig {
    pub name: &'static str,
    pub source: &'static str,
    pub identifier: &'static str,
    pub quarterly_avg_required: bool,
}

impl DatasetsConfig {
    pub fn new(name: &str, source: &str, identifier: &str, quarterly_avg_required: bool) -> Self {
        DatasetsConfig {
            name,
            source,
            identifier,
            quarterly_avg_required,
        }
    }
}

pub fn get_all_datasets_configs() -> Vec<DatasetsConfig> {
    let all_datasets_configs: Vec<DatasetsConfig> = vec![
        DatasetsConfig::new("fx_rates", "csv", "csv_data/DEXUSEU.csv", true),
        DatasetsConfig::new("sp500", "fred", "SP500", true),
        DatasetsConfig::new("us_gdp", "fred", "FYGDP", true),
        DatasetsConfig::new("us_total_public_debt", "fred", "GFDEBTN", true),
        DatasetsConfig::new("us_inflation", "fred", "CORESTICKM159SFRBATL", true),
        DatasetsConfig::new("eu_government_debt", "ecb", "GFS/Q.N.I9.W0.S13.S1.C.L.LE.GD.T._Z.XDC._T.F.V.N._T", false),
        DatasetsConfig::new("eu_gdp", "fred", "MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.N", true),
        DatasetsConfig::new("eu_inflation", "fred", "ICP/M.U2.N.XEF000.4.ANR", true),
    ];

    all_datasets_configs
}

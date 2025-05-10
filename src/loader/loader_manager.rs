use polars::prelude::{DataFrame, PlSmallStr};

use loader::csv;
use loader::graph::generate_graphs;
use loader::parquet;

use crate::loader;

pub fn load(result: DataFrame) -> () {
    let result_final_column_names = set_final_column_names(result);

    csv::load(result_final_column_names.clone());
    parquet::load(result_final_column_names.clone());
    generate_graphs(result_final_column_names.clone());
}

fn set_final_column_names(mut result: DataFrame) -> DataFrame {
    let result = result.rename("eur_to_usd", PlSmallStr::from_str("fx_rate_eur_to_usd")).unwrap();
    let result = result.rename("us_gdp_usd", PlSmallStr::from_str("us_gdp_usd_billions")).unwrap();
    let result = result.rename("us_total_debt_usd", PlSmallStr::from_str("us_total_debt_usd_millions")).unwrap();
    let result = result.rename("us_inflation_usd", PlSmallStr::from_str("us_inflation_perc")).unwrap();
    let result = result.rename("eu_inflation", PlSmallStr::from_str("eu_inflation_perc")).unwrap();
    let result = result.rename("eu_government_debt_converted", PlSmallStr::from_str("eu_government_debt_usd_millions")).unwrap();
    let result = result.rename("eu_gdp_converted", PlSmallStr::from_str("eu_gdp_usd_millions")).unwrap();

    result.clone()
}
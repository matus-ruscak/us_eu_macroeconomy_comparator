use std::collections::HashSet;

use polars::prelude::*;
use rayon::prelude::*;

use crate::datasets_config::datasets_config::QuarterlyAverageConfig;
use crate::model::data_model::{AllDatasets, DatasetWithConfig};

pub fn normalize_data(all_datasets: AllDatasets) -> Vec<DatasetWithConfig> {
    all_datasets
        .items()
        .par_iter()
        .map(|dataset| {
            let dataset_name = dataset.name;

            if dataset.dataset_config.quarterly_avg_required {
                let dataset_quarterly_average_config: QuarterlyAverageConfig = dataset
                    .dataset_config
                    .quarterly_average_config
                    .clone()
                    .unwrap();

                let processed_dataframe = process_quarterly_average(
                    dataset_quarterly_average_config.date_column_name,
                    dataset_quarterly_average_config.target_column_name,
                    dataset_quarterly_average_config.target_column_alias,
                    dataset_quarterly_average_config.date_format_mask,
                    dataset.dataframe.clone(),
                )
                    .unwrap();

                DatasetWithConfig::new(dataset_name, processed_dataframe, dataset.dataset_config)
            } else {
                DatasetWithConfig::new(dataset_name, dataset.dataframe.clone(), dataset.dataset_config)
            }
        })
        .collect()
}


pub fn process_quarterly_average(date_column_name: &str,
                                 target_column_name: &str,
                                 target_column_alias: &str,
                                 date_format_mask: &str,
                                 mut df: DataFrame) -> PolarsResult<DataFrame> {
    let date_series = df
        .column(date_column_name)?
        .str()?
        .as_date(
            Some(date_format_mask),
            false
        )?;

    df.replace(date_column_name, date_series.into_series())?;

    let year = df.column(date_column_name)?.date()?.year();
    let month = df.column(date_column_name)?.date()?.month();

    let quarter_num = month
        .into_iter()
        .map(|opt_month| {
            opt_month.map(|m| {
                match m {
                    1..=3 => "Q1",
                    4..=6 => "Q2",
                    7..=9 => "Q3",
                    10..=12 => "Q4",
                    _ => "Invalid", // Should not happen
                }
            })
        })
        .collect::<StringChunked>();

    let mut binding = year
        .into_iter()
        .zip(quarter_num.into_iter())
        .map(|(opt_year, opt_quarter)| {
            match (opt_year, opt_quarter) {
                (Some(y), Some(q)) => Some(format!("{}-{}", y, q)),
                _ => None,
            }
        })
        .collect::<StringChunked>()
        .into_series();

    binding.rename("quarter".into());
    df.with_column(binding)?;

    let lazy_df = df.lazy();
    let lazy_result = lazy_df.select(&[
        col("quarter"),
        col(target_column_name),
    ])
        .group_by([col("quarter")])
        .agg([col(target_column_name).mean().alias(target_column_alias)]);

    let result = lazy_result.collect()?;

    Ok(result)
}

pub fn rename_columns(datasets: Vec<DatasetWithConfig>) -> Vec<DatasetWithConfig> {
    let mut output_datasets: Vec<DatasetWithConfig> = vec![];

    for dataset in datasets {
        let dataset_name = dataset.name;
        let mut dataframe = dataset.dataframe;
        let dataset_config = dataset.dataset_config;

        match dataset_name {
            "fx_rates" => {
                dataframe.set_column_names(["quarter", "eur_to_usd"]).unwrap();
            },
            "eu_government_debt" => {
                dataframe.set_column_names(["quarter", "eu_government_debt"]).unwrap();
            },
            "eu_gdp" => {
                dataframe.set_column_names(["quarter", "eu_gdp"]).unwrap();
            },
            "eu_inflation" => {
                dataframe.set_column_names(["quarter", "eu_inflation"]).unwrap();
            },
            _ => {}
        }

        let renamed_dataset = DatasetWithConfig::new(dataset_name, dataframe, dataset_config);
        output_datasets.push(renamed_dataset);
        }

    output_datasets
}

pub fn join_all_datasets(all_datasets: Vec<DatasetWithConfig>) -> DataFrame {
    let mut all_dataframes: Vec<DataFrame> = vec![];

    for dataset in all_datasets {
        let dataframe = dataset.dataframe;
        let mut df_no_nulls = dataframe.drop_nulls::<String>(None).unwrap();
        df_no_nulls.rechunk_mut();
        all_dataframes.push(df_no_nulls);
    }

    let mut df_out = all_dataframes.pop().unwrap();

    for df in all_dataframes {
        df_out = df_out.join(&df, ["quarter"], ["quarter"], JoinArgs::new(JoinType::Inner), None).unwrap();
        df_out.rechunk_mut();
    }

    df_out
}

pub fn convert_eu_to_usd(all_datasets: Vec<DatasetWithConfig>) -> Vec<DatasetWithConfig> {
    let fx_rates_df = all_datasets.clone()
        .into_iter()
        .find(|d| d.name == "fx_rates")
        .map(|d| d.dataframe).unwrap();

    let eu_dataset_names: HashSet<&'static str> = ["eu_gdp", "eu_government_debt"].into_iter().collect();

    let eu_datasets: Vec<DatasetWithConfig> = all_datasets.clone()
        .into_iter()
        .filter(|d| eu_dataset_names.contains(d.name))
        .map(|d| d)
        .collect();

    let mut converted_eu_datasets: Vec<DatasetWithConfig> = vec![];

    for eu_dataset in eu_datasets {
        let name = eu_dataset.name;
        let eu_dataset_config = eu_dataset.dataset_config;
        let eu_dataframe = eu_dataset.dataframe;

        let eu_dataframe_joined = eu_dataframe.join(&fx_rates_df, ["quarter"], ["quarter"], JoinArgs::new(JoinType::Left), None).unwrap();

        if name == "eu_gdp" {
            let eu_dataframe_converted = eu_dataframe_joined.lazy().with_column((col("eu_gdp") * col("eur_to_usd")).alias("eu_gdp_converted")).collect().unwrap();
            // let eu_dataframe_converted = eu_dataframe.lazy().with_column(col("eu_gdp") * col("eur_to_usd")).alias("eu_gdp_converted");
            let eu_dataframe_converted = eu_dataframe_converted.select(["quarter", "eu_gdp_converted"]).unwrap();
            let eu_dataset_with_config = DatasetWithConfig::new(name, eu_dataframe_converted, eu_dataset_config);
            converted_eu_datasets.push(eu_dataset_with_config);
        } else {
            let eu_dataframe_converted = eu_dataframe_joined.lazy().with_column((col("eu_government_debt") * col("eur_to_usd")).alias("eu_government_debt_converted")).collect().unwrap();
            let eu_dataframe_converted = eu_dataframe_converted.select(["quarter", "eu_government_debt_converted"]).unwrap();
            let eu_dataset_with_config = DatasetWithConfig::new(name, eu_dataframe_converted, eu_dataset_config);
            converted_eu_datasets.push(eu_dataset_with_config);
        }
    }

    let mut us_datasets: Vec<DatasetWithConfig> = all_datasets.clone()
        .into_iter()
        .filter(|d| !eu_dataset_names.contains(d.name))
        .map(|d| d)
        .collect();

    us_datasets.extend(converted_eu_datasets);

    us_datasets
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use polars::prelude::*;

    use crate::datasets_config::datasets_config::DatasetConfig;
    use crate::model::data_model::{AllDatasets, DatasetWithConfig};

    use super::*;

    #[test]
    fn test_process_quarterly_average() -> PolarsResult<()> {
        let dates = Series::new(PlSmallStr::from_str("date"), &[
            "2023-01-15", "2023-02-20", // Q1
            "2023-04-10",               // Q2
            "2023-07-25", "2023-08-05", // Q3
            "2023-11-01",               // Q4
        ]);

        let values = Series::new(PlSmallStr::from_str("value"), &[10.0, 20.0, 30.0, 40.0, 60.0, 90.0]);
        let df = DataFrame::new(vec![dates.into(), values.into()])?;
        let result = process_quarterly_average("date", "value", "quarterly_avg", "%Y-%m-%d", df)?;
        let sorted = result.sort(["quarter"], SortMultipleOptions::new())?;
        let quarter = sorted.column("quarter")?.str()?.into_no_null_iter().collect::<Vec<_>>();
        let avg = sorted.column("quarterly_avg")?.f64()?.into_no_null_iter().collect::<Vec<_>>();


        let expected_quarters = vec!["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4"];
        let expected_avg = vec![
            (10.0 + 20.0) / 2.0,
            30.0,
            (40.0 + 60.0) / 2.0,
            90.0,
        ];

        assert_eq!(quarter, expected_quarters);
        for (a, e) in avg.iter().zip(expected_avg.iter()) {
            assert!((a - e).abs() < 1e-6);
        }

        Ok(())
    }

    fn mock_dataframe(column_names: Option<Vec<&str>>) -> PolarsResult<DataFrame> {
        if column_names.is_some() {
            let column_names_actual = column_names.unwrap();
            df! {
                column_names_actual[0] => &["2023-01-01", "2023-02-01", "2023-03-01"],
                column_names_actual[1] => &[1.0, 2.0, 3.0]
            }
        } else {
            df! {
                "date" => &["2023-01-01", "2023-02-01", "2023-03-01"],
                "value" => &[1.0, 2.0, 3.0]
            }
        }
    }

    fn mock_dataset(name: &'static str, with_quarterly: bool) -> DatasetWithConfig {
        let config = if with_quarterly {
            DatasetConfig::new(
                name,
                "test_source",
                "test_id",
                true,
                Some(QuarterlyAverageConfig::new("date", "value", "%Y-%m", "avg_value")),
            )
        } else {
            DatasetConfig::new(name, "test_source", "test_id", false, None)
        };

        DatasetWithConfig::new(name, mock_dataframe(None).unwrap(), config)
    }

    #[test]
    fn test_normalize_data_mixed_datasets() {
        let dataset1 = mock_dataset("quarterly_dataset", true);
        let dataset2 = mock_dataset("regular_dataset", false);

        let all = AllDatasets::new(
            dataset1.clone(), // fx_rates_df
            dataset2.clone(), // fred_sp500_df
            dataset2.clone(), // fred_us_gdp_df
            dataset2.clone(), // fred_us_total_public_debt_df
            dataset2.clone(), // fred_us_inflation_df
            dataset2.clone(), // ecb_government_debt_df
            dataset2.clone(), // ecb_gdp_df
            dataset2.clone(), // ecb_inflation_df
        );

        let normalized = normalize_data(all);

        assert_eq!(normalized.len(), 8);

        let first = &normalized[0];
        assert_eq!(first.name, "quarterly_dataset");
        assert!(first.dataset_config.quarterly_avg_required);

        let others_all_regular = normalized[1..].iter().all(|ds| !ds.dataset_config.quarterly_avg_required);
        assert!(others_all_regular);
    }

    #[test]
    fn test_rename_columns() {
        fn check_column_rename(dataset_name: &'static str, expected_columns: Vec<&str>) {
            let df_config = DatasetConfig::new(dataset_name, "test_source", "test_id", false, None);
            let df = mock_dataframe(Some(vec!["quarter", "value"])).unwrap();
            let df_with_config = DatasetWithConfig::new(dataset_name, df, df_config);
            let renamed_df = rename_columns(vec![df_with_config]);
            let expected_df = mock_dataframe(Some(expected_columns)).unwrap();
            assert_eq!(renamed_df[0].dataframe, expected_df);
        }

        check_column_rename("fx_rates", vec!["quarter", "eur_to_usd"]);
        check_column_rename("eu_government_debt", vec!["quarter", "eu_government_debt"]);
        check_column_rename("eu_gdp", vec!["quarter", "eu_gdp"]);
        check_column_rename("eu_inflation", vec!["quarter", "eu_inflation"]);
        check_column_rename("unchanged", vec!["quarter", "value"]);
    }

    fn create_df(name: &str, quarters: Vec<&str>, values: Vec<f64>) -> DataFrame {
        let quarter_series = Series::new(PlSmallStr::from_str("quarter"), quarters);
        let value_series = Series::new(PlSmallStr::from_str(name), values);
        DataFrame::new(vec![quarter_series.into(), value_series.into()]).unwrap()
    }

    fn create_fx_df(quarters: Vec<&str>, rates: Vec<f64>) -> DataFrame {
        let quarter_series = Series::new(PlSmallStr::from_str("quarter"), quarters);
        let rate_series = Series::new(PlSmallStr::from_str("eur_to_usd"), rates);
        DataFrame::new(vec![quarter_series.into(), rate_series.into()]).unwrap()
    }

    #[test]
    fn test_convert_eu_to_usd() {
        let quarters = vec!["2023-Q1", "2023-Q2"];

        let gdp_df = create_df("eu_gdp", quarters.clone(), vec![100.0, 200.0]);
        let debt_df = create_df("eu_government_debt", quarters.clone(), vec![300.0, 400.0]);
        let inflation_df = create_df("eu_inflation", quarters.clone(), vec![2.0, 2.5]);
        let fx_df = create_fx_df(quarters.clone(), vec![1.1, 1.2]);
        let us_df = create_df("us_gdp", quarters.clone(), vec![500.0, 600.0]);

        let dummy_config = DatasetConfig::new("dummy_name",
                                              "dummy_source",
                                              "dummy_identifier",
                                              false,
                                              None);

        let datasets = vec![
            DatasetWithConfig::new("eu_gdp", gdp_df, dummy_config.clone()),
            DatasetWithConfig::new("eu_government_debt", debt_df, dummy_config.clone()),
            DatasetWithConfig::new("eu_inflation", inflation_df, dummy_config.clone()),
            DatasetWithConfig::new("fx_rates", fx_df, dummy_config.clone()),
            DatasetWithConfig::new("us_gdp", us_df.clone(), dummy_config.clone()),
        ];

        let result = convert_eu_to_usd(datasets);

        let result_map: HashMap<&str, &DataFrame> = result.iter()
            .map(|d| (d.name, &d.dataframe))
            .collect();

        let gdp_converted = result_map.get("eu_gdp").unwrap().column("eu_gdp_converted").unwrap().f64().unwrap();
        let val = gdp_converted.get(0).unwrap();
        assert!((val - 110.0).abs() < 1e-6);
        assert_eq!(gdp_converted.get(1), Some(240.0));

        let debt_converted = result_map.get("eu_government_debt").unwrap().column("eu_government_debt_converted").unwrap().f64().unwrap();
        assert_eq!(debt_converted.get(0), Some(330.0));
        assert_eq!(debt_converted.get(1), Some(480.0));

        let inflation_converted = result_map.get("eu_inflation").unwrap().column("eu_inflation").unwrap().f64().unwrap();
        assert_eq!(inflation_converted.get(0), Some(2.0));
        assert_eq!(inflation_converted.get(1), Some(2.5));

        // Check that non-EU dataset is untouched
        assert!(result_map.contains_key("us_gdp"));
        assert_eq!(**result_map.get("us_gdp").unwrap(), us_df);
    }
}


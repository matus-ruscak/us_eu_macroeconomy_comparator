use polars::prelude::*;
use crate::datasets_config::datasets_config::QuarterlyAverageConfig;
use crate::model::data_model::{AllDatasets, DatasetWithConfig};
use rayon::prelude::*;

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
                                 mut df: DataFrame) -> PolarsResult<DataFrame> {
    let date_series = df
        .column(date_column_name)?
        .str()?
        .as_date(
            Some("%Y-%m-%d"),
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


#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use super::*;
    use crate::model::data_model::{AllDatasets, DatasetWithConfig};
    use crate::datasets_config::datasets_config::DatasetConfig;

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
        let result = process_quarterly_average("date", "value", "quarterly_avg", df)?;
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

    fn mock_dataframe() -> PolarsResult<DataFrame> {
        df! {
            "date" => &["2023-01-01", "2023-02-01", "2023-03-01"],
            "value" => &[1.0, 2.0, 3.0]
        }
    }

    fn mock_dataset(name: &'static str, with_quarterly: bool) -> DatasetWithConfig {
        let config = if with_quarterly {
            DatasetConfig::new(
                name,
                "test_source",
                "test_id",
                true,
                Some(QuarterlyAverageConfig::new("date", "value", "avg_value")),
            )
        } else {
            DatasetConfig::new(name, "test_source", "test_id", false, None)
        };

        DatasetWithConfig::new(name, mock_dataframe().unwrap(), config)
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
}


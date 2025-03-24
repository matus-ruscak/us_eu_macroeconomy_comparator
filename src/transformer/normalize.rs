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
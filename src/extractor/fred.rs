use std::env;
use std::error::Error;
use config::{Config, Environment};
use dotenvy::dotenv;
use reqwest;
use reqwest::Client;
use serde::Deserialize;
use polars::prelude::*;
use polars::datatypes::DataType;


#[derive(Debug, Deserialize)]
pub struct Observation {
    date: String,
    value: String,
}

#[derive(Deserialize)]
pub struct FredResponse {
    observations: Vec<Observation>,
}

pub async fn get_data(series_id: &str, input_base_url: Option<&str>) -> Result<DataFrame, Box<dyn Error>> {
    let api_key = get_api_key();

    let default_base_url = "https://api.stlouisfed.org/fred/series/observations";
    let base_url = input_base_url.unwrap_or(default_base_url);

    let url = format!("{}?series_id={}&api_key={}&file_type=json", base_url, series_id, api_key);
    println!("retrieving data from fred: {url}");

    let client = Client::new();

    let response = client.get(url)
        .send().await?;

    let api_response: FredResponse = response.json().await.ok().unwrap();

    let dates: Vec<String> = api_response.observations.iter().map(|o| o.date.clone()).collect();
    let values: Vec<String> = api_response.observations.iter().map(|o| o.value.clone()).collect();

    let df = df![
        "date" => dates,
        "value" => values
    ].ok().unwrap();

    let df: DataFrame = df.lazy()
        .select([
            col("date"),
            col("value")
                .cast(DataType::Float64)
                .alias("value"),
        ]).collect()?;

    Ok(df)
}

fn get_api_key() -> String {
    dotenv().ok();

    let settings = Config::builder()
        .add_source(Environment::default()) // Environment variables take precedence
        .build()
        .expect("Failed to read configuration");

    let api_key = env::var("API_KEY")
        .or_else(|_| settings.get::<String>("api_key"))
        .expect("API_KEY not found");

    api_key
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_frame_equal(df1: &DataFrame, df2: &DataFrame) {
        assert_eq!(df1.shape(), df2.shape(), "Shape mismatch");

        let df1_sorted = sort_dataframe(df1);
        let df2_sorted = sort_dataframe(df2);

        assert_eq!(df1_sorted, df2_sorted, "DataFrames do not match");
    }

    fn sort_dataframe(df: &DataFrame) -> DataFrame {
        let mut sorted_cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        sorted_cols.sort();
        let sorted_df = df.select(&sorted_cols).unwrap();
        let sort_columns: Vec<String> = sorted_df.get_column_names().iter().map(|s| s.to_string()).collect();
        sorted_df.sort(&sort_columns, SortMultipleOptions::new()).unwrap()
    }
    #[tokio::test]
    async fn test_get_data_success() {
        let xml_response = r#"
        <root>
            <generic:ObsDimension value="2023-Q1" xmlns:generic="generic"/>
            <generic:ObsValue value="1.23" xmlns:generic="generic"/>
            <generic:ObsDimension value="2023-Q2" xmlns:generic="generic"/>
            <generic:ObsValue value="2.34" xmlns:generic="generic"/>
        </root>
        "#;

        let mut server = mockito::Server::new_async().await;

        server.mock("GET", "/mock-endpoint")
            .with_status(200)
            .with_header("content-type", "application/vnd.sdmx.genericdata+xml;version=2.1")
            .with_body(xml_response)
            .create();

        let endpoint = "/mock-endpoint";
        let base_url = server.url();

        let df_result = crate::extractor::ecb::get_data(&endpoint, Some(&base_url)).await.expect("Failed to get data");

        // Expected DataFrame
        let expected_quarters = Series::new(PlSmallStr::from_str("quarter"), &["2023-Q1", "2023-Q2"]);
        let expected_values = Series::new(PlSmallStr::from_str("value"), &[1.23, 2.34]);
        let expected_df = DataFrame::new(vec![expected_quarters.into(), expected_values.into()])
            .expect("Failed to create expected DataFrame");

        assert_frame_equal(&df_result, &expected_df);
    }
}
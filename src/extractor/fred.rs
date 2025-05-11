use log::{info};
use std::env;
use std::error::Error;

use config::{Config, Environment};
use dotenvy::dotenv;
use polars::datatypes::DataType;
use polars::prelude::*;
use reqwest;
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Observation {
    date: String,
    value: String,
}

#[derive(Deserialize)]
pub struct FredResponse {
    observations: Vec<Observation>,
}

pub async fn get_data(series_id: String, input_base_url: Option<&str>, get_api_key: fn() -> String) -> Result<DataFrame, Box<dyn Error + Send + Sync>> {
    let api_key = get_api_key();

    let default_base_url = "https://api.stlouisfed.org/fred/series/observations";
    let base_url = input_base_url.unwrap_or(default_base_url);

    let url = format!("{}?series_id={}&api_key={}&file_type=json", base_url, series_id, api_key);
    info!("retrieving data from fred: {url}");

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

pub fn get_fred_api_key() -> String {
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
    use std::env;

    use crate::tests::test_helpers::test_helpers::assert_frame_equal;

    use super::*;

    #[tokio::test]
    async fn test_get_data_success() {
        let json_response = r#"
        {"realtime_start":"2025-03-24","realtime_end":"2025-03-24","observation_start":"1600-01-01","observation_end":"9999-12-31","units":"lin","output_type":1,"file_type":"json","order_by":"observation_date","sort_order":"asc","count":236,"offset":0,"limit":100000,
            "observations":[
                {"realtime_start":"2025-03-24","realtime_end":"2025-03-24","date":"1966-01-01","value":"320999"},
                {"realtime_start":"2025-03-24","realtime_end":"2025-03-24","date":"1966-04-01","value":"316097"},
                {"realtime_start":"2025-03-24","realtime_end":"2025-03-24","date":"1966-07-01","value":"324748"}
                ]
            }
        "#;

        let mut server = mockito::Server::new_async().await;

        server.mock("GET", "/mock-endpoint?series_id=dummy_series&api_key=mocked_api_key&file_type=json")
            .with_status(200)
            .with_body(json_response)
            .create();


        let endpoint = "/mock-endpoint";
        let series_id = "dummy_series".to_string();
        fn mock_get_api_key() -> String {
            "mocked_api_key".to_string()
        }
        let base_url = server.url();
        let input_url = format!("{}{}", base_url, endpoint);

        let df_result = get_data(series_id, Some(&input_url), mock_get_api_key).await.expect("Failed to get data");

        // Expected DataFrame
        let expected_quarters = Series::new(PlSmallStr::from_str("quarter"), &["1966-01-01", "1966-04-01", "1966-07-01"]);
        let expected_values = Series::new(PlSmallStr::from_str("value"), &[320999, 316097, 324748]);
        let expected_df = DataFrame::new(vec![expected_quarters.into(), expected_values.into()])
            .expect("Failed to create expected DataFrame");

        assert_frame_equal(&df_result, &expected_df);
    }

    #[test]
    fn test_get_fred_api_key_from_env() {
        unsafe {
            env::set_var("API_KEY", "mocked_api_key");
        }
        let api_key = get_fred_api_key();
        assert_eq!(api_key, "mocked_api_key");
        unsafe {
            env::remove_var("API_KEY");
        }
    }
}

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

pub async fn get_data(series_id: &str) -> Result<DataFrame, Box<dyn Error>> {
    let api_key = get_api_key();

    let url = format!("https://api.stlouisfed.org/fred/series/observations?series_id={}&api_key={}&file_type=json", series_id, api_key);
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
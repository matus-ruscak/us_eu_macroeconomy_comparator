use polars::prelude::*;
use reqwest;
use reqwest::Client;
use reqwest::header;
use polars::frame::DataFrame;
use polars::datatypes::PlSmallStr;
use quick_xml::Reader;
use std::str;
use quick_xml::events::Event;
use std::error::Error;
use std::collections::HashMap;



pub async fn get_data(endpoint: &str, input_base_url: Option<&str>) -> Result<DataFrame, Box<dyn Error>> {
    let client = Client::new();
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        header::HeaderValue::from_static("application/vnd.sdmx.genericdata+xml;version=2.1")
    );
    let default_base_url = "https://data-api.ecb.europa.eu/service/data/";
    let base_url = input_base_url.unwrap_or(default_base_url);

    let url = format!("{}{}", base_url, endpoint);
    println!("retrieving data from ecb: {url}");

    let resp = client
        .get(&url)
        .headers(headers)
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(format!("Request failed with status: {}", resp.status()).into());
    }

    let response_body = resp.text().await?;

    let result = parse_xml(&response_body).unwrap();

    let quarters: Vec<&str> = result.keys().map(|s| s.as_str()).collect();
    let values: Vec<f64> = result.values().copied().collect();

    let quarter_col_name = PlSmallStr::from_str("quarter");
    let value_col_name = PlSmallStr::from_str("value");
    let quarter_series = Series::new(quarter_col_name, quarters);
    let value_series = Series::new(value_col_name, values);

    let df = DataFrame::new(vec![quarter_series.into(), value_series.into()]).unwrap();

    Ok(df)
    }

fn parse_xml(xml: &str) -> Result<HashMap<String, f64>, Box<dyn Error>> {
    let mut reader = Reader::from_str(xml);

    let mut quarters: Vec<String> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    while let Ok(event) = reader.read_event() {

        match event {
            Event::Empty(ref e) => {
                if e.name().as_ref().ends_with("generic:ObsDimension".as_bytes()) {
                    for attr in e.attributes() {
                        let attr = attr?;
                        let key = attr.key.as_ref();
                        let val = attr.value.clone();

                        if key == b"value" {
                            let quarter_value = String::from_utf8(val.into_owned())?;
                            quarters.push(quarter_value);
                        }
                    }
                }
                else if e.name().as_ref().ends_with("generic:ObsValue".as_bytes()) {
                    for attr in e.attributes() {
                        let attr = attr?;
                        let key = attr.key.as_ref();
                        let val = attr.value.clone();

                        if key == b"value" {
                            let debt_value = String::from_utf8(val.into_owned())?;
                            let debt_value_int: f64 = debt_value.parse()?;
                            values.push(debt_value_int);
                        }
                    }
                }
            }

            Event::Eof => break,
            _ => {}
        }
    }

    validate_quarters_and_values(&quarters, &values);

    let result_hashmap: HashMap<String, f64> = quarters.into_iter().zip(values.into_iter()).collect();

    Ok(result_hashmap)
}

fn validate_quarters_and_values(quarters: &Vec<String>, values: &Vec<f64>) -> () {
    if quarters.len() != values.len() {
        panic!("parse_xml() validation failed - quarters and values are not the same length.");
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    use mockito;
    use polars::datatypes::PlSmallStr;
    use crate::tests::test_helpers::test_helpers::assert_frame_equal;

    #[test]
    fn test_parse_xml_basic_case() {
        let xml_data = r#"
        <root>
            <generic:ObsDimension value="2024-Q1" xmlns:generic="generic"/>
            <generic:ObsValue value="1000.50" xmlns:generic="generic"/>
            <generic:ObsDimension value="2024-Q2" xmlns:generic="generic"/>
            <generic:ObsValue value="2000.75" xmlns:generic="generic"/>
            <generic:ObsDimension value="2024-Q3" xmlns:generic="generic"/>
            <generic:ObsValue value="3000.25" xmlns:generic="generic"/>
        </root>
        "#;

        let result = parse_xml(xml_data).expect("XML parsing failed");

        let mut expected = HashMap::new();
        expected.insert("2024-Q1".to_string(), 1000.50);
        expected.insert("2024-Q2".to_string(), 2000.75);
        expected.insert("2024-Q3".to_string(), 3000.25);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_xml_empty_input() {
        let xml_data = r#"<root></root>"#;

        let result = parse_xml(xml_data).expect("Should return an empty map");

        let expected: HashMap<String, f64> = HashMap::new();

        assert_eq!(result, expected);
    }

    #[test]
    #[should_panic(
        expected = "parse_xml() validation failed - quarters and values are not the same length."
    )]
    fn test_parse_xml_incomplete_data() {
        let xml_data = r#"
        <root>
            <generic:ObsDimension value="2025-Q1" xmlns:generic="generic"/>
            <!-- Missing ObsValue -->
            <generic:ObsDimension value="2025-Q2" xmlns:generic="generic"/>
            <generic:ObsValue value="1500.00" xmlns:generic="generic"/>
        </root>
        "#;

        parse_xml(xml_data).unwrap();
    }

    #[test]
    fn test_parse_xml_invalid_number() {
        let xml_data = r#"
        <root>
            <generic:ObsDimension value="2025-Q3" xmlns:generic="generic"/>
            <generic:ObsValue value="not_a_number" xmlns:generic="generic"/>
        </root>
        "#;

        let result = parse_xml(xml_data);

        assert!(result.is_err(), "Should return an error due to invalid number parsing");
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

        let df_result = get_data(&endpoint, Some(&base_url)).await.expect("Failed to get data");

        // Expected DataFrame
        let expected_quarters = Series::new(PlSmallStr::from_str("quarter"), &["2023-Q1", "2023-Q2"]);
        let expected_values = Series::new(PlSmallStr::from_str("value"), &[1.23, 2.34]);
        let expected_df = DataFrame::new(vec![expected_quarters.into(), expected_values.into()])
            .expect("Failed to create expected DataFrame");

        assert_frame_equal(&df_result, &expected_df);
    }

    #[tokio::test]
    async fn test_get_data_http_error() {
        let mut server = mockito::Server::new_async().await;

        server.mock("GET", "/mock-endpoint")
            .with_status(500)
            .create();

        let endpoint = "/mock-endpoint";
        let base_url = server.url();

        let result = get_data(&endpoint, Some(&base_url)).await;
        assert!(result.is_err(), "Expected an error on HTTP 500 response");
    }
}

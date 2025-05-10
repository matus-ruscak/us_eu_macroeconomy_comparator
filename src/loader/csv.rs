use std::fs::File;

use polars::prelude::*;

pub fn load(mut result_dataframe: DataFrame) -> () {
    let mut file = File::create("outputs/csv/result.csv").expect("could not create file");
    let _ = CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut result_dataframe);
}


#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;

    #[test]
    fn test_load_writes_csv_correctly() {
        let quarters = ["2025-01", "2025-02"];
        let financial_metrics = vec!["dummy_metric_1", "dummy_metric_2"];
        let quarter_series = Series::new(PlSmallStr::from_str("quarter"), quarters);
        let rate_series = Series::new(PlSmallStr::from_str("financial_metric"), financial_metrics);
        let df = DataFrame::new(vec![quarter_series.into(), rate_series.into()]).unwrap();

        fs::create_dir_all("outputs/csv").unwrap();

        load(df.clone());

        let path = "outputs/csv/result.csv";
        assert!(Path::new(path).exists(), "CSV file was not created");

        let contents = fs::read_to_string(path).expect("Failed to read result.csv");

        let expected = "quarter,financial_metric\n2025-01,dummy_metric_1\n2025-02,dummy_metric_2\n";

        let normalized = contents.replace("\r\n", "\n");
        assert_eq!(normalized, expected);
    }
}
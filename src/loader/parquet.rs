use std::fs::File;

use polars::prelude::*;

pub fn load(mut result_dataframe: DataFrame) -> () {
    let file = File::create("outputs/parquet/result.parquet").expect("could not create file");
    let _ = ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut result_dataframe);
}


#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;

    #[test]
    fn test_load_writes_parquet_correctly() {
        let df = df![
            "quarter" => &["2024-Q1", "2024-Q2"],
            "financial_metric" => &["dummy_metric_1", "dummy_metric_2"]
        ].unwrap();

        fs::create_dir_all("outputs/parquet").unwrap();

        load(df.clone());

        let path = "outputs/parquet/result.parquet";
        assert!(Path::new(path).exists(), "Parquet file was not created");

        let file = File::open(path).expect("Failed to open result.parquet");
        let read_df = ParquetReader::new(file).finish().expect("Failed to read Parquet");

        assert!(df.equals(&read_df));
    }
}
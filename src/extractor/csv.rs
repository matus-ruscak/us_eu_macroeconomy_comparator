use std::error::Error;

use polars::prelude::*;
use tokio::task;

pub async fn get_data(csv_file_path: String) -> Result<DataFrame, Box<dyn Error + Send + Sync>> {
    println!("retrieving data from csv: {csv_file_path}");

    let csv_file_path_owned = csv_file_path.to_string();

    let csv_df = task::spawn_blocking(move || {
        CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some(csv_file_path_owned.into()))
            .unwrap()
            .finish()
            .unwrap()
    }).await?;

    Ok(csv_df)
}
#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_get_data_reads_csv() {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        writeln!(temp_file, "name,age\nAlice,30\nBob,25").expect("Failed to write to temp file");
        let file_path = temp_file.path().to_str().unwrap().to_string();

        let df = get_data(file_path).await.unwrap();

        assert_eq!(df.shape(), (2, 2)); // 2 rows, 2 columns

        let name_series = df.column("name").unwrap();
        let name_str = name_series.str().unwrap(); // `.str()` instead of `.utf8()`
        assert_eq!(name_str.get(0), Some("Alice"));
        assert_eq!(name_str.get(1), Some("Bob"));

        let age_series = df.column("age").unwrap();
        let age_i32 = age_series.i64().unwrap();
        assert_eq!(age_i32.get(0), Some(30));
        assert_eq!(age_i32.get(1), Some(25));
    }
}
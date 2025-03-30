use polars::frame::DataFrame;
use polars::prelude::*;

pub fn assert_frame_equal(df1: &DataFrame, df2: &DataFrame) {
    assert_eq!(df1.shape(), df2.shape(), "Shape mismatch");

    let df1_sorted = sort_dataframe(df1);
    let df2_sorted = sort_dataframe(df2);

    assert_eq!(df1_sorted, df2_sorted, "DataFrames do not match");
}

pub fn sort_dataframe(df: &DataFrame) -> DataFrame {
    let mut sorted_cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    sorted_cols.sort();
    let sorted_df = df.select(&sorted_cols).unwrap();
    let sort_columns: Vec<String> = sorted_df.get_column_names().iter().map(|s| s.to_string()).collect();
    sorted_df.sort(&sort_columns, SortMultipleOptions::new()).unwrap()
}
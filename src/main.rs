mod extractor;
use polars::frame::DataFrame;
use extractor::csv::get_csv_file;
use extractor::fred::fetch_fred_data;
use extractor::ecb::fetch_ecb_data;
mod transformer;
mod loader;
mod utils;
mod tests;

#[tokio::main]
async fn main() {
    // EUR to USD exchange rate
    let fx_rates_df: DataFrame = get_csv_file("csv_data/DEXUSEU.csv").unwrap();
    println!("{fx_rates_df}");

    // S&P500
    let fred_sp500_id: &str = "SP500";
    let fred_sp500_df: DataFrame = fetch_fred_data(fred_sp500_id).await;
    println!("{fred_sp500_df}");

    // US GDP
    let fred_us_gdp_id: &str = "FYGDP";
    let fred_us_gdp_df: DataFrame = fetch_fred_data(fred_us_gdp_id).await;
    println!("{fred_us_gdp_df}");

    // US Total Public Debt
    let fred_us_total_public_debt_id: &str = "GFDEBTN";
    let fred_us_total_public_debt_df: DataFrame = fetch_fred_data(fred_us_total_public_debt_id).await;
    println!("{fred_us_total_public_debt_df}");

    // US Inflation
    let fred_us_inflation_id: &str = "CORESTICKM159SFRBATL";
    let fred_us_inflation_df: DataFrame = fetch_fred_data(fred_us_inflation_id).await;
    println!("{fred_us_inflation_df}");

    // ECB data - Government debt
    let ecb_government_debt = fetch_ecb_data().await;
    // println!("{ecb_government_debt}");

}
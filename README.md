# EU vs USA Macroeconomic Comparison

This Rust project compares macroeconomic indicators between the **European Union** and the **United States**, specifically focusing on:

- Inflation
- Gross Domestic Product (GDP)
- Total Government Debt

Each of these metrics is compared alongside the **S&P 500** index, allowing for a contextual understanding of market performance relative to fundamental economic conditions.

## Features

- Fetches and processes macroeconomic data from authoritative sources:
    - **EU Data**: [European Central Bank Data Portal](https://data.ecb.europa.eu/help/api/overview)
    - **US Data**: [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/docs/api/fred/)
    - **Foreign Exchange Rates**: Provided as a local CSV file, used to convert EUR values to USD for consistency
- Joins datasets on a common quarterly frequency — only quarters present in all sources are retained
- Outputs a consolidated CSV file: `result.csv`
- Generates visual comparisons of:
    - `debt.png`: S&P 500 vs Government Debt
    - `gdp.png`: S&P 500 vs GDP
    - `inflation.png`: S&P 500 vs Inflation

## Output Files

| File        | Description                                  |
|-------------|----------------------------------------------|
| `result.csv`| Merged dataset with all quarterly data       |
| `debt.png`  | Debt levels vs S&P 500 index                 |
| `gdp.png`   | GDP trends vs S&P 500 index                  |
| `inflation.png` | Inflation rates vs S&P 500 index         |

## Data Sources

- **ECB Data Portal**  
  Source of EU inflation, GDP, and debt data  
  [https://data.ecb.europa.eu/help/api/overview](https://data.ecb.europa.eu/help/api/overview)

- **FRED (St. Louis Fed)**  
  Source of US macroeconomic data  
  [https://fred.stlouisfed.org/docs/api/fred/](https://fred.stlouisfed.org/docs/api/fred/)

- **FX Rates CSV**  
  A manually downloaded CSV file with EUR/USD exchange rates, used for currency normalization.

## Usage
1. Register on the Federal Reserve Bank of St.Louis and generate an API key -> https://fredaccount.stlouisfed.org/apikey
2. Add a .env file and add the generated API key in the format API_KEY=<API_KEY>
3. Ensure all dependencies are installed:
   ```bash
   cargo build --release
4. Run the project:
   ```bash
   cargo run --release
5. View the output:
   - result.csv will contain the merged and cleaned data
   - Graphs will be saved as PNG images in the working directory

## License
MIT License
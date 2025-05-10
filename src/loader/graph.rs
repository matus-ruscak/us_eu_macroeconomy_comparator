use polars::prelude::*;
use plotters::prelude::*;

#[derive(Clone, Copy)]
pub struct GraphConfiguration {
    pub col_name_1: &'static str,
    pub col_name_1_label: &'static str,
    pub col_name_2: &'static str,
    pub col_name_2_label: &'static str,
    pub col_name_sp500: &'static str,
    pub col_name_sp500_label: &'static str,
    pub file_name: &'static str,
    pub caption: &'static str
}

impl GraphConfiguration {
    pub fn new(col_name_1: &'static str,
               col_name_1_label: &'static str,
               col_name_2: &'static str,
               col_name_2_label: &'static str,
               col_name_sp500: &'static str,
               col_name_sp500_label: &'static str,
               file_name: &'static str,
               caption: &'static str

    ) -> Self {
        GraphConfiguration {
            col_name_1,
            col_name_1_label,
            col_name_2,
            col_name_2_label,
            col_name_sp500,
            col_name_sp500_label,
            file_name,
            caption
        }
    }
}

pub fn generate_graphs(result: DataFrame) -> () {
    let result = result.lazy().with_column((col("sp500_usd") / lit(1000)).alias("sp500_usd_in_thousands")).collect().unwrap();
    let result = result.lazy().with_column((col("sp500_usd") * lit(10000)).alias("sp500_usd_mult_by_ten_thousand")).collect().unwrap();
    let result = result.lazy().with_column((col("eu_gdp_usd_millions") / lit(1000)).alias("eu_gdp_usd_billions")).collect().unwrap();

    let inflation_graph_configuration = GraphConfiguration::new("eu_inflation_perc",
                                                                "EU Inflation in %",
                                                                "us_inflation_perc",
                                                                "US Inflation in %",
                                                                "sp500_usd_in_thousands",
                                                                "S&P 500 in thousands",
                                                                "inflation",
                                                                "Inflation comparison EU vs USA");

    let gdp_graph_configuration = GraphConfiguration::new("eu_gdp_usd_billions",
                                                          "EU GDP in billions USD",
                                                          "us_gdp_usd_billions",
                                                          "US GDP in billions USD",
                                                          "sp500_usd",
                                                          "S&P 500 in thousands",
                                                          "gdp",
                                                          "GDP comparison EU vs USA");

    let total_debt_graph_configuration = GraphConfiguration::new("eu_government_debt_usd_millions",
                                                          "EU Government debt in millions USD",
                                                          "us_total_debt_usd_millions",
                                                          "US Debt in millions USD",
                                                          "sp500_usd_mult_by_ten_thousand",
                                                          "S&P 500 multiplied by 10'000",
                                                          "debt",
                                                          "Debt comparison EU vs USA");

    generate_graph(result.clone(), inflation_graph_configuration);
    generate_graph(result.clone(), gdp_graph_configuration);
    generate_graph(result.clone(), total_debt_graph_configuration);

}


fn generate_graph(result: DataFrame, graph_configuration: GraphConfiguration) -> () {
    let result = result.sort(["quarter"], SortMultipleOptions::new()).unwrap();
    let quarters = result.column("quarter").unwrap().str().unwrap().into_no_null_iter().collect::<Vec<_>>();
    let sp_500 = result.column(graph_configuration.col_name_sp500).unwrap().f64().unwrap().into_no_null_iter().collect::<Vec<_>>();
    let col_1 = result.column(graph_configuration.col_name_1).unwrap().f64().unwrap().into_no_null_iter().collect::<Vec<_>>();
    let col_2 = result.column(graph_configuration.col_name_2).unwrap().f64().unwrap().into_no_null_iter().collect::<Vec<_>>();

    // Set up drawing area
    let all_values = sp_500.iter()
        .chain(col_1.iter())
        .chain(col_2.iter())
        ;

    let (y_min, y_max) = all_values.clone().fold((f64::MAX, f64::MIN), |(min, max), &v| {
        (min.min(v), max.max(v))
    });
    let padding = (y_max - y_min) * 0.1;
    let y_range = (y_min - padding)..(y_max + padding);

    // Plotting
    let file_name = graph_configuration.file_name;
    let file_path = format!("outputs/graph/{file_name}.png");
    let root = BitMapBackend::new(&file_path, (2000, 1500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let mut chart = ChartBuilder::on(&root)
        .caption(graph_configuration.caption, ("sans-serif", 50))
        .margin(10)
        .x_label_area_size(50)
        .y_label_area_size(70)
        .build_cartesian_2d(0..quarters.len(), y_range).unwrap();

    chart.configure_mesh()
        .x_labels(quarters.len())
        .x_label_formatter(&|idx| quarters.get(*idx).unwrap_or(&"").to_string())
        .x_desc("Quarter")
        .y_desc("Value")
        .draw().unwrap();

    // Helper function to draw a series
    let mut draw_series = |name: &str, data: &Vec<f64>, color: RGBColor| -> Result<(), Box<dyn std::error::Error>> {
        chart.draw_series(LineSeries::new(
            (0..).zip(data.iter()).map(|(i, y)| (i, *y)),
            ShapeStyle::from(&color).stroke_width(2),
        ))?
            .label(name)
            .legend(move |(x, y)| {
                PathElement::new(
                    [(x, y), (x + 20, y)],
                    ShapeStyle::from(&color).stroke_width(3),
                )
            });
        Ok(())
    };

    // Draw each line
    draw_series(graph_configuration.col_name_sp500_label, &sp_500, RGBColor(0, 102, 0)).unwrap();
    draw_series(graph_configuration.col_name_1_label, &col_1, RGBColor(0, 0, 204)).unwrap();
    draw_series(graph_configuration.col_name_2_label, &col_2, RGBColor(204, 0, 0)).unwrap();

    // Draw the legend
    chart.configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .position(SeriesLabelPosition::UpperLeft)
        .label_font(("sans-serif", 40)) // ← Increase font size here
        .draw().unwrap();

    println!("Plot saved as {file_name}.png");
}

use std::path::PathBuf;
use walkdir::WalkDir;
use bybit::{read_daily};
use bybit::TickArray;

const TICK_PATH: &str = "F:/bybit/tick";
const PATH: &str = "F:/bybit/daily";

fn main() {
    let mut tick_array = TickArray::new();
    for i in WalkDir::new(PATH).sort_by_file_name().min_depth(1).max_depth(1).into_iter().filter_map(|entry| entry.ok())
    {
        println!("detecting {:?}", i);
        let daily = i.file_name();
        let output_path = PathBuf::from(TICK_PATH).join(format!("{}.parquet", daily.to_str().unwrap()));
        if output_path.exists() {
            println!("ignore {:?}, existed", output_path);
        } else {
            let read_daily_path = PathBuf::from(PATH).join(daily);
            if let Ok(array) = read_daily(read_daily_path.to_str().unwrap(), 5) {
                for tick in array {
                    tick_array.push(&tick);
                }
            }
            println!("wrote to {:?}", output_path);
            tick_array.output(output_path)
        }
    }
}
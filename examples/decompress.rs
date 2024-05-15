///
/// 处理每日数据模块
///
use std::fs;
use std::path::Path;
use walkdir::WalkDir;
use bybit::{decompress_gz, extract_date, unzip_to};

const OUT_PATH: &str = "F:/bybit/daily";
const ZIP_PATH: &str = "F:/bybit/order_book_zip/";
const GZ_PATH: &str = "F:/bybit/trade_zip/";

fn decompress_orderbook() {
    let dir = WalkDir::new(ZIP_PATH);
    for i in dir {
        if let Ok(file) = i {
            let path = file.path().as_os_str().to_str().unwrap();
            if path.eq(ZIP_PATH) {
                continue;
            }
            unzip_to(path, OUT_PATH).unwrap();
        }
    }
}


fn decompress_trade() {
    let out_path: &Path = Path::new(OUT_PATH);
    let dir = WalkDir::new(GZ_PATH).min_depth(1).max_depth(1).sort_by_file_name().into_iter().filter_map(|x| x.ok());
    for file in dir {
        let path = file.path().as_os_str().to_str().unwrap();
        let file_name = file.file_name().to_str().unwrap();
        let date_dir = out_path.join(extract_date(file_name));
        if !date_dir.exists() {
            fs::create_dir_all(date_dir.clone()).unwrap();
        }
        let output_path = date_dir.join(file_name.replace(".gz", ""));
        if output_path.exists() {
            println!("ignore {}, existed", output_path.to_str().unwrap());
        } else {
            println!("resolve: {} write_to:{:?}", path, output_path);
            decompress_gz(path, output_path.to_str().unwrap()).unwrap();
        }
    }
}


fn main() {
    decompress_orderbook();
    decompress_trade();
}
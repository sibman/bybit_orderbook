mod util;
mod tick;
mod parquet;

use std::collections::{BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use csv::Reader;
use regex::Error;
use serde::{Deserialize, Serialize};
use walkdir::DirEntry;
use crate::util::{PriceVecArray, round};
pub use util::{extract_date, unzip_to, decompress_gz};
pub use crate::tick::TickData;
pub use crate::parquet::TickArray;

pub fn filter_file(date_dir: &str, file_type: &str) -> DirEntry {
    let dir = walkdir::WalkDir::new(date_dir);
    let mut data_file = dir.into_iter().filter_map(|entry| {
        if let Ok(entry) = entry {
            if entry.file_name().to_str().unwrap().ends_with(file_type) {
                Some(entry)
            } else {
                None
            }
        } else {
            None
        }
    }).collect::<Vec<DirEntry>>();
    data_file.pop().unwrap()
}

#[derive(Debug)]
pub enum Data {
    Depth(Depth),
    Trade(Trade),
}

impl Data {
    fn timestamp(&self) -> u64 {
        match self {
            Data::Depth(x) => x.timestamp,
            Data::Trade(x) => x.timestamp as u64
        }
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub enum OrderBookType {
    #[serde(alias = "delta")]
    Delta,
    #[serde(alias = "snapshot")]
    Snapshot,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Direction {
    Buy,
    Sell,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DepthData {
    #[serde(alias = "s")]
    pub symbol: String,
    #[serde(alias = "a")]
    pub ask: PriceVecArray,
    #[serde(alias = "b")]
    pub bid: PriceVecArray,
    #[serde(alias = "seq")]
    pub seq: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Depth {
    #[serde(alias = "topic")]
    pub topic: String,
    #[serde(alias = "type")]
    pub data_type: OrderBookType,
    #[serde(alias = "ts")]
    pub timestamp: u64,
    #[serde(alias = "data")]
    pub data: DepthData,
    #[serde(alias = "cts")]
    pub cts: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Trade {
    pub timestamp: f64,
    pub symbol: String,
    #[serde(alias = "side")]
    pub direction: Direction,
    #[serde(alias = "size")]
    pub volume: f64,
    pub price: f64,
    #[serde(alias = "tickDirection")]
    pub tick_direction: String,
    #[serde(alias = "trdMatchID")]
    pub match_id: String,
}

/// 读取每日数据 包含成交量+orderbook流
/// 需要将数据按照时间进行排序 然后以此回放得到数据切片
pub fn read_daily(date_dir: &str, interval: i32) -> Result<Vec<TickData>, Error> {
    let mut result: Vec<Data> = Vec::new();
    let instant = Instant::now();

    let book_file = filter_file(date_dir, ".data");
    let data_file = File::open(book_file.path()).unwrap();
    let buf = BufReader::new(data_file);

    for line in buf.lines().map_while(Result::ok) {
        let depth: Depth = serde_json::from_str(line.as_str()).expect("parse error");
        result.push(Data::Depth(depth));
    }
    let csv_file = filter_file(date_dir, ".csv");
    let file = File::open(csv_file.path()).unwrap();
    let mut reader = Reader::from_reader(file);
    for line in reader.deserialize() {
        let mut record: Trade = line.unwrap();
        record.timestamp = (record.timestamp * 1000.0).round();
        result.push(Data::Trade(record));
    }

    println!("read: {:?} and {:?} len:{} time:{}s", csv_file, book_file, result.len(), instant.elapsed().as_secs());

    let data = sort_data(result);
    Ok(merge_tick(data, interval))
}

pub fn sort_data(mut data: Vec<Data>) -> Vec<Data> {
    data.sort_by(|x, y| {
        let time_1 = x.timestamp();
        let time_2 = y.timestamp();
        time_1.cmp(&time_2)
    });
    data
}


#[derive(Default)]
pub struct OrderBook {
    ask: BTreeMap<u64, f64>,
    bid: BTreeMap<u64, f64>,
    buy_volume: f64,
    sell_volume: f64,
    init: bool,
    timestamp: u64,
    turnover: f64,
    volume: f64,
    symbol: u64,
    last_price: f64,
}


impl OrderBook {
    pub fn update_with_depth(&mut self, depth: Depth) {
        for (ask_price, volume) in depth.data.ask.0 {
            if volume.gt(&0.0) {
                self.ask.insert(ask_price.to_bits(), volume);
            } else {
                self.ask.remove(&ask_price.to_bits());
            }
        }
        for (bid_price, volume) in depth.data.bid.0 {
            if volume.gt(&0.0) {
                self.bid.insert(bid_price.to_bits(), volume);
            } else {
                self.bid.remove(&bid_price.to_bits());
            }
        }
    }
    pub fn update_with_trade(&mut self, trade: Trade) {
        match trade.direction {
            Direction::Buy => {
                self.buy_volume = round(self.buy_volume + trade.volume);
            }
            Direction::Sell => {
                self.sell_volume = round(self.sell_volume + trade.volume);
            }
        }
        self.volume = round(self.volume + trade.volume);
        self.turnover += trade.volume * trade.price;
        self.last_price = trade.price;
    }

    fn get_five_lob(&self, direction: u8) -> [(f64, f64); 5] {
        let mut five = [(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)];
        if direction == 1 {
            for (index, (price, volume)) in self.ask.iter().enumerate() {
                five[index] = (f64::from_bits(*price), *volume);
                if index >= 4 {
                    break;
                }
            }
        } else {
            for (index, (price, volume)) in self.bid.iter().rev().enumerate() {
                five[index] = (f64::from_bits(*price), *volume);
                if index >= 4 {
                    break;
                }
            }
        }
        five
    }

    pub fn snapshot_tick(&mut self) -> TickData {
        let ask = self.get_five_lob(1);
        let bid = self.get_five_lob(0);
        let snap_time = self.timestamp / 1000;
        let ms = (self.timestamp % 1000) as u16;
        let mid_price = round(ask[0].0 + bid[0].0) / 2.0;
        TickData {
            ask_price: ask,
            bid_price: bid,
            mid_price,
            turnover: self.turnover,
            volume: self.volume,
            buy_volume: self.buy_volume,
            sell_volume: self.sell_volume,
            snap_time,
            ms,
            last_price: self.last_price,
            code: self.symbol,
        }
    }

    pub fn reset(&mut self) {
        self.buy_volume = 0.0;
        self.sell_volume = 0.0;
        self.volume = 0.0;
        self.turnover = 0.0;
    }
}


/// 将数据进行排序后 然后重新按照指定周期进行采样, 默认为100ms
pub fn merge_tick(data: Vec<Data>, interval: i32) -> Vec<TickData> {
    let instant = Instant::now();
    let mut book = OrderBook::default();
    let mut opt = vec![];
    let mut count = 0;
    for i in data {
        match i {
            Data::Depth(depth) => {
                match depth.data_type {
                    OrderBookType::Snapshot => {
                        book.init = true;
                        book.timestamp = depth.timestamp;
                        book.update_with_depth(depth);
                    }
                    OrderBookType::Delta => {
                        if book.init {
                            book.timestamp = depth.timestamp;
                            book.update_with_depth(depth);
                            if count % interval == 0 {
                                let tick = book.snapshot_tick();
                                opt.push(tick);
                                book.reset();
                            }
                            count += 1;
                        }
                    }
                }
            }
            Data::Trade(trade) => if book.init { book.update_with_trade(trade) }
        }
    }
    println!("merge to orderbook: {}s", instant.elapsed().as_secs());
    opt
}

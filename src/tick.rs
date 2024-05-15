use recv::TickDataStructure;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TickData {
    pub ask_price: [(f64, f64); 5],
    pub bid_price: [(f64, f64); 5],
    pub mid_price: f64,
    pub turnover: f64,
    pub volume: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub snap_time: u64,
    pub ms: u16,
    pub last_price: f64,
    pub code: u64,
}

impl TickDataStructure<f64> for TickData {
    fn last_price(&self) -> f64 {
        self.last_price
    }

    fn volume(&self) -> f64 {
        self.volume
    }

    fn open_interest(&self) -> f64 {
        0.0
    }

    fn bid_price(&self, index: usize) -> f64 {
        unsafe { self.bid_price.get_unchecked(index).0 }
    }

    fn ask_price(&self, index: usize) -> f64 {
        unsafe { self.ask_price.get_unchecked(index).0 }
    }

    fn bid_volume(&self, index: usize) -> f64 {
        unsafe { self.bid_price.get_unchecked(index).1 }
    }

    fn ask_volume(&self, index: usize) -> f64 {
        unsafe { self.ask_price.get_unchecked(index).1 }
    }

    fn mid_price(&self) -> f64 {
        self.mid_price
    }

    fn turnover(&self) -> f64 {
        self.turnover
    }

    fn hms(&self, _base_time: u32) -> (u32, u32, u32) {
        todo!()
    }

    fn timestamp(&self, _base_time: u64) -> u64 {
        self.snap_time
    }

    fn snap_time(&self) -> u64 {
        self.snap_time
    }

    fn ms(&self) -> u16 {
        self.ms
    }

    fn code(&self) -> u64 {
        self.code
    }

    fn ask_volume_all(&self) -> f64 {
        self.ask_volume(0)
            + self.ask_volume(1)
            + self.ask_volume(2)
            + self.ask_volume(3)
            + self.ask_volume(4)
    }

    fn bid_volume_all(&self) -> f64 {
        self.bid_volume(0)
            + self.bid_volume(1)
            + self.bid_volume(2)
            + self.bid_volume(3)
            + self.bid_volume(4)
    }

    fn buy_volume(&self) -> f64 {
        self.buy_volume
    }

    fn sell_volume(&self) -> f64 {
        self.sell_volume
    }
}

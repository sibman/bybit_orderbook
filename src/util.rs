use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{BufReader, copy};
use std::ops::{Deref, DerefMut};
use flate2::bufread;

use zip::ZipArchive;

type Error = Box<dyn std::error::Error + Send + Sync>;


use regex::Regex;
use serde::{Deserialize, Serialize};
use serde::de;


pub fn extract_date(filename: &str) -> PathBuf {
    let re = Regex::new(r"(\d{4}-\d{2}-\d{2})").unwrap();
    // re.find(filename).and_then(|mat| Some(mat.as_str().to_string())).unwrap().into()
    re.find(filename).map(|mat| mat.as_str().to_string()).unwrap().into()
}

pub fn unzip_to<P: AsRef<Path> + Copy>(target: P, des: P) -> Result<(), Error> {
    let file = File::open(target)?;
    let mut zip_file = ZipArchive::new(file)?;
    if !des.as_ref().exists() {
        fs::create_dir_all(des.as_ref())?;
    }
    for i in 0..zip_file.len() {
        let mut index_file = zip_file.by_index(i)?;
        let name = index_file.name();
        let date = extract_date(name);
        let date_dir = des.as_ref().join(date);
        if !date_dir.exists() {
            tracing::debug!("{:?} not exist, create it", date_dir);
            fs::create_dir_all(date_dir.clone())?;
        }
        let target = date_dir.join(name);

        if target.exists() {
            println!("ignore {:?}, existed", target);
            continue;
        } else if index_file.is_dir() {
            println!("create {:?}", target);
            fs::create_dir_all(target)?;
        } else {
            println!("write to {:?}", target);
            let mut target_file = OpenOptions::new().create(true).truncate(true).write(true).read(true).open(target)?;
            copy(&mut index_file, &mut target_file)?;
        }
    }
    Ok(())
}


pub fn decompress_gz(input_path: &str, output_path: &str) -> Result<(), Error> {
    // 打开.gz文件
    let input_file = File::open(input_path)?;
    let input = BufReader::new(input_file);
    let mut output = File::create(output_path)?;
    let mut decoder = bufread::GzDecoder::new(input);
    copy(&mut decoder, &mut output)?;
    Ok(())
}


#[derive(Debug, Clone, Default, Serialize)]
pub struct PriceVecArray(pub Vec<(f64, f64)>);

impl Deref for PriceVecArray {
    type Target = Vec<(f64, f64)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PriceVecArray {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'de> Deserialize<'de> for PriceVecArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: de::Deserializer<'de>,
    {
        struct TupleVecVisitor;

        impl<'de> de::Visitor<'de> for TupleVecVisitor {
            type Value = Vec<(f64, f64)>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence of tuples")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: de::SeqAccess<'de>,
            {
                let mut tuple_vec = Vec::new();
                while let Some((x, y)) = seq.next_element::<(&'de str, &'de str)>()? {
                    let x = x.parse().map_err(de::Error::custom)?;
                    let y = y.parse().map_err(de::Error::custom)?;
                    tuple_vec.push((x, y));
                }
                Ok(tuple_vec)
            }
        }

        deserializer
            .deserialize_seq(TupleVecVisitor)
            .map(PriceVecArray)
    }
}

pub fn round(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

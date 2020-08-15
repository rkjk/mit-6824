mod master;
mod wc;
mod worker;

use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use std::cmp::{Eq, PartialEq};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum Task {
    Map,
    Reduce,
    Wait,
    Exit,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Payload {
    task: Task,
    file: PathBuf,
}

#[derive(Debug, Clone, Ord, PartialOrd, Serialize, Deserialize)]
pub struct KeyValue {
    key: String,
    value: String,
}

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl Eq for KeyValue {}

pub trait MapReduce {
    /// Map part of Mapreduce
    ///
    /// filename is the filename that we have to consume
    /// contents -> contents of the file from which we have to generate key-value pairs
    fn map<'a>(&self, filename: &'a str, contents: &'a str) -> Vec<KeyValue>;

    /// Reduce part of Mapreduce
    ///
    /// key is the key that this instance of reduce is tasked with
    /// Values is the list of values associated with this key
    fn reduce<'a>(&self, key: &'a str, values: Vec<&'a str>) -> String;
}

fn main() {
    let now = Instant::now();
    // The master module launches in a different thread and returns a CloseHandle
    let master = master::start_server();

    // Launch one or more workers here
    worker::worker(4);

    // Close master once the workers have finished
    // This is different from the original Mapreduce implementation. Check Paper
    master.close();
    println!("Mapreduce in {} milliseconds", now.elapsed().as_millis());
}

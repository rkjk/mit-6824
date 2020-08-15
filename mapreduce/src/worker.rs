#[macro_use]
use jsonrpc_client_core::{jsonrpc_client, expand_params};
use jsonrpc_client_http::HttpTransport;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::fs::{read_to_string, OpenOptions};
use std::io::prelude::*;
use std::io::BufReader;
use std::iter::FromIterator;
use std::thread;
use std::time::Duration;

use crate::{wc::Wc, KeyValue, MapReduce, Payload, Task};

pub struct MapReducef {
    pub mapreducef: Box<dyn MapReduce>,
    id: u32,
}

impl MapReducef {
    fn map(&self, filename: &str) {
        let path = "./data/".to_string() + filename;

        let mut intermediate: Vec<KeyValue> = Vec::new();
        let content = read_to_string(path).unwrap();
        let kva = self.mapreducef.map(filename, &content);
        for val in kva.into_iter() {
            intermediate.push(val);
        }
        let outfile = "./output/parallel/".to_string() + filename;
        let file = File::create(outfile).unwrap();

        serde_json::to_writer(&file, &intermediate).unwrap();
        println!("Thread {} -> Map task finished: {}", self.id, filename);
    }

    fn reduce(&self, filename: &str) {
        // TODO: Read contents of file from /dev/shm into a HashMap called intermediate
        // Lump all items with the same key together
        let infile = "./output/parallel/".to_string() + filename;
        let file = File::open(infile).unwrap();
        let reader = BufReader::new(file);
        let mut intermediate: Vec<KeyValue> = serde_json::from_reader(reader).unwrap();

        intermediate.sort_by(|a, b| a.key.cmp(&b.key));
        let outfile = "./output/parallel/mr-out-".to_string() + filename;
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(outfile)
            .unwrap();
        let mut i = 0;
        while i < intermediate.len() {
            let mut j = i + 1;
            while j < intermediate.len() && intermediate[j].key == intermediate[i].key {
                j += 1;
            }
            let values: Vec<&str> = Vec::from_iter(
                intermediate[i..j]
                    .iter()
                    .map(|item| item.value.as_str())
                    .collect::<Vec<&str>>(),
            );
            let output = self.mapreducef.reduce(intermediate[i].key.as_str(), values);

            writeln!(
                &mut file,
                "{:?} {:?}",
                intermediate[i].key.to_string(),
                output
            )
            .unwrap();
            i = j;
        }
        println!("Thread {} -> Reduce task finished: {}", self.id, filename);
    }
}

jsonrpc_client!(pub struct MapReduceClient {
    /// Call master and return task.
    pub fn return_task(&mut self) -> RpcRequest<String>;
});

fn send_request(id: u32) {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle("http://127.0.0.1:3030").unwrap();
    let mut client = MapReduceClient::new(transport_handle);

    let word_counter: Wc = Wc {};
    let mapreduce_worker = MapReducef {
        mapreducef: Box::new(word_counter),
        id: id,
    };
    //let mut count = 100;
    loop {
        //count -= 1;
        //if count == 0 {
        //    break;
        //}
        let response = client.return_task().call().unwrap();
        let result: Payload = serde_json::from_str(&response).unwrap();
        match result.task {
            Task::Exit => break,
            Task::Wait => thread::sleep(Duration::from_millis(100)),
            Task::Map => {
                let file = result.file.to_str();
                match file {
                    Some(_) => mapreduce_worker.map(file.unwrap()),
                    None => println!("Map Task: No file found"),
                };
            }
            Task::Reduce => {
                let file = result.file.to_str();
                match file {
                    Some(_) => mapreduce_worker.reduce(file.unwrap()),
                    None => println!("Reduce Task: No file found"),
                };
            }
        }
    }
}

pub fn worker(num_worker: u32) {
    let mut worker_vec = vec![];

    for i in 0..num_worker {
        worker_vec.push(thread::spawn(move || {
            println!("Spawning thread {}", i);
            send_request(i);
        }))
    }
    for worker in worker_vec {
        // Wait for the thread to finish. Returns a result.
        let _ = worker.join();
    }
}

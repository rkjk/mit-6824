use std::fs::{read_to_string, OpenOptions};
use std::io::prelude::*;
use std::iter::FromIterator;

use crate::{KeyValue, MapReduce};

pub struct MapReduceSeq {
    pub mapreducef: Box<dyn MapReduce>,
}

impl MapReduceSeq {
    /// Run Map function followed by Reduce function in sequence
    pub fn run(&self, filenames: Vec<&str>) {
        let mut intermediate: Vec<KeyValue> = Vec::new();
        for f in filenames {
            let content = read_to_string(f).unwrap();
            let kva = self.mapreducef.map(f, &content);
            for val in kva.into_iter() {
                intermediate.push(val);
            }
        }

        // Lump all items with the same key together
        intermediate.sort_by(|a, b| a.key.cmp(&b.key));
        let outfile = "mr-out-0";
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            //.append(true)
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
    }
}

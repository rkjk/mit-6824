//
// a word-count application "plugin" for MapReduce.
//
use crate::{Map, Reduce, KeyValue};

pub struct Wc;

impl Map for Wc {
    ///
    /// The map function is called once for each file of input. The first
    /// argument is the name of the input file, and the second is the
    /// file's complete contents. You should ignore the input file name,
    /// and look only at the contents argument. The return value is a slice
    /// of key/value pairs.
    ///
    fn map<'a>(&self, filename: &'a str, contents: &'a str) -> Vec<KeyValue<'a>> {
        let words = contents.split(|c: char| !c.is_alphanumeric());

        let mut kva: Vec<KeyValue> = Vec::new();

        for word in words {
            if word == "" {
                continue;
            }
            let kv = KeyValue {
                key: word,
                value: "1"
            };
            kva.push(kv);
        }
        kva
    }
}

impl Reduce for Wc {
    ///
    /// The reduce function is called once for each key generated by the
    /// map tasks, with a list of all the values created for that key by
    /// any map task.
    ///
    fn reduce(&self, key: &str, values: Vec<&str>) -> String {
        values.len().to_string()
    }
}
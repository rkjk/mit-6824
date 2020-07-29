pub mod mrsequential;
pub mod wc;

#[derive(Debug, Clone, Ord, PartialOrd)]
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

#[cfg(test)]
mod tests {
    #[test]
    fn wc_map_test() {
        use crate::{wc::Wc, KeyValue, MapReduce};
        use std::fs;
        let word_counter: Wc = Wc {};
        let filename = "./data/smalltest.txt";
        let test_string = fs::read_to_string(filename).unwrap();
        let map_output = word_counter.map(filename, &test_string);
        assert_eq!(
            vec![
                KeyValue {
                    key: String::from("This"),
                    value: String::from("1")
                },
                KeyValue {
                    key: String::from("is"),
                    value: String::from("1")
                },
                KeyValue {
                    key: String::from("a"),
                    value: String::from("1")
                },
                KeyValue {
                    key: String::from("test"),
                    value: String::from("1")
                }
            ],
            map_output
        );
    }

    #[test]
    fn wc_reduce_test() {
        use crate::{wc::Wc, MapReduce};

        let word_counter: Wc = Wc {};
        assert_eq!("3", word_counter.reduce("test", vec!["1", "2", "3"]));
    }

    #[test]
    fn wc_mapreduce_test() {
        use crate::{mrsequential::MapReduceSeq, wc::Wc};
        use std::time::Instant;

        let word_counter: Wc = Wc {};

        let mapreduceseq = MapReduceSeq {
            mapreducef: Box::new(word_counter),
        };
        //let files = vec!["./data/smalltest.txt"];
        let files = vec![
            "./data/pg-being_ernest.txt",
            "./data/pg-dorian_gray.txt",
            "./data/pg-frankenstein.txt",
            "./data/pg-grimm.txt",
            "./data/pg-huckleberry_finn.txt",
            "./data/pg-metamorphosis.txt",
            "./data/pg-sherlock_holmes.txt",
            "./data/pg-tom_sawyer.txt",
        ];
        let now = Instant::now();
        mapreduceseq.run(files);
        println!(
            "Sequential Mapreduce in {} milliseconds",
            now.elapsed().as_millis()
        );
    }
}

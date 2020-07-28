pub mod wc;

#[derive(Debug)]
pub struct KeyValue<'a> {
    key: &'a str,
    value: &'a str,
}

impl<'a> PartialEq for KeyValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl<'a> Eq for KeyValue<'a> {}

pub trait Map {
    /// Map part of Mapreduce
    ///
    /// filename is the filename that we have to consume
    /// contents -> contents of the file from which we have to generate key-value pairs
    fn map<'a>(&self, filename: &'a str, contents: &'a str) -> Vec<KeyValue<'a>>;
}

pub trait Reduce {
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
        use crate::{wc::Wc, KeyValue, Map};
        use std::fs;
        let word_counter: Wc = Wc {};
        let filename = "./data/smalltest.txt";
        let test_string = fs::read_to_string(filename).unwrap();
        let map_output = word_counter.map(filename, &test_string);
        assert_eq!(
            vec![
                KeyValue {
                    key: "This",
                    value: "1"
                },
                KeyValue {
                    key: "is",
                    value: "1"
                },
                KeyValue {
                    key: "a",
                    value: "1"
                },
                KeyValue {
                    key: "test",
                    value: "1"
                }
            ],
            map_output
        );
    }

    #[test]
    fn wc_reduce_test() {
        use crate::{wc::Wc, Reduce};

        let word_counter: Wc = Wc {};
        assert_eq!("3", word_counter.reduce("test", vec!["1", "2", "3"]));
    }
}

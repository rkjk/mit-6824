mod master;
mod worker;

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use std::cmp::{Eq, PartialEq};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
enum Task {
    Map,
    Reduce,
    Wait,
    Exit,
}

#[derive(Serialize, Deserialize, Debug)]
struct Payload {
    task: Task,
    file: PathBuf,
}

fn main() {
    // The master module launches in a different thread and returns a CloseHandle
    let master = master::start_server();

    // Launch one or more workers here
    worker::send_request();

    // Close master once the workers have finished
    // This might be different from the original Mapreduce implementation. Check Paper
    master.close();
}

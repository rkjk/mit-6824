use std::thread;

mod master;
mod worker;

fn main() {
    let master = master::start_server();
    worker::send_request();
    //master.join().unwrap();
    master.close();
}

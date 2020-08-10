mod master;
mod worker;

fn main() {
    // The master module launches in a different thread and returns a CloseHandle
    let master = master::start_server();

    // Launch one or more workers here
    worker::send_request();

    // Close master once the workers have finished
    // This might be different from the original Mapreduce implementation. Check Paper
    master.close();
}

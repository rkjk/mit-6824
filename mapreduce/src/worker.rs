#[macro_use]
use jsonrpc_client_core::{jsonrpc_client, expand_params};
use jsonrpc_client_http::HttpTransport;

use crate::{Payload, Task};
use serde::{Deserialize, Serialize};

jsonrpc_client!(pub struct MapReduceClient {
    /// Call master and return task.
    pub fn return_task(&mut self) -> RpcRequest<String>;
});

pub fn send_request() {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle("http://127.0.0.1:3030").unwrap();
    let mut client = MapReduceClient::new(transport_handle);
    //let mut count = 100;
    loop {
        //count -= 1;
        //if count == 0 {
        //    break;
        //}
        let response = client.return_task().call().unwrap();
        let result: Payload = serde_json::from_str(&response).unwrap();
        if result.task == Task::Exit {
            break;
        }
        println!("{:?}", result);
        println!("");
    }
}

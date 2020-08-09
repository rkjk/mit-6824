#[macro_use]
use jsonrpc_client_core::{jsonrpc_client, expand_params};
use jsonrpc_client_http::HttpTransport;

jsonrpc_client!(pub struct MapReduceClient {
    /// Call master and return task.
    pub fn return_file(&mut self) -> RpcRequest<String>;
});

pub fn send_request() {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle("http://127.0.0.1:3030").unwrap();
    let mut client = MapReduceClient::new(transport_handle);
    loop {
        let result = client.return_file().call().unwrap();
        if result == "0" {
            break;
        }
        println!("{}", result);
    }
}

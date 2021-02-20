use jsonrpc_client_core::{expand_params, jsonrpc_client};

use jsonrpc_core::types::error;
use jsonrpc_core::Result;

use super::{AppendEntriesPayload, Payload, RequestVotePayload, HEARTBEAT};
use jsonrpc_client_http::HttpTransport;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

jsonrpc_client!(pub struct RpcClient {
    /// Counterparts to the RPC endpoints on the sending side
    pub fn request_vote(&mut self, payload: RequestVotePayload) -> RpcRequest<String>;

    pub fn append_entries(&mut self, payload: AppendEntriesPayload) -> RpcRequest<String>;
});

lazy_static! {
    /// Initialize the Rpc Sender at the beginning of program execution
    pub static ref SEND_RPC: Mutex<RpcSender> = Mutex::new(RpcSender {
        receivers: HashMap::new()
    });
}
/// RpcSender structure that caches the HTTP handle for each replica node. Otherwise, we run out of threads very soon due to the heartbeat threads
pub struct RpcSender {
    receivers: HashMap<String, HttpTransport>,
}

impl RpcSender {
    /// Check Cache for an existing HttpHandle for the given destination. Return if exists otherwise create a new one and cache it
    fn get_http_transport(&mut self, destination: String) -> jsonrpc_client_http::HttpHandle {
        let destination = "http://".to_string() + &destination;
        let http_transport = self.receivers.get(&destination);
        match http_transport {
            None => {
                let transport = HttpTransport::new()
                    .timeout(Duration::from_millis(HEARTBEAT as u64 / 2))
                    .standalone()
                    .unwrap();
                let handle = transport.handle(&destination).unwrap();
                self.receivers.insert(destination, transport);
                return handle;
            }
            Some(transport) => {
                let handle = transport.handle(&destination).unwrap();
                return handle;
            }
        }
    }
    /// Send Payload to the given destination (IP Address or URL)
    /// Depending on the type of payload, send_rpc will send either RequestVote or AppendEntries RPC
    pub fn send_rpc(&mut self, destination: String, payload: Payload) -> Result<String> {
        let transport_handle = self.get_http_transport(destination);
        let mut client = RpcClient::new(transport_handle);
        let response = match payload {
            Payload::RequestVote(data) => client.request_vote(data).call(),
            Payload::AppendEntries(data) => client.append_entries(data).call(),
        };
        match response {
            Ok(val) => return Ok(val),
            Err(_) => {
                return Err(error::Error {
                    code: error::ErrorCode::InternalError,
                    message: "Something went wrong when sending RPC".to_owned(),
                    data: None,
                })
            }
        };
    }
}

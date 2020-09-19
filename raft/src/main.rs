#[macro_use]
use jsonrpc_client_core::{jsonrpc_client, expand_params};
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use jsonrpc_client_http::HttpTransport;
use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{CloseHandle, Server, ServerBuilder};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Debug)]
enum State {
    Follower,
    Candidate,
    Leader,
}

type Payload = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVotePayload {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

enum RpcType {
    RequestVote,
    AppendEntries,
}

type LogType = (u64, u64);

#[derive(Debug)]
struct Node {
    currentTerm: u64,
    state: State,
    votedFor: Option<u64>,
    log: Vec<LogType>,
}

#[rpc]
pub trait Rpc {
    /// Pop File name if available and return else signal slave to exit
    #[rpc(name = "request_vote")]
    fn request_vote(&self, payload: Payload) -> Result<String>;

    #[rpc(name = "append_entries")]
    fn append_entries(&self, payload: Payload) -> Result<String>;
}

jsonrpc_client!(pub struct RpcClient {
    pub fn request_vote(&mut self, payload: Payload) -> RpcRequest<String>;

    pub fn append_entries(&mut self, payload: Payload) -> RpcRequest<String>;
});

impl Node {
    fn new() -> Self {
        Node {
            currentTerm: 0,
            state: State::Follower,
            votedFor: None,
            log: Vec::new(),
        }
    }

    fn call_election(&mut self, replica_urls: &Vec<String>) {
        println!("Calling election");
        self.state = State::Candidate;
        self.currentTerm += 1;
        println!("Increase term to {}", self.currentTerm);
        println!("Issue parallel RequestVote RPCs");
        println!("Replicas: {:?}", replica_urls);

        for url in replica_urls {
            let url = url.clone();
            std::thread::spawn(
                move || match send_rpc(url.to_string(), RpcType::RequestVote) {
                    Ok(_) => (),
                    Err(val) => println!("{}", val),
                },
            );
        }
    }
}

#[derive(Debug)]
struct NodeRpc {
    node: Arc<RwLock<Node>>,
    tx_election_timer: Sender<bool>,
    id: u64,
    replica_urls: Vec<String>,
}

impl NodeRpc {
    fn new(node_id: u64, urls: &Vec<String>) -> (NodeRpc, String, Receiver<bool>) {
        let mut replica_urls = Vec::new();
        let address = urls[node_id as usize].clone();
        for (i, v) in urls.iter().enumerate() {
            if i != node_id as usize {
                replica_urls.push(v.clone());
            }
        }
        // Create channel for communication between main thread and election timer thread.
        // Move rx to the election timer thread and tx to the NodeRpc Object
        let (tx, rx) = unbounded();
        let node_rpc = NodeRpc {
            node: Arc::new(RwLock::new(Node::new())),
            tx_election_timer: tx,
            id: node_id,
            replica_urls: replica_urls,
        };

        return (node_rpc, address, rx);
    }
}

impl Rpc for NodeRpc {
    /// RequestVote Rpcs are handled here
    fn request_vote(&self, payload: Payload) -> Result<String> {
        println!(
            "Node {}: Got RequestVote Rpc with payload {:?}",
            self.id, payload
        );
        match self.tx_election_timer.send(true) {
            Ok(_) => {
                println!("Reset Election timer");
            }
            Err(_) => {
                println!(
                    "Main Thread: Cannot communicate with election timeout thread. Return Early"
                );
                return Err(error::Error {
                    code: error::ErrorCode::InternalError,
                    message: "Something went wrong when resetting election timer".to_owned(),
                    data: None,
                });
            }
        }
        Ok("Got Vote".to_string())
    }

    fn append_entries(&self, payload: Payload) -> Result<String> {
        println!(
            "Node {}: Got AppendEntries Rpc with payload {:?}",
            self.id, payload
        );
        Ok("Got data".to_string())
    }
}

/// Election Timer function. Accepts Receiver's side of an unbounded channel
/// The Timer is reset everytime it receives communication from the main thread. If no communication for a period
/// of 150-300ms (random), then election is called.
fn election_timer(rx: Receiver<bool>, node_clone: Arc<RwLock<Node>>, replica_urls: Vec<String>) {
    loop {
        let mut rng = rand::thread_rng();
        let timeout = Duration::from_millis(rng.gen_range(4000, 10000)); // Should be 150-300 ms
        match rx.recv_timeout(timeout) {
            Ok(_val) => (),
            Err(RecvTimeoutError::Timeout) => {
                println!("Election Timeout thread: Timeout elapsed");
                let node_wlock = node_clone.write();
                match node_wlock {
                    Ok(_) => {
                        println!("Election Timeout thread: Calling election");
                        node_wlock.unwrap().call_election(&replica_urls);
                    }
                    Err(_) => (),
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                println!("No communication from main thread. Exit");
                break;
            }
        }
    }
}

fn send_rpc(destination: String, rpc_type: RpcType) -> Result<String> {
    let data = 176;
    let destination = "http://".to_string() + &destination;
    println!("Destination: {}", destination);
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle(&destination).unwrap();
    let mut client = RpcClient::new(transport_handle);
    let response = match rpc_type {
        RpcType::RequestVote => client.request_vote(data).call(),
        RpcType::AppendEntries => client.append_entries(data).call(),
    };
    let resp = match response {
        Ok(val) => val,
        Err(_) => {
            return Err(error::Error {
                code: error::ErrorCode::InternalError,
                message: "Something went wrong when sending RPC".to_owned(),
                data: None,
            })
        }
    };

    let result: std::result::Result<(), serde_json::error::Error> = serde_json::from_str(&resp);
    println!("{:?}", result);
    return Ok("success".to_string());
}

fn read_config() -> (u64, Vec<String>) {
    let args: Vec<String> = env::args().collect();

    // Get Node ID from command line argument
    let id = match args[1].parse::<u64>() {
        Ok(val) => val,
        Err(_) => panic!("Invalid Node Id"),
    };

    // Read the IP Address - Port pairs of all nodes from config file
    // The Node ID gives us the IP-PORT of the current node
    let config_file = match File::open("./config") {
        Ok(f) => f,
        Err(_) => panic!("Cannot find config file"),
    };
    let nodes: Vec<String> = io::BufReader::new(config_file)
        .lines()
        .map(|s| s.unwrap())
        .collect::<Vec<String>>();
    (id, nodes)
}

fn main() {
    let (id, nodes) = read_config();
    let (node_rpc, address, rx) = NodeRpc::new(id, &nodes);
    let replicas = node_rpc.replica_urls.clone();
    let node_clone = Arc::clone(&node_rpc.node);

    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(node_rpc.to_delegate());
    let server = ServerBuilder::new(io)
        .threads(1)
        .start_http(&address.parse().unwrap())
        .unwrap();

    println!("Node Address: {:?}", server.address());
    std::thread::spawn(move || election_timer(rx, node_clone, replicas));
    server.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_election_timeout() {
        let (id, nodes) = (
            0,
            vec![
                "127.0.0.1:3030".to_string(),
                "127.0.0.1:4030".to_string(),
                "127.0.0.1:5030".to_string(),
            ],
        );
        let (mut node_rpc, address, rx) = NodeRpc::new(id, &nodes);
        println!("Node Wrapper created: {:?}", node_rpc);
        let mut node_clone = Arc::clone(&node_rpc.node);

        std::thread::spawn(move || {
            election_timer(
                rx,
                node_clone,
                vec!["127.0.0.1:4030".to_string(), "127.0.0.1:5030".to_string()],
            );
        });

        for _ in 0..10 {
            println!("Main thread: Send ping");
            let timeout_reset = node_rpc.tx_election_timer.send(true);
            match timeout_reset {
                Ok(_) => {
                    let mut rng = rand::thread_rng();
                    std::thread::sleep_ms(rng.gen_range(150, 300));
                }
                Err(_) => {
                    println!("Main Thread: Cannot communicate with election timeout thread. Exit");
                    break;
                }
            }
        }
    }
}

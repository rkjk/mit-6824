#[macro_use]
use jsonrpc_client_core::{jsonrpc_client, expand_params};
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use jsonrpc_client_http::HttpTransport;
use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::{Arc, RwLock};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

#[derive(Debug)]
enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Payload {
    RequestVote(RequestVotePayload),
    AppendEntries(AppendEntriesPayload),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestVotePayload {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendEntriesPayload {
    term: u64,
}

type LogType = (u64, u64);

#[derive(Debug)]
struct Node {
    current_term: u64,
    state: State,
    voted_for: Option<u64>,
    log: Vec<LogType>,
}

#[rpc]
pub trait Rpc {
    /// Rpc Endpoints for the node
    #[rpc(name = "request_vote")]
    fn request_vote(&self, payload: RequestVotePayload) -> Result<String>;

    #[rpc(name = "append_entries")]
    fn append_entries(&self, payload: AppendEntriesPayload) -> Result<String>;
}

jsonrpc_client!(pub struct RpcClient {
    /// Counterparts to the endpoints on the sending side
    pub fn request_vote(&mut self, payload: RequestVotePayload) -> RpcRequest<String>;

    pub fn append_entries(&mut self, payload: AppendEntriesPayload) -> RpcRequest<String>;
});

impl Node {
    fn new() -> Self {
        Node {
            current_term: 0,
            state: State::Follower,
            voted_for: None,
            log: vec![(0, 0)],
        }
    }

    fn call_election(&mut self, id: u64, replica_urls: &Vec<String>) {
        println!("Calling election");
        self.state = State::Candidate;
        self.current_term += 1;
        self.log.push((self.current_term, 0));
        println!("Increase term to {}", self.current_term);
        let mut join_handles: Vec<JoinHandle<Result<String>>> = Vec::new();
        for url in replica_urls {
            let payload = Payload::RequestVote(RequestVotePayload {
                term: self.current_term,
                candidate_id: id,
                last_log_index: self.log.len() as u64,
                last_log_term: self.log.last().unwrap().0.clone() as u64,
            });
            let url = url.clone();
            join_handles.push(
                Builder::new()
                    .name(url.to_string())
                    .spawn(move || {
                        return send_rpc(url.to_string(), payload);
                    })
                    .unwrap(),
            );
        }
        let mut response_vec = Vec::new();
        for t in join_handles {
            response_vec.push(t.join().unwrap());
        }
        let mut num_votes = 0;
        let total_nodes = replica_urls.len();
        for r in response_vec.iter() {
            match r {
                Ok(v) => {
                    if v == "yes" {
                        num_votes += 1;
                    }
                }
                _ => (),
            }
        }
        if num_votes > (total_nodes - 1) / 2 {
            self.state = State::Leader;
            println!("Elected Leader!!!!!!")
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
    fn request_vote(&self, payload: RequestVotePayload) -> Result<String> {
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
        let node = self.node.read().unwrap();

        // If potential leader's term is less than current term, vote NO
        if node.current_term > payload.term {
            return Ok("no".to_string());
        }
        let latest_log = node.log.last().unwrap();

        // If voted_for is None, or if potential leader's log is as at least as long as and as up to date as current log, vote YES
        if node.voted_for == None
            || (payload.last_log_term >= latest_log.0
                && payload.last_log_index >= node.log.len() as u64)
        {
            // Drop Read lock
            drop(node);

            // Get write lock to the node
            let mut node_wlock = self.node.write().unwrap();
            node_wlock.voted_for = Some(payload.candidate_id);
            return Ok("yes".to_string());
        }
        Ok("no".to_string())
    }

    fn append_entries(&self, payload: AppendEntriesPayload) -> Result<String> {
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
fn election_timer(
    rx: Receiver<bool>,
    node_clone: Arc<RwLock<Node>>,
    replica_urls: Vec<String>,
    id: u64,
) {
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
                        node_wlock.unwrap().call_election(id, &replica_urls);
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

fn send_rpc(destination: String, payload: Payload) -> Result<String> {
    //let data = 176;
    let destination = "http://".to_string() + &destination;
    //println!("Destination: {}", destination);
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle(&destination).unwrap();
    let mut client = RpcClient::new(transport_handle);
    let response = match payload {
        Payload::RequestVote(data) => client.request_vote(data).call(),
        Payload::AppendEntries(data) => client.append_entries(data).call(),
    };
    let resp = match response {
        Ok(val) => return Ok(val),
        Err(_) => {
            return Err(error::Error {
                code: error::ErrorCode::InternalError,
                message: "Something went wrong when sending RPC".to_owned(),
                data: None,
            })
        }
    };
    /*
    let result: std::result::Result<String, serde_json::error::Error> = serde_json::from_str(&resp);
    match result {
        Ok(val) => return Ok(val),
        Err(err) => {
            return Err(error::Error {
                code: error::ErrorCode::InternalError,
                message: format!("Deserialization error: {:?}", err),
                data: None,
            })
        }
    }
    */
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
    std::thread::spawn(move || election_timer(rx, node_clone, replicas, id));
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

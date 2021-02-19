use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};

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
use std::time::Duration;

mod node;
mod rpcsender;

use node::Node;

static TIMER_LOW: u64 = 150;
static TIMER_HIGH: u64 = 300;
static HEARTBEAT: u32 = TIMER_LOW as u32 / 3;

/// Possible states for a Node
#[derive(Debug, PartialEq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

/// Payload for the 2 possible RPC requests
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Payload {
    RequestVote(RequestVotePayload),
    AppendEntries(AppendEntriesPayload),
}

/// Payload for a RequestVote RPC
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestVotePayload {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

/// Payload for an AppendEntries RPC
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppendEntriesPayload {
    term: u64,
}

/// Log Entry -> (Log Index, Value)
type LogType = (u64, u64);

#[rpc]
pub trait Rpc {
    /// Rpc Endpoints for the node. Represent the HTTP/other protocol endpoints to which RPC clients send requests
    ///
    /// ```
    /// Endpoint for RequestVote RPCs
    /// ```
    #[rpc(name = "request_vote")]
    fn request_vote(&self, payload: RequestVotePayload) -> Result<String>;
    /// Endpoint for AppendEntries RPC
    #[rpc(name = "append_entries")]
    fn append_entries(&self, payload: AppendEntriesPayload) -> Result<String>;
}

/// Data structure that will accept RPCs and reset election timer using the Sender side of an unbounded crossbeam channel.
/// Holds an Atomic Reference Counter to the main Node data structure
#[derive(Debug)]
struct NodeRpc {
    node: Arc<RwLock<Node>>,
    tx_election_timer: Sender<bool>,
}

impl NodeRpc {
    /// Return a new instance of NodeRpc appropriately initialized
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
            node: Arc::new(RwLock::new(Node::new(node_id, replica_urls))),
            tx_election_timer: tx,
        };

        return (node_rpc, address, rx);
    }

    /// Reset the election timer by sending a bool over the channel
    fn reset_timer(&self) -> Result<String> {
        match self.tx_election_timer.send(true) {
            Ok(_) => {
                println!("Reset Election timer");
                return Ok("Reset Election timer".to_string());
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
    }
}

impl Rpc for NodeRpc {
    /// Implement the Rpc trait (RPC Endpoints declared in the trait)
    ///
    /// ```
    /// Handle RequestVote Rpc requests
    /// ```
    fn request_vote(&self, payload: RequestVotePayload) -> Result<String> {
        println!("Got RequestVote Rpc with payload {:?}", payload);
        self.reset_timer()?;

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

    /// Handle AppendEntries RPC requests
    fn append_entries(&self, payload: AppendEntriesPayload) -> Result<String> {
        println!("Got AppendEntries Rpc with payload {:?}", payload);
        self.reset_timer()?;
        Ok("Got data".to_string())
    }
}

/// Election Timer function. Accepts Receiver's side of an unbounded channel
/// The Timer is reset everytime it receives communication from the main thread. If no communication for a period
/// of 150-300ms (random), then election is called.
fn election_timer(rx: Receiver<bool>, node_clone: Arc<RwLock<Node>>) {
    loop {
        let mut rng = rand::thread_rng();
        let timeout = Duration::from_millis(rng.gen_range(TIMER_LOW, TIMER_HIGH)); // Should be 150-300 ms
        match rx.recv_timeout(timeout) {
            Ok(_val) => (),
            Err(RecvTimeoutError::Timeout) => {
                println!("Election Timeout thread: Timeout elapsed");
                let node_wlock = node_clone.write();
                match node_wlock {
                    Ok(_) => {
                        println!("Election Timeout thread: Calling election");
                        node_wlock.unwrap().call_election();
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

/// Read config file in the root directory and extract ID of current node and IP Address:Port combinations of the other nodes.AppendEntriesPayload
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
    let node_clone = Arc::clone(&node_rpc.node);

    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(node_rpc.to_delegate());
    let server = ServerBuilder::new(io)
        .threads(1)
        .start_http(&address.parse().unwrap())
        .unwrap();

    println!("Node Address: {:?}", server.address());
    std::thread::spawn(move || election_timer(rx, node_clone));
    server.wait();
}

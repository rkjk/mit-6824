use jsonrpc_http_server::ServerBuilder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod node;
mod noderpc;
mod rpcsender;
mod utils;

use noderpc::{NodeRpc, Rpc};
use utils::{election_timer, read_config};

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

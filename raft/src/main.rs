use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError};
use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{CloseHandle, Server, ServerBuilder};
use rand::Rng;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

enum State {
    Follower,
    Candidate,
    Leader,
}

type LogType = (u64, u64);

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
    fn request_vote(&self) -> Result<String>;
}

impl Node {
    fn new() -> Self {
        Node {
            currentTerm: 0,
            state: State::Follower,
            votedFor: None,
            log: Vec::new(),
        }
    }

    fn call_election(&mut self) {
        println!("Calling election");
        self.state = State::Candidate;
        self.currentTerm += 1;
        println!("Increase term to {}", self.currentTerm);
        println!("Issue parallel RequestVote RPCs");
    }
}

struct NodeRpc {
    node: Arc<RwLock<Node>>,
}

impl NodeRpc {
    fn new() -> NodeRpc {
        NodeRpc {
            node: Arc::new(RwLock::new(Node::new())),
        }
    }
}

fn election_timer(rx: Receiver<bool>, node_clone: Arc<RwLock<Node>>) {
    loop {
        let mut rng = rand::thread_rng();
        let timeout = Duration::from_millis(rng.gen_range(150, 300));
        match rx.recv_timeout(timeout) {
            Ok(val) => (),
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

fn main() {
    println!("Starting Server");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_election_timeout() {
        let (tx, rx) = unbounded();
        let mut node_rpc = NodeRpc::new();
        let mut node_clone = Arc::clone(&node_rpc.node);

        std::thread::spawn(move || {
            election_timer(rx, node_clone);
        });

        for _ in 0..10 {
            println!("Main thread: Send ping");
            let timeout_reset = tx.send(true);
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

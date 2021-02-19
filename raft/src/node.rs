use super::{AppendEntriesPayload, LogType, Payload, RequestVotePayload, State, HEARTBEAT};
use jsonrpc_core::Result;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

use super::rpcsender;

/// Main Node data structure, holds all the information required for a Raft Node
#[derive(Debug)]
pub struct Node {
    pub current_term: u64,
    state: State,
    pub voted_for: Option<u64>,
    pub log: Vec<LogType>,
    id: u64,
    pub replica_urls: Vec<String>,
}

/// Function that sends a heartbeat periodically to all node_replicas
fn heartbeat(node_replicas: Vec<String>, payload: Payload) {
    loop {
        for node in &node_replicas {
            let payload_clone = payload.clone();
            let node_clone = node.clone();
            std::thread::spawn(move || {
                match rpcsender::SendRpc
                    .lock()
                    .unwrap()
                    .send_rpc(node_clone, payload_clone)
                {
                    Err(x) => println!("Heartbeat failed: {}", x),
                    _ => (),
                }
            });
            std::thread::sleep(Duration::from_millis(HEARTBEAT as u64));
        }
    }
}

impl Node {
    /// Return a new Node instance with the data members initialized
    pub fn new(id: u64, replica_urls: Vec<String>) -> Self {
        Node {
            current_term: 0,
            state: State::Follower,
            voted_for: None,
            log: vec![(0, 0)],
            id: id,
            replica_urls: replica_urls,
        }
    }

    /// Change state to Candidate and send RequestVote RPCs to all other nodes
    pub fn call_election(&mut self) {
        if self.state == State::Leader {
            println!("Already elected leader");
            return;
        }
        println!("Calling election");
        self.state = State::Candidate;
        self.current_term += 1;
        self.log.push((self.current_term, 0));
        println!("Increase term to {}", self.current_term);
        let mut join_handles: Vec<JoinHandle<Result<String>>> = Vec::new();
        for url in &self.replica_urls {
            let payload = Payload::RequestVote(RequestVotePayload {
                term: self.current_term,
                candidate_id: self.id,
                last_log_index: self.log.len() as u64,
                last_log_term: self.log.last().unwrap().0.clone() as u64,
            });
            let url = url.clone();
            join_handles.push(
                Builder::new()
                    .name(url.to_string())
                    .spawn(move || {
                        return rpcsender::SendRpc
                            .lock()
                            .unwrap()
                            .send_rpc(url.to_string(), payload);
                    })
                    .unwrap(),
            );
        }
        let mut response_vec = Vec::new();
        for t in join_handles {
            response_vec.push(t.join().unwrap());
        }

        // Vote for self
        let mut num_votes = 1;
        let total_nodes = self.replica_urls.len();
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
            println!("Elected Leader!. Start Heartbeat thread");
            let payload = Payload::AppendEntries(AppendEntriesPayload {
                term: self.current_term,
            });
            let node_replicas = self.replica_urls.clone();
            std::thread::spawn(move || {
                heartbeat(node_replicas, payload);
            });
        }
    }
}

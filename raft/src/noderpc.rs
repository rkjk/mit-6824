use super::{AppendEntriesPayload, RequestVotePayload};
use crossbeam_channel::{unbounded, Receiver, Sender};
use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use std::sync::{Arc, RwLock};

use super::node::Node;

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
pub struct NodeRpc {
    pub node: Arc<RwLock<Node>>,
    tx_election_timer: Sender<bool>,
}

impl NodeRpc {
    /// Return a new instance of NodeRpc appropriately initialized
    pub fn new(node_id: u64, urls: &Vec<String>) -> (NodeRpc, String, Receiver<bool>) {
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

use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use std::sync::RwLock;

#[rpc]
pub trait Rpc {
    /// Pop File name if available and return else signal slave to exit
    #[rpc(name = "return_file")]
    fn return_file(&self) -> Result<String>;
}

pub struct RpcImpl {
    files: RwLock<Vec<String>>,
}

impl RpcImpl {
    /// Files to return to Slave
    fn new() -> Self {
        RpcImpl {
            files: RwLock::new(vec![
                "pg-being_ernest.txt".to_string(),
                "pg-dorian_gray.txt".to_string(),
                "pg-frankenstein.txt".to_string(),
                "pg-grimm.txt".to_string(),
                "pg-huckleberry_finn.txt".to_string(),
                "pg-metamorphosis.txt".to_string(),
                "pg-sherlock_holmes.txt".to_string(),
                "pg-tom_sawyer.txt".to_string(),
            ]),
        }
    }
}

impl Rpc for RpcImpl {
    fn return_file(&self) -> Result<String> {
        {
            let files = self.files.read().unwrap();
            if files.is_empty() {
                return Ok("0".to_owned());
            }
        }
        let mut files = self.files.write().unwrap();
        match files.pop() {
            Some(val) => return Ok(val),
            None => {
                return Err(error::Error {
                    code: error::ErrorCode::InternalError,
                    message: "Something went wrong when returning filename".to_owned(),
                    data: None,
                })
            }
        }
    }
}

pub fn rpc_test() {
    let rpc_impl = RpcImpl::new();
    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(rpc_impl.to_delegate());

    let server = ServerBuilder::new(io)
        .threads(1)
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .unwrap();

    server.wait();
}

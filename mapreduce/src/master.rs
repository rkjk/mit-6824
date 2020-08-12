use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{CloseHandle, ServerBuilder};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::{Payload, Task};

static FAIL_TIME: Duration = Duration::from_secs(1);

#[rpc]
pub trait Rpc {
    /// Pop File name if available and return else signal slave to exit
    #[rpc(name = "return_file")]
    fn return_task(&self) -> Result<Payload>;
}

/// Use a Read-Write Lock for thread-safe Interior Mutability
/// The RpcImpl trait does not allow for &mut.
pub struct RpcImpl {
    mapfiles: RwLock<VecDeque<String>>,
    reducefiles: RwLock<VecDeque<String>>,
    mapdone: RwLock<u32>,
    reducedone: RwLock<u32>,
}

impl RpcImpl {
    /// Files to return to Slave
    fn new() -> Self {
        RpcImpl {
            mapfiles: RwLock::new(
                vec![
                    "pg-being_ernest.txt".to_string(),
                    "pg-dorian_gray.txt".to_string(),
                    "pg-frankenstein.txt".to_string(),
                    "pg-grimm.txt".to_string(),
                    "pg-huckleberry_finn.txt".to_string(),
                    "pg-metamorphosis.txt".to_string(),
                    "pg-sherlock_holmes.txt".to_string(),
                    "pg-tom_sawyer.txt".to_string(),
                ]
                .into_iter()
                .collect(),
            ),
            mapdone: RwLock::new(0),
            reducedone: RwLock::new(0),
            reducefiles: RwLock::new(VecDeque::new()),
        }
    }

    fn check_task(&self, task: Task, filename: String) {
        thread::sleep(FAIL_TIME);
        match task {
            Task::Map => {
                *self.mapdone.write().unwrap() += 1;
                self.reducefiles
                    .write()
                    .unwrap()
                    .push_front(filename.to_owned());
            }
            Task::Reduce => {
                *self.reducedone.write().unwrap() += 1;
            }
        }
    }
}

impl Rpc for RpcImpl {
    // Check if task is finished
    /// RPC handler function that will return tasks to the worker
    fn return_task(&self) -> Result<Payload> {
        let mapdone_num = self.mapdone.read().unwrap().clone();
        let task = match mapdone_num {
            7 => Task::Map,
            _ => Task::Reduce,
        };
        {
            let files = match task {
                Task::Map => *self.mapfiles.read().unwrap(),
                Task::Reduce => *self.reducefiles.read().unwrap(),
                _ => panic!("No matching task"),
            };
            // Start Reduce only after all maps are done
            if task == Task::Reduce && mapdone_num != 7 {
                return Ok(Payload {
                    task: Task::Wait,
                    file: PathBuf::from("/dev/null"),
                });
            }
            // All tasks done -> Exit
            if files.is_empty() {
                return Ok(Payload {
                    task: Task::Exit,
                    file: PathBuf::from("/dev/null"),
                });
            }
        }
        let mut files = match task {
            Task::Map => self.mapfiles.write().unwrap(),
            Task::Reduce => self.reducefiles.write().unwrap(),
            _ => panic!("Wrong task"),
        };

        match task {
            Task::Map => match files.pop_back() {
                Some(val) => {
                    thread::spawn(|| self.check_task(task.clone(), val.clone()));
                    return Ok(Payload {
                        task: task,
                        file: PathBuf::from(val),
                    });
                }
                None => {
                    return Err(error::Error {
                        code: error::ErrorCode::InternalError,
                        message: "Something went wrong when returning filename".to_owned(),
                        data: None,
                    })
                }
            },
            Task::Reduce => match files.pop_back() {
                Some(val) => {
                    thread::spawn(|| self.check_task(task.clone(), val.clone()));
                    return Ok(Payload {
                        task: task,
                        file: PathBuf::from(val),
                    });
                }
                None => {
                    return Err(error::Error {
                        code: error::ErrorCode::InternalError,
                        message: "Something went wrong when returning filename".to_owned(),
                        data: None,
                    });
                }
            },
        }
    }
}

/// Start Master -> Returns a Handle to the server to main which will control the server's exit
pub fn start_server() -> CloseHandle {
    let rpc_impl = RwLock::new(RpcImpl::new());
    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(rpc_impl.get_mut().unwrap().to_delegate());

    let server = ServerBuilder::new(io)
        .threads(1)
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .unwrap();
    let close_handle = server.close_handle();

    thread::spawn(move || {
        server.wait();
    });
    return close_handle;
}

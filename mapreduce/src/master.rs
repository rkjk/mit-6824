use jsonrpc_core::types::error;
use jsonrpc_core::Result;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{CloseHandle, Server, ServerBuilder};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::{Payload, Task};

static FAIL_TIME: Duration = Duration::from_millis(5000);

#[rpc]
pub trait Rpc {
    /// Pop File name if available and return else signal slave to exit
    #[rpc(name = "return_task")]
    fn return_task(&self) -> Result<String>;
}

pub struct Master {
    mapfiles: RwLock<VecDeque<String>>,
    reducefiles: RwLock<VecDeque<String>>,
    mapdone: RwLock<u32>,
    reducedone: RwLock<u32>,
    numfiles: u32,
}

/// Use a Read-Write Lock for thread-safe Interior Mutability
/// The RpcImpl trait does not allow for &mut.
pub struct RpcImpl {
    master: Arc<Master>,
}

impl RpcImpl {
    /// Files to return to Slave
    fn new() -> Self {
        RpcImpl {
            master: Arc::new(Master {
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
                numfiles: 8,
            }),
        }
    }

    fn serialize(&self, payload: &Payload) -> String {
        let serial = serde_json::to_string(payload).unwrap();
        serial
    }
}

/// Wait for FAIL_TIME and then check if task is complete.
/// If Map task is complete, add the task to reduce queue and increase mapdone by 1.
/// If Reduce task is complete, increase reducedone by 1
///
/// TODO: Check if Task is complete
fn check_task(master: Arc<Master>, task: Task, filename: String) {
    thread::sleep(FAIL_TIME);
    match task {
        Task::Map => {
            // Check if Map task is completed (file exists in /dev/shm)
            *master.mapdone.write().unwrap() += 1;
            master
                .reducefiles
                .write()
                .unwrap()
                .push_front(filename.to_owned());
        }
        Task::Reduce => {
            // check if reduce task is completed (file exists is ../results/)
            *master.reducedone.write().unwrap() += 1;
        }
        _ => {
            panic!("Unknown state");
        }
    }
}

impl Rpc for RpcImpl {
    // Check if task is finished
    /// RPC handler function that will return tasks to the worker
    ///
    /// Logic: mapdone checks that all the Map tasks have been completed. When it reaches numfiles + 1,
    /// master will switch to providing workers with Reduce tasks. If master has to provide a Map task
    /// but mapfiles is empty (meaning all the tasks have been doled out), the master will signal the worker to wait.
    /// If the Master detects an error, then the check_task function will add the task back to the VecDeque in another thread.
    fn return_task(&self) -> Result<String> {
        let mapdone_num = self.master.mapdone.read().unwrap().clone();
        let reducedone_num = self.master.reducedone.read().unwrap().clone();

        let threshold = self.master.numfiles;
        //println!("mapdone_num: {}", mapdone_num);
        //println!("reducedone_num: {}", reducedone_num);
        //println!("Mapfiles {:?}", self.master.mapfiles.read().unwrap());
        //println!("Reducefiles {:?}", self.master.reducefiles.read().unwrap());
        let task = match mapdone_num == threshold {
            true => Task::Reduce,
            false => Task::Map,
        };
        //println!("task: {:?}", task);
        {
            if reducedone_num == threshold {
                return Ok(self.serialize(&Payload {
                    task: Task::Exit,
                    file: PathBuf::from("/dev/null"),
                }));
            }
            let files = match task {
                Task::Map => self.master.mapfiles.read().unwrap(),
                Task::Reduce => self.master.reducefiles.read().unwrap(),
                _ => panic!("No matching task"),
            };
            //println!("files: {:?}", files);
            // Start Reduce only after all maps are done
            if (task == Task::Reduce && mapdone_num != threshold)
                || task == Task::Reduce && files.is_empty()
            {
                return Ok(self.serialize(&Payload {
                    task: Task::Wait,
                    file: PathBuf::from("/dev/null"),
                }));
            }
            // All tasks done -> Exit
        }
        let mut files = match task {
            Task::Map => self.master.mapfiles.write().unwrap(),
            Task::Reduce => self.master.reducefiles.write().unwrap(),
            _ => panic!("Wrong task"),
        };

        match task {
            Task::Map => match files.pop_back() {
                Some(val) => {
                    let master_clone = Arc::clone(&self.master);
                    let task_clone = task.clone();
                    let val_clone = val.clone();
                    thread::spawn(|| check_task(master_clone, task_clone, val_clone));
                    return Ok(self.serialize(&Payload {
                        task: Task::Map,
                        file: PathBuf::from(val.to_owned()),
                    }));
                }
                None => {
                    return Ok(self.serialize(&Payload {
                        task: Task::Wait,
                        file: PathBuf::from("/dev/null"),
                    }));
                }
            },
            Task::Reduce => match files.pop_back() {
                Some(val) => {
                    let master_clone = Arc::clone(&self.master);
                    let task_clone = task.clone();
                    let val_clone = val.clone();
                    thread::spawn(|| check_task(master_clone, task_clone, val_clone));
                    return Ok(self.serialize(&Payload {
                        task: Task::Reduce,
                        file: PathBuf::from(val.to_owned()),
                    }));
                }
                None => {
                    //return Err(error::Error {
                    //    code: error::ErrorCode::InternalError,
                    //    message: "Something went wrong when returning filename".to_owned(),
                    //    data: None,
                    //});
                    return Ok(self.serialize(&Payload {
                        task: Task::Wait,
                        file: PathBuf::from("/dev/null"),
                    }));
                }
            },
            _ => panic!("Wrong task"),
        }
    }
}

/// Start Master -> Returns a Handle to the server to main which will control the server's exit
pub fn start_server() -> CloseHandle {
    let rpc_impl = RpcImpl::new();
    let mut io = jsonrpc_core::IoHandler::new();
    io.extend_with(rpc_impl.to_delegate());

    let server = ServerBuilder::new(io)
        .threads(1)
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .unwrap();
    let close_handle = server.close_handle();
    println!("Master Address: {:?}", server.address());

    thread::spawn(move || {
        server.wait();
    });
    return close_handle;
}

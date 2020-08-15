# Mapreduce implementation in Rust  
To run Word Count and Sequential Mapreduce tests, run  
`cargo test -- --nocapture`  

To run parallel mapreduce,  
`cargo run`  

TODO: The Master does not do any error handling. It just waits for 5 seconds and then assumes the worker succeeded. If there is a runtime error, consider increasing the wait time. 


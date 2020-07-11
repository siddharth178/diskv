use std::sync::Arc;
use std::thread;

mod diskv;

fn rwthread(name: String, cnt: u32, dkv: Arc<diskv::Diskv>) -> thread::JoinHandle<()> {
    let th = thread::spawn(move || {
        let mut keys: Vec<String> = Vec::new();
        for i in 0..cnt {
            keys.push(format!("k{}", i));
        }

        println!("writing keys in {}", name);
        for key in &keys {
            println!("[{} put] key: {}", name, key.to_string());
            let val = format!("value of key {}", key);
            dkv.put(key.to_string(), val.into_bytes())
                .expect("failed to put");
        }

        println!("reading keys in {}", name);
        for key in &keys {
            match dkv.get(key.to_string()).expect("failed to get") {
                Some(v) => println!(
                    "[{} get] key: {}, val: {}",
                    name,
                    key.to_string(),
                    String::from_utf8_lossy(&v)
                ),
                None => println!("key: {}, val: not found", key.to_string()),
            }
        }

        println!("deleting keys in {}", name);
        for key in &keys {
            println!("[{} delete] key: {}", name, key.to_string());
            dkv.delete(key.to_string()).expect("failed to delete");
        }
    });
    th
}

fn main() {
    let dkv = Arc::new(
        diskv::Diskv::new(diskv::Options {
            base_path: String::from("data"),
        })
        .expect("failed to create diskv"),
    );

    let th1 = rwthread(String::from("worker1"), 10, Arc::clone(&dkv));
    let th2 = rwthread(String::from("worker2"), 10, Arc::clone(&dkv));

    match th1.join() {
        Ok(_) => println!("th1 finished."),
        Err(e) => eprintln!("failed to join on th1, {:?}", e),
    }
    match th2.join() {
        Ok(_) => println!("th2 finished"),
        Err(e) => eprintln!("failed to join on th1, {:?}", e),
    }
}

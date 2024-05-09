use std::{
    io::{BufRead, BufReader, Write}, sync::{Arc, RwLock}, time::Duration
};

use rhai::{Engine, Scope};

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[derive(Default, Debug, Clone, Copy)]
struct MyStruct {
    val: i32,
}

impl MyStruct {
    fn foo(&self) {
        println!("val read: {:p}: {:?}", self, self)
    }
    fn bar(&mut self) {
        *self = MyStruct { val: 5 };
        println!("val changed {:p}: {:?}", &(*self), self);
    }
}

fn read_only() {
    let obj = MyStruct::default();
    for _ in 0..10 {
        tokio::spawn(async move {
            // count is cloned for each thread
            println!("{:p}: {:?}", &obj, obj);
        });
    }
}

async fn read_only_arc() {
    let obj = Arc::new(MyStruct::default());
    for _ in 0..10 {
        let obj = Arc::clone(&obj);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            // same underlying memory is used
            println!("{:p}: {:?}", &(*obj), obj);
        });
    }
}

async fn read_write_arc() {
    let obj = Arc::new(RwLock::new(MyStruct::default()));
    for _ in 0..10 {
        let obj = Arc::clone(&obj);
        tokio::spawn(async move {
            // thread-safe read
            if let Ok(guard) = obj.read() {
                guard.foo();
            }

            // atomic write
            if let Ok(mut guard) = obj.write() {
                guard.bar();
            }
        });
    }
}

fn run_script() {
    let mut engine = Engine::new();

    let script = String::from("fn perform_op(x, y) { op(x, y) }");

    let ast = engine.compile(script).unwrap();
    engine.register_fn("op", |a: i32, b: i32| a + b);

    let mut scope = Scope::new();
    let result: i32 = engine
        .call_fn(&mut scope, &ast, "perform_op", (41, 2))
        .unwrap();

    println!("Result: {}", result);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let args: Vec<String> = std::env::args().collect();
    // println!("Reducer:\tArgs: {:?}", args);
    //
    // let to_run: i32 = args[1].parse().unwrap();
    //
    // match to_run {
    //     1 => read_only(),
    //     2 => read_only_arc().await,
    //     3 => read_write_arc().await,
    //     _ => eprintln!("Unimplemented"),
    // }
    //
    // tokio::time::sleep(Duration::from_secs(1)).await;

    // run_script();

    // let mut file = std::fs::OpenOptions::new().create(true).write(true).append(true).open("data/out/map/some.txt").unwrap();
    // file.write("some".as_bytes()).unwrap();


    let reduce_func = String::from(r#"
        fn reduce(k, vals) { 
            for v in vals {
                let n: i32 = v.parse().unwrap();
                total = total + n;
            };
            emit(k, total);
        }
        "#);


    let mut engine = Engine::new();
    let ast = engine.compile(reduce_func).unwrap();
    engine.register_fn("emit", |key: String, value: String, out_file: String| {
        fn open_file(key: String, out_file: String) -> std::fs::File {
            let path = format!("{}.kv", out_file);
            std::fs::OpenOptions::new().create(true).write(true).append(true).open(path).unwrap()
        }
        let mut file = open_file(key.clone(), out_file.clone());
        let out = format!("{},{}\n", key.clone(), value.clone());
        file.write(out.as_bytes()).unwrap();
    });


    Ok(())
}

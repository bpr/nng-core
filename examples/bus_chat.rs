//! BUS0 chat room — many-to-many broadcast with dynamic membership.
//!
//! One hub node listens; any number of participant nodes dial in.  Each
//! participant sends a message every second and prints every message it
//! receives, including its own echoes from the hub.  New participants can
//! join at any time; the hub picks them up via `accept_pending()`.
//!
//! Start order:
//!   cargo run --example bus_chat -- hub
//!   cargo run --example bus_chat -- alice    # any number of participants
//!   cargo run --example bus_chat -- bob
//!   cargo run --example bus_chat -- carol    # can join late

use nng_core::{Message, socket::bus0::Bus0};
use std::{env, time::Duration};

const HUB_ADDR: &str = "tcp://127.0.0.1:11006";

#[tokio::main]
async fn main() {
    let name = env::args().nth(1).unwrap_or_else(|| "hub".to_string());

    if name == "hub" {
        run_hub().await;
    } else {
        run_participant(&name).await;
    }
}

async fn run_hub() {
    println!("[hub] listening on {HUB_ADDR}");

    // Accept the first participant before starting the relay loop.
    let mut hub = Bus0::listen(HUB_ADDR).await.expect("listen failed");
    println!("[hub] first participant connected");

    let mut seq = 0u32;
    loop {
        // Pick up any new participants that connected since last iteration.
        hub.accept_pending().await;

        // Relay: receive one message from any participant and re-broadcast it
        // so all other participants see it.  (In a real chat server you'd use
        // concurrent recv + broadcast; here we keep it simple and sequential.)
        match tokio::time::timeout(Duration::from_millis(100), hub.recv_any()).await {
            Ok(Ok(msg)) => {
                let text = String::from_utf8_lossy(msg.body()).into_owned();
                println!("[hub] relaying: {text}");
                let mut out = Message::new();
                out.push_back(text.as_bytes());
                hub.broadcast(out).await.ok();
            }
            Ok(Err(e)) => {
                println!("[hub] error: {e}");
                break;
            }
            Err(_) => {} // timeout — no message this tick, continue
        }

        seq += 1;
        if seq % 50 == 0 {
            println!("[hub] {} ticks, {} participants", seq, hub.peer_count());
        }
    }
}

async fn run_participant(name: &str) {
    let mut node = Bus0::dial(HUB_ADDR).await.expect("dial failed");
    println!("[{name}] connected to hub at {HUB_ADDR}");

    let mut counter = 1u32;
    loop {
        // Send a message to the hub.
        let payload = format!("{name}: hello #{counter}");
        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());
        if node.broadcast(msg).await.is_err() {
            println!("[{name}] hub disconnected");
            break;
        }
        counter += 1;

        // Receive any messages the hub relayed back (from other participants).
        loop {
            match tokio::time::timeout(Duration::from_millis(50), node.recv_any()).await {
                Ok(Ok(msg)) => {
                    let text = String::from_utf8_lossy(msg.body());
                    println!("[{name}] received: {text}");
                }
                Ok(Err(e)) => {
                    println!("[{name}] done: {e}");
                    return;
                }
                Err(_) => break, // no more messages buffered right now
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

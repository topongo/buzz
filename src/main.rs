use std::{io::ErrorKind, path::PathBuf, sync::Arc, time::Duration};
use buzz::{Client, Config, ConfigError, Operation, Storage};
use serde_json::Value;
use tokio::{io::{AsyncReadExt, AsyncWriteExt, BufWriter}, net::UnixListener, signal::unix::{signal, SignalKind}, sync::{broadcast, Mutex}};

use log::{debug, error, info, warn};

use clap::{Parser};

#[derive(Parser, Debug)]
struct Command {
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    subcomm: Subcommand,
}

#[derive(Debug,clap::Subcommand)]
enum Subcommand {
    Listen {
    },
    Send {
        #[clap(trailing_var_arg=true)]
        data: Vec<String>,
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();


    // let expr = Expression::And(
    //     Box::new(Expression::Variable("a".to_string())),
    //     Box::new(Expression::Or(
    //         Box::new(Expression::Variable("b".to_string())),
    //         Box::new(Expression::Static(Value::Bool(true))),
    //     ))
    // );

    let comm = Command::parse();

    let config = match Config::from_default_path() {
        Ok(c) => c,
        Err(ConfigError::ParseError(e)) => {
            error!("error parsing config file: {}: {e}", p.display());
            return
        }
        Err(ConfigError::NotFound) => {
            error!("config file not found");
            return
        }
    };

    match comm.subcomm {
        Subcommand::Listen {} => listen(config).await,
        Subcommand::Send { data } => send(config, data).await,
    }
}

async fn listen(config: Config) {
    let Config { socket_path, .. } = config;

    let (tx_s, mut rx_s) = broadcast::channel(100);
    tokio::spawn(signal_handler(tx_s));

    info!("opening socket {}...", socket_path.display());
    let sock = match UnixListener::bind(&socket_path) {
        Ok(sock) => {
            info!("socket opened successfully");
            sock
        }
        Err(e) => match e.kind() {
            ErrorKind::AddrInUse => {
                info!("socket already exists, trying to remove it...");
                if std::fs::remove_file(&socket_path).is_ok() {
                    info!("socket removed successfully, trying to bind again...");
                    match UnixListener::bind(&socket_path) {
                        Ok(sock) => {
                            info!("socket opened successfully after removal");
                            sock
                        }
                        Err(e) => {
                            error!("failed to open socket after removal: {}", e);
                            return;
                        }
                    }
                } else {
                    error!("failed to remove existing socket");
                    return;
                }
            }
            _ => {
                error!("failed to open socket: {}", e);
                return;
            }
        }
    };

    let conn_count = Mutex::new(0);

    let storage = Arc::new(Mutex::new(Storage::default()));

    async fn conn_increment(count: &Mutex<u16>) -> u16 {
        let mut lock = count.lock().await;
        let v = *lock;
        *lock += 1;
        v
    }

    loop {
        tokio::select! {
            res = sock.accept() => match res {
                Ok((stream, _addr)) => {
                    debug!("new connection established, spawning new task");

                    let id = conn_increment(&conn_count).await;
                    let s = storage.clone();
                    // subscribe to signal handler
                    let sb = rx_s.resubscribe();

                    tokio::spawn(handler(id, stream, s, sb));
                }
                Err(e) => error!("error accepting connection: {}", e),
            },
            _ = rx_s.recv() => {
                // "graceful" shutdown
                warn!("received interrupt, gracefully shutdown");
                tokio::time::sleep(Duration::from_secs(2)).await;
                return;
            }
        }
    }
}

async fn handler(id: u16, mut stream: tokio::net::UnixStream, data: Arc<Mutex<Storage>>, mut signaling: broadcast::Receiver<()>) {
    debug!("handler started for connection {}", id);
    let mut output = BufWriter::new(Vec::new());
    let mut buffer = [0; 1 << 10];
    match stream.read(&mut buffer).await {
        Ok(0) => {
            info!("connection {} closed by peer", id);
        }
        Ok(n) => {
            debug!("connection {} read {} bytes: {:2x?}", id, n, &buffer[..n]);
            if let Err(e) = output.write_all(&buffer[..n]).await {
                error!("error writing to output for connection {}: {}", id, e);
                return;
            }
        }
        Err(e) => {
            error!("error reading from stream for connection {}: {}", id, e);
        }
    }

    output.flush().await.expect("failed to flush output");

    let output = output.into_inner();
    debug!("connection {} output length: {}", id, output.len());

    let output = match String::from_utf8(output) {
        Ok(output) => {
            output.trim().to_string()
        }
        Err(e) => {
            error!("error converting output to string for connection {}: {}", id, e);
            return;
        }
    };

    info!("connection {} received command: {}", id, output);

    let args = output.split('\0').collect::<Vec<_>>();
    if args.is_empty() {
        error!("no command received from connection {}", id);
        return;
    }

    match args[0] {
        "set" => {
            if args.len() < 3 {
                error!("set command requires at least 2 arguments, received {}", args.len() - 1);
                return;
            }
            let key = args[1];
            let value = args[2];
            let mut storage = data.lock().await;
            let des: Value = match serde_json::from_str(value) {
                Ok(v) => v,
                Err(e) => {
                    error!("error parsing value '{}' for key '{}' in connection {}: {}", value, key, id, e);
                    return;
                }
            };
            storage.insert(key.to_string(), des);
            info!("set command resolved: {key} => {value}");
        }
        "get" => {
            if args.len() < 2 {
                error!("get command requires at least 1 argument, received {}", args.len() - 1);
                return;
            }

            let key = args[1];
            let storage = data.lock().await;
            let value = storage.get(key);
            let serialized = serde_json::to_string(&value).unwrap();
            if let Err(e) = stream.write_all(serialized.as_bytes()).await {
                error!("error writing response for connection {}: {}", id, e);
            }
            info!("get command resolved: {serialized}");
        }
        "listen" => {
            if args.len() < 2 {
                error!("listen command requires at least 1 argument, received {}", args.len() - 1);
                return;
            }

            let key = args[1];

            let mut storage = data.lock().await;
            let mut rx = if key == "$" {
                debug!("requested expression listener");
                if args.len() < 5 {
                    error!("listen command with '$' requires at least 4 arguments, received {}", args.len() - 1);
                    return;
                } else {
                    let op1 = args[2];
                    let operation = match Operation::parse(args[3]) {
                        Some(op) => op,
                        None => {
                            error!("unknown operation '{}' in connection {}", args[3], id);
                            return;
                        }
                    };
                    let op2 = args[4];

                    let mut v1 = storage.get(op1).clone();
                    let mut v2 = storage.get(op2).clone();
                    let mut rx1 = storage.listen(op1.to_string()).await;
                    let mut rx2 = storage.listen(op2.to_string()).await;

                    let (tx, rx) = broadcast::channel::<Value>(100);
                    tokio::spawn(async move {
                        // send initial value
                        let initial = operation.apply(v1.clone(), v2.clone());
                        debug!("sending initial value: {initial}");
                        if let Err(e) = tx.send(initial) {
                            error!("while sending initial value to listener: {e}");
                        }
                        // then wait for events:
                        loop {
                            tokio::select! {
                                val = rx1.recv() => {
                                    match val {
                                        Ok(val) => v1 = val.clone(),
                                        Err(e) => {
                                            error!("error receiving value for expression listener in connection {}: {}", id, e);
                                            break;
                                        }
                                    }
                                }
                                val = rx2.recv() => {
                                    match val {
                                        Ok(val) => v2 = val.clone(),
                                        Err(e) => {
                                            error!("error receiving value for expression listener in connection {}: {}", id, e);
                                            break;
                                        }
                                    }
                                }
                            };
                            if let Err(e) = tx.send(operation.clone().apply(v1.clone(), v2.clone())) {
                                error!("error sending value for expression listener in connection {}: {}", id, e);
                                break;
                            }
                        }
                    });
                    debug!("created joint listener for expression '{} {} {}' in connection {}", op1, operation, op2, id);
                    rx
                }
            } else {
                debug!("requested plain listener");
                let initial = storage.get(key).clone();
                debug!("sending initial value: {initial}");
                let serialized = format!("{}\n", serde_json::to_string(&initial).unwrap());
                if let Err(e) = stream.write_all(serialized.as_bytes()).await {
                    error!("while sending initial value: {e}");
                    return;
                }
                storage.listen(key.to_string()).await
            };
            drop(storage);

            loop {
                tokio::select! {
                    // receive from change listener
                    res = rx.recv() => match res {
                        Ok(value) => {
                            let serialized = format!("{}\n", serde_json::to_string(&value).unwrap());
                            debug!("notifying connection {id} of change");
                            if let Err(e) = stream.write_all(serialized.as_bytes()).await {
                                error!("error writing response for connection {}: {}", id, e);
                                return;
                            }
                        }
                        Err(e) => {
                            error!("while listening to events: {e}");
                            return;
                        }
                    },
                    // receive from client (for closed connections, other is ignored)
                    s = stream.read_u8() => if let Err(e) = s && e.kind() == ErrorKind::UnexpectedEof {
                        warn!("peer closed connection {id}");
                        return;
                    },
                    // listen for os signals
                    _ = signaling.recv() => {
                        warn!("connection {id}: terminating connection");
                        if let Err(e) = stream.shutdown().await {
                            error!("connection {id}: while shutting down connection: {e}");
                            return;
                        } else {
                            info!("connection {id}: connection closed");
                            return;
                        }
                    }
                }
            }
        }
        _ => {
            error!("unknown command '{}' received from connection {}", args[0], id);
            return;
        }
    }

    debug!("connection {id} transaction ended successfully");
    warn!("closing connection {id}");
    if let Err(e) = stream.shutdown().await {
        error!("while shutting down connection {id}: {e}");
    }
    debug!("connection {id} closed")
}

async fn send(config: Config, data: Vec<String>) {
    debug!("connecting to socket...");
    let mut client = match Client::new_from_config(config).await {
        Ok(c) => {
            info!("connected to socket successfully");
            c
        }
        Err(e) => {
            error!("could not connect to socket: {e}");
            return;
        },
    };

    // create SIGINT channel
    let (tx, mut rx) = broadcast::channel(100);
    tokio::spawn(signal_handler(tx));

    client.send_sequence(data).await;
    let mut handle = client.stream().await;
    let mut stdout = tokio::io::stdout();

    loop {
        tokio::select! {
            v = handle.recv_raw() => match v {
                Ok(v) => {
                    if let Err(e) = stdout.write_all(v.as_bytes()).await {
                        error!("while writing to stdout: {e}");
                        return;
                    }
                    if let Err(e) = stdout.flush().await {
                        error!("while flushing stdout: {e}");
                        return;
                    }
                },
                Err(e) => {
                    if matches!(e, broadcast::error::RecvError::Closed) {
                        handle.shutdown().await;
                        return;
                    } else {
                        error!("while receiving from data socket: {:?}", e);
                    }
                },
            },
            _ = rx.recv() => {
                handle.shutdown().await;
                return;
            }
        }
    }
}

async fn signal_handler(tx: broadcast::Sender<()>) {
    let mut sigint = signal(SignalKind::interrupt()).expect("could not setup sigint trap");
    let mut sigterm = signal(SignalKind::terminate()).expect("could not setup sigterm trap");

    if let Err(e) = tokio::select! {
        _ = sigint.recv() => tx.send(()),
        _ = sigterm.recv() => tx.send(()),
    } {
        error!("while listening for process signaling: {e}");
    }
}

use std::{collections::HashMap, fmt::Display, fs::read_to_string, io::ErrorKind, path::PathBuf, str::FromStr};
use serde_json::{Number, Value};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::UnixStream, sync::broadcast, task::JoinHandle};

use log::{debug, error, info, warn};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
enum Expression {
    Variable(String),
    Static(Value),
    Not(Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Equals(Box<Expression>, Box<Expression>),
}

impl Expression {
    #[allow(dead_code)]
    fn get_listeners(&self) -> Vec<String> {
        match self {
            Expression::And(left, right) |
            Expression::Equals(left, right) |
            Expression::Or(left, right) => left.get_listeners()
                .into_iter()
                .chain(right.get_listeners())
                .collect(),
            Expression::Not(expr) => expr.get_listeners(),
            Expression::Static(_) => vec![],
            Expression::Variable(var) => vec![var.clone()],
        }
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
enum Reaction {
    Command(Vec<String>),
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Listener {
    /// The socket path to send updates to
    condition: Expression,
    /// Wherer to fire the event on every set or only on change
    #[serde(default)]
    sensible: bool,
    /// Reaction to perform on condition true
    #[serde(default)]
    operation: Option<Reaction>,
    /// Reaction to perform on condition false
    #[serde(default)]
    operation_false: Option<Reaction>,
}

fn default_socket_path() -> PathBuf { PathBuf::from("config.toml") }

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
    #[allow(dead_code)]
    listeners: Option<Vec<Listener>>,
}

pub enum ConfigError {
    NotFound,
    ParseError(toml::de::Error),
}

impl Config {
    pub fn from_default_path() -> Result<Self, ConfigError> {
        let p = PathBuf::from(directories::BaseDirs::new().unwrap().config_dir()).join("buzz/config.toml");
        Self::from_path(p)
    }

    pub fn from_path(path: PathBuf) -> Result<Self, ConfigError> {
        if !path.exists() {
            Err(ConfigError::NotFound)
        } else {
            // read to string
            let raw = read_to_string(&path).expect("error while reading config file");
            match toml::from_str(&raw) {
                Ok(c) => Ok(c),
                Err(e) => {
                    error!("error parsing config file: {}: {e}", path.display());
                    Err(ConfigError::ParseError(e))
                }
            }
        }
    }
}

#[derive(Default)]
pub struct Storage {
    data: HashMap<String, Value>,
    listeners: HashMap<String, broadcast::Sender<Value>>,
}

impl Storage {
    pub fn insert(&mut self, key: String, value: Value) {
        let prev = self.data.get(&key).cloned().unwrap_or(Value::Null);
        self.data.insert(key.clone(), value.clone());
        self.listeners
            .get(&key)
            .map(|tx| {
                debug!("notifying listeners for key '{}'", key);
                if prev != value {
                    if tx.send(value.clone()).is_err() {
                        error!("failed to send value to listeners for key '{}'", key);
                    }
                }
            });
    }

    pub fn get(&self, key: &str) -> &Value {
        self.data.get(key).unwrap_or(&Value::Null)
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, key: &str) {
        self.data.remove(key);
    }

    pub async fn listen(&mut self, key: String) -> broadcast::Receiver<Value> {
        if self.listeners.contains_key(&key) {
            debug!("listener for key '{}' already exists, using that", key);
            self.listeners.get(&key).unwrap().subscribe()
        } else {
            debug!("creating new listener for key '{}'", key);
            let (tx, rx) = tokio::sync::broadcast::channel(100);
            self.listeners.insert(key.clone(), tx);
            rx
        }
    }

    #[allow(dead_code)]
    pub fn unsubscribe(&mut self, key: &str) {
        let list = self.listeners.get(key).unwrap();
        info!("current subscribers for value {key}: {}", list.receiver_count());

        self.listeners.remove(key);
    }

    // fn eval(&self, expr: &Expression) -> bool {
    //     match expr {
    //         Expression::Equals(left, right) => {
    //             let left_value = self.eval(left);
    //             let right_value = self.eval(right);
    //         }
    //     }
    // }
}

#[derive(Clone,Copy)]
pub enum Operation {
    // Add,
    // Subtract,
    // Multiply,
    // Divide,
    Or,
    And,
}

impl Operation {
    pub fn parse(op: &str) -> Option<Self> {
        Some(match op {
            // "+" => Operation::Add,
            // "-" => Operation::Subtract,
            // "*" => Operation::Multiply,
            // "/" => Operation::Divide,
            "or" => Operation::Or,
            "and" => Operation::And,
            _ => return None,
        })
    }

    pub fn apply(self, op1: Value, op2: Value) -> Value {
        let op1 = match op1 {
            Value::Number(n) => Value::Number(n),
            Value::Bool(b) => Value::Number(Number::from_f64(if b { 1.0 } else { 0.0 }).unwrap()),
            _ => Value::Null,
        };
        let op2 = match op2 {
            Value::Number(n) => Value::Number(n),
            Value::Bool(b) => Value::Number(Number::from_f64(if b { 1.0 } else { 0.0 }).unwrap()),
            _ => Value::Null,
        };

        match self {
            Operation::And => {
                if let (Value::Number(n1), Value::Number(n2)) = (op1, op2) {
                    Value::Bool(n1.as_f64().unwrap_or(0.0) != 0.0 && n2.as_f64().unwrap_or(0.0) != 0.0)
                } else {
                    Value::Null
                }
            }
            Operation::Or => {
                match (op1, op2) {
                    (Value::Number(n1), _) if n1.as_f64().unwrap_or(0.0) != 0.0 => Value::Bool(true),
                    (_, Value::Number(n2)) if n2.as_f64().unwrap_or(0.0) != 0.0 => Value::Bool(true),
                    _ => Value::Bool(false),
                }
            }
        }
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Operation::Add => write!(f, "+"),
            // Operation::Subtract => write!(f, "-"),
            // Operation::Multiply => write!(f, "*"),
            // Operation::Divide => write!(f, "/"),
            Operation::Or => write!(f, "or"),
            Operation::And => write!(f, "and"),
        }
    }
}

#[derive(Debug)]
pub struct Client {
    socket: UnixStream,
}

impl Client {
    pub fn new(socket: UnixStream) -> Self {
        Self { socket }
    }

    pub async fn new_from_config(config: Config) -> Result<Self, std::io::Error> {
        UnixStream::connect(config.socket_path).await.map(|socket| Self { socket })
    }

    pub async fn write(&mut self, data: &[u8]) {
        self.socket.write_all(data).await.expect("could not write to socket");
    }

    pub async fn send_sequence(&mut self, data: Vec<String>) {
        self.write(data.join("\0").as_bytes()).await;
    }

    pub async fn stream(mut self) -> ClientStreamHandle {
        let (tx_data, rx_data) = broadcast::channel(1);
        let (tx_ctrl, mut rx_ctrl) = broadcast::channel(1);

        let handle = tokio::spawn(async move {
            let (sock_r, mut sock_w) = self.socket.split();
            let mut reader = BufReader::new(sock_r);
            let mut buffer = Vec::<u8>::with_capacity(1 << 10);

            loop {
                buffer.clear();
                tokio::select! {
                    res = reader.read_until(b'\n', &mut buffer) => match res {
                        Ok(s) => {
                            debug!("read {s} bytes: {:?}", &buffer[..s]);
                            if s == 0 {
                                info!("empty read from server, shutting down connection");
                                if let Err(e) = self.socket.shutdown().await {
                                    error!("while shutting down connection: {e}");
                                    return;
                                }
                                // continue;
                                return;
                            }
                            let decode = String::from_utf8(buffer[..s].to_vec()).expect("cannot decode data into UTF-8 from server");
                            tx_data.send(decode).expect("could not send value to channel");
                        },
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                            // connection closed
                            info!("connection closed");
                            return;
                        }
                        Err(e) => {
                            error!("while reading data from socket: {:?}", e.kind());
                            return;
                        }
                    },
                    _ = rx_ctrl.recv() => {
                        warn!("terminated.");
                        info!("shutting down connection");
                        if let Err(e) = sock_w.shutdown().await {
                            error!("while shutting down connection: {e}");
                        }
                        return;
                    },
                }
            }
        });

        ClientStreamHandle { handle, data_pipe: rx_data, control_pipe: tx_ctrl }
    }
}

#[derive(Debug)]
pub struct ClientStreamHandle {
    handle: JoinHandle<()>,
    data_pipe: broadcast::Receiver<String>,
    control_pipe: broadcast::Sender<()>,
}

impl ClientStreamHandle {
    pub async fn recv_raw(&mut self) -> Result<String, broadcast::error::RecvError> {
        self.data_pipe.recv().await
    }

    pub async fn recv(&mut self) -> Result<Value, broadcast::error::RecvError> {
        let raw = self.recv_raw().await?;
        Ok(Value::from_str(&raw).expect("cannot deserialized data coming from server"))
    }

    pub async fn shutdown(self) {
        if !self.handle.is_finished() {
            if let Err(e) = self.control_pipe.send(()) {
                error!("while sending shutdown control signal: {:?}", e)
            }
            self.handle.await.unwrap();
        }
    }
}

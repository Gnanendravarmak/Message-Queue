use std::{
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub topic: String,
    pub message: String,
    pub created_at: u32,
    pub updated_at: u32,
    pub ack: bool,
    pub retry_count: u32,
    pub group_name: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Queue {
    pub queue: Vec<Message>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MessageQueue {
    pub id: String,
    pub group_name: String,
    pub created_at: u32,
    pub updated_at: u32,
    pub pending: Queue,
    pub is_active: bool,
    pub is_delete: bool,
    pub is_parent: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BackUpMessage {
    pub id: String,
    #[serde(default)]
    pub topic: String,

    #[serde(default)]
    pub message: String,
    pub created_at: u32,
    pub updated_at: u32,

    #[serde(default)]
    pub ack: bool,

    #[serde(default)]
    pub retry_count: u32,
    pub group_name: String,

    #[serde(default)]
    pub pending: Queue,

    #[serde(default)]
    pub is_active: bool,

    #[serde(default)]
    pub is_delete: bool,

    #[serde(default)]
    pub is_parent: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Storage {
    pub queues: Vec<MessageQueue>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Results {
    pub queue: String,
    pub total_message: u16,
    pub topics: Topic,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Topic {
    pub pending: u16,
    pub in_process: u16,
    pub ack: u16,
}

#[derive(Clone, Serialize, Deserialize)]

pub struct Response {
    pub response: Vec<Results>,
}

impl Storage {
    pub fn new() -> Self {
        Self { queues: Vec::new() }
    }

    pub fn add_queue(&mut self, name: String) {
        let now = SystemTime::now();

        let st = MessageQueue {
            id: Uuid::new_v4().to_string(),
            group_name: name,
            created_at: now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
            updated_at: now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
            pending: Queue { queue: Vec::new() },
            is_active: true,
            is_delete: false,
            is_parent: true,
        };
        let _ = self.backup(&st);

        self.queues.push(st);
    }

    pub fn add_message(&mut self, mess: String, topic: String, group_name: String) -> String {
        let now = SystemTime::now();
        let message = Message {
            id: Uuid::new_v4().to_string(),
            topic,
            message: mess,
            created_at: now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
            updated_at: now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32,
            ack: false,
            retry_count: 0,
            group_name: String::from(&group_name),
        };

        if self
            .queues
            .iter()
            .find(|q| q.group_name == group_name)
            .is_none()
        {
            Self::add_queue(self, group_name.clone());
        }

        if let Some(queue) = self.queues.iter_mut().find(|q| q.group_name == group_name) {
            queue.pending.queue.push(message.clone());
            let _ = self.backup(&message);
            return String::from("Message added to queue");
        }

        String::from("Queue not found")
    }

    pub fn consume_message(&self, topic: String, group_name: String) -> Option<Vec<Message>> {
        if !group_name.is_empty() && !topic.is_empty() {
            if let Some(group) = self.queues.iter().find(|g| g.group_name == group_name) {
                let messages: Vec<Message> = group
                    .pending
                    .queue
                    .iter()
                    .filter(|t| t.topic == topic)
                    .cloned()
                    .collect();

                return Some(messages);
            }
        }

        if !group_name.is_empty() && topic.is_empty() {
            let messages: Vec<Message> = self
                .queues
                .iter()
                .filter(|gp| gp.group_name == group_name)
                .flat_map(|gp| gp.pending.queue.iter())
                .cloned()
                .collect();
            return Some(messages);
        }

        if group_name.is_empty() && !topic.is_empty() {
            let messages: Vec<_> = self
                .queues
                .iter()
                .flat_map(|gp| gp.pending.queue.iter())
                .filter(|t| t.topic == topic)
                .cloned()
                .collect();
            return Some(messages);
        }

        None
    }

    pub fn ack_message(&mut self, message_id: String, group_name: String, topic: String) -> bool {
        if let Some(group) = self.queues.iter_mut().find(|g| g.group_name == group_name) {
            if let Some(message) = group
                .pending
                .queue
                .iter_mut()
                .find(|m| m.id == message_id && m.topic == topic)
            {
                message.ack = true;
                let now = SystemTime::now();
                message.updated_at = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;

                return true;
            }
        }

        false
    }

    pub fn get_status(&self) -> Option<Response> {
        let results: Vec<Results> = self
            .queues
            .iter()
            .map(|queue| {
                let total_message = queue.pending.queue.len() as u16;

                let pending = queue.pending.queue.iter().filter(|m| !m.ack).count() as u16;

                let in_process = queue
                    .pending
                    .queue
                    .iter()
                    .filter(|m| m.ack && m.retry_count > 0)
                    .count() as u16;

                let ack = queue.pending.queue.iter().filter(|m| m.ack).count() as u16;

                Results {
                    queue: queue.group_name.clone(),
                    total_message,
                    topics: Topic {
                        pending,
                        in_process,
                        ack,
                    },
                }
            })
            .collect();

        if results.is_empty() {
            None
        } else {
            Some(Response { response: results })
        }
    }

    pub fn backup<T: Serialize>(&self, message: &T) -> io::Result<()> {
        std::fs::create_dir_all("./backup")?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("./backup/backup_data.dat")?;

        serde_json::to_writer(&mut file, message).map_err(io::Error::other)?;

        file.write_all(b"\n")?;
        file.flush()?;

        Ok(())
    }

    pub fn load_message(&mut self) -> io::Result<()> {
        std::fs::create_dir_all("./backup")?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("./backup/backup_data.dat")?;

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;

            if line.trim().is_empty() {
                continue;
            }

            let message: BackUpMessage = serde_json::from_str(&line).map_err(io::Error::other)?;

            let index = match self
                .queues
                .iter()
                .position(|gp| gp.group_name == message.group_name && gp.is_parent)
            {
                Some(index) => index,
                None => {
                    self.queues.push(MessageQueue {
                        id: message.id.clone(),
                        group_name: message.group_name.clone(),
                        is_active: message.is_active,
                        created_at: message.created_at,
                        updated_at: message.updated_at,
                        pending: Queue { queue: Vec::new() },
                        is_delete: message.is_delete,
                        is_parent: message.is_parent,
                    });

                    self.queues.len() - 1
                }
            };

            self.queues[index].pending.queue.push(Message {
                id: message.id,
                topic: message.topic,
                message: message.message,
                created_at: message.created_at,
                updated_at: message.updated_at,
                ack: message.ack,
                retry_count: message.retry_count,
                group_name: message.group_name,
            });
        }

        Ok(())
    }
}

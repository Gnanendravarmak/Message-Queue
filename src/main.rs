mod queue;

use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
};

fn main() -> std::io::Result<()> {
    let listners = TcpListener::bind("127.0.0.1:9090").unwrap();

    let mut my_db = queue::Storage::new();

    if let Err(e) = my_db.load_message() {
        eprintln!("Failed to load backup: {e}");
    }

    for streams in listners.incoming() {
        let stream = streams?;
        let mut reader = BufReader::new(stream);

        loop {
            let prompt = format!("\x1b[32m{}: \x1b[0m", "\nEnter command => ");

            reader.get_mut().write_all(prompt.as_bytes())?;
            let mut buff = String::new();
            let _ = reader.read_line(&mut buff);
            let command = buff.trim();

            let cmd = command.split_whitespace().collect::<Vec<&str>>();
            let operation = cmd.get(1).copied().unwrap_or("");
            let topic = cmd.get(2).copied().unwrap_or("").to_string();

            match operation {
                "publish" => {
                    let message = cmd.get(3).copied().unwrap_or("").to_string();
                    let mini_grp = cmd.get(4).copied().unwrap_or("").to_string();
                    let group_name = cmd.get(5).copied().unwrap_or("").to_string();

                    let response;
                    if !mini_grp.is_empty() && mini_grp == "--group" {
                        response = my_db.add_message(message, topic, group_name);
                    } else {
                        response = my_db.add_message(message, topic, String::new());
                    }

                    reader.get_mut().write_all(response.as_bytes())?;
                }
                "consume" => {
                    let mini_grp = cmd.get(4).copied().unwrap_or("").to_string();
                    let group_name = cmd.get(5).copied().unwrap_or("").to_string();

                    let response;
                    if !mini_grp.is_empty() && mini_grp == "--group" {
                        response = my_db.consume_message(topic, group_name);
                    } else {
                        response = my_db.consume_message(topic, String::new());
                    }

                    match response {
                        Some(res) => {
                            let json = serde_json::to_vec(&res)
                                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                            reader.get_mut().write_all(&json)?;
                        }
                        None => reader.get_mut().write_all(b"[]")?,
                    }
                }
                "ack" => {
                    let mini_grp = cmd.get(3).copied().unwrap_or("").to_string();
                    let group_name = cmd.get(4).copied().unwrap_or("").to_string();
                    let _messsage_key = cmd.get(5).copied().unwrap_or("").to_string();
                    let messsage_id = cmd.get(6).copied().unwrap_or("").to_string();

                    let response;
                    if !mini_grp.is_empty() && mini_grp == "--group" {
                        response = my_db.ack_message(messsage_id, group_name, topic);
                    } else {
                        response = my_db.ack_message(messsage_id, String::new(), topic);
                    }

                    if response {
                        reader.get_mut().write_all(b"Message acknowledged")?;
                    } else {
                        reader.get_mut().write_all(b"Message not found")?;
                    }
                }
                "status" => {
                    let res = my_db.get_status();

                    match res {
                        Some(r) => {
                            let json = serde_json::to_vec(&r)
                                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                            reader.get_mut().write_all(&json)?;
                        }
                        None => {
                            reader.get_mut().write_all(b"null")?;
                        }
                    }
                }
                "" => {}
                _ => {
                    reader
                        .get_mut()
                        .write_all(b"Invalid command. Please try again.")?;
                }
            }
        }
    }

    Ok(())
}

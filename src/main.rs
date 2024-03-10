use std::net::{SocketAddr, TcpListener, TcpStream};

fn get_stream_ip(stream: &TcpStream) -> anyhow::Result<String> {
    let ip = match stream.local_addr()? {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => addr.ip().to_string(),
    };

    Ok(ip)
}

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    println!("[redis] server started");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let ip = get_stream_ip(&stream)?;
                println!("[redis] connection established with {}", ip);
            }
            Err(err) => {
                eprintln!("[redis - error] unknown error occurred: {}", err);
            }
        }
    }

    Ok(())
}

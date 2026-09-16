//! Starts one userspace `tailscaled` for a named tailnet, walks through the
//! browser login if needed, and proves the split proxy reaches a peer on
//! that tailnet while public destinations still connect directly. The
//! machine's own Tailscale login is never touched.
//!
//! ```bash
//! cargo run --example tailnet_smoke --features tailnet -- <name> <state-dir> [peer-host:port]
//! ```
//!
//! Run it twice with different names and state directories to hold two
//! tailnets open at the same time.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use temps_agent_runtime::tailnet::{TailnetDaemon, TailnetSpec, TailnetState, TailscaleBinaries};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const LOGIN_WAIT: Duration = Duration::from_secs(60 * 60);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (Some(name), Some(state_dir)) = (args.next(), args.next()) else {
        eprintln!("usage: tailnet_smoke <name> <state-dir> [peer-host:port]");
        std::process::exit(2);
    };
    let peer_override = args.next();
    let state_dir = std::path::absolute(PathBuf::from(state_dir))?;

    let binaries = TailscaleBinaries::discover()?;
    println!("tailscaled: {}", binaries.tailscaled.display());
    let daemon = TailnetDaemon::start(TailnetSpec::new(&name, state_dir, binaries)?).await?;

    let started = Instant::now();
    let mut announced_url = None;
    let status = loop {
        let status = daemon.status().await;
        match status.state {
            TailnetState::Running => break status,
            TailnetState::NeedsLogin => {
                let status = daemon.login().await?;
                if let Some(url) = status.auth_url.as_deref() {
                    if announced_url.as_deref() != Some(url) {
                        println!("[{name}] open in a browser signed into the wanted account:");
                        println!("[{name}]   {url}");
                        announced_url = Some(url.to_string());
                    }
                }
            }
            TailnetState::Failed => {
                daemon.stop().await?;
                return Err(format!("tailscaled failed: {}", status.detail).into());
            }
            _ => {}
        }
        if started.elapsed() > LOGIN_WAIT {
            daemon.stop().await?;
            return Err("timed out waiting for login".into());
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    };
    println!(
        "[{name}] connected to {} as {} ({}), {} of {} peers online",
        status.tailnet_name.as_deref().unwrap_or("?"),
        status.self_dns_name.as_deref().unwrap_or("?"),
        status.self_ips.join(", "),
        status.online_peers,
        status.total_peers
    );

    let access = daemon.access().await?;
    println!(
        "[{name}] split proxy {}  socks5 {}",
        access.proxy_addr, access.socks_addr
    );

    let peer = match peer_override {
        Some(peer) => peer,
        None => first_online_peer(&access.tailscale, &access.socket_path).await?,
    };
    println!("[{name}] ping {} through this daemon:", peer_host(&peer));
    let ping = tokio::process::Command::new(&access.tailscale)
        .arg(format!("--socket={}", access.socket_path.display()))
        .args(["ping", "--c", "3", "--timeout", "5s", peer_host(&peer)])
        .stdin(Stdio::null())
        .output()
        .await?;
    print!("{}", String::from_utf8_lossy(&ping.stdout));
    print!("{}", String::from_utf8_lossy(&ping.stderr));

    let tailnet_line = connect(access.proxy_addr, &peer).await?;
    println!("[{name}] CONNECT {peer} via proxy -> {tailnet_line}");
    let public_line = connect(access.proxy_addr, "example.com:443").await?;
    println!("[{name}] CONNECT example.com:443 via proxy -> {public_line}");

    println!("[{name}] holding the tailnet open; press Enter (or close stdin) to stop the daemon");
    let _ = tokio::task::spawn_blocking(|| {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)
    })
    .await;
    daemon.stop().await?;
    println!("[{name}] daemon stopped");
    Ok(())
}

fn peer_host(peer: &str) -> &str {
    peer.rsplit_once(':').map_or(peer, |(host, _)| host)
}

async fn first_online_peer(
    tailscale: &std::path::Path,
    socket: &std::path::Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let output = tokio::process::Command::new(tailscale)
        .arg(format!("--socket={}", socket.display()))
        .args(["status", "--json"])
        .stdin(Stdio::null())
        .output()
        .await?;
    let status: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let peers = status["Peer"].as_object().ok_or("no peers in status")?;
    let mut candidates: Vec<(String, String)> = peers
        .values()
        .filter(|peer| peer["Online"].as_bool() == Some(true))
        .filter_map(|peer| {
            let ip = peer["TailscaleIPs"][0].as_str()?;
            let host = peer["HostName"].as_str().unwrap_or("?");
            Some((host.to_string(), ip.to_string()))
        })
        .collect();
    candidates.sort();
    println!("online peers:");
    for (host, ip) in &candidates {
        println!("  {host:<32} {ip}");
    }
    let (_, ip) = candidates.first().ok_or("no online peer to test against")?;
    Ok(format!("{ip}:22"))
}

async fn connect(proxy: std::net::SocketAddr, target: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(proxy).await?;
    stream
        .write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes())
        .await?;
    let mut buffer = vec![0u8; 512];
    let read = tokio::time::timeout(Duration::from_secs(20), stream.read(&mut buffer)).await??;
    let text = String::from_utf8_lossy(&buffer[..read]);
    Ok(text.lines().next().unwrap_or("").to_string())
}

//! Per-tailnet split proxy.
//!
//! A userspace `tailscaled` exposes a SOCKS5 proxy that only knows how to
//! reach tailnet destinations. An agent still needs to reach the public
//! internet (model APIs, package registries, git hosts), so each
//! [`TailnetDaemon`](super::TailnetDaemon) fronts its daemon with a tiny HTTP
//! `CONNECT` proxy that decides per connection:
//!
//! - tailnet destination (MagicDNS name, peer hostname, `*.ts.net`,
//!   `100.64.0.0/10`, `fd7a:115c:a1e0::/48`) is forwarded through the
//!   tailscaled SOCKS5 listener, with the name left unresolved so tailscaled
//!   resolves MagicDNS itself;
//! - any other name is resolved first; if it points at a tailnet address
//!   (a custom DNS record such as `app.internal.example.com -> 100.x.y.z`)
//!   it is forwarded through tailscaled by that address, because the
//!   embedding host has no route to the CGNAT range without a TUN device;
//! - everything else is connected directly from the embedding process.
//!
//! The proxy speaks plain `CONNECT host:port` (what every HTTPS client,
//! `nono --upstream-proxy`, and `HTTPS_PROXY` users send) plus absolute-form
//! plain HTTP requests. It never inspects or terminates TLS. It is only ever
//! bound on loopback.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;

/// Upper bound on concurrent proxied connections per tailnet. Agents fan out
/// package downloads and git fetches, but never anywhere near this.
const MAX_CONNECTIONS: usize = 256;
/// A request head larger than this is not a proxy request we want to serve.
const MAX_HEAD_BYTES: usize = 16 * 1024;

/// Which destinations count as "inside the tailnet". Rebuilt from
/// `tailscale status --json` whenever the runtime refreshes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteTable {
    /// MagicDNS suffix of the tailnet, e.g. `jerboa-altered.ts.net`.
    pub magic_dns_suffix: Option<String>,
    /// Lowercased fully-qualified peer names without the trailing dot.
    pub dns_names: Vec<String>,
    /// Lowercased short hostnames of self and peers.
    pub host_names: Vec<String>,
}

impl RouteTable {
    /// Whether `host` (a hostname or IP literal from a CONNECT target) should
    /// be routed through tailscaled instead of connected directly.
    pub fn is_tailnet_host(&self, host: &str) -> bool {
        let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() {
            return false;
        }
        let ip_literal = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = ip_literal.parse::<IpAddr>() {
            return is_tailnet_ip(ip);
        }
        if host.ends_with(".ts.net") {
            return true;
        }
        if let Some(suffix) = &self.magic_dns_suffix {
            if host == *suffix || host.ends_with(&format!(".{suffix}")) {
                return true;
            }
        }
        if self.dns_names.contains(&host) {
            return true;
        }
        if !host.contains('.') && self.host_names.contains(&host) {
            return true;
        }
        false
    }
}

/// Tailscale's CGNAT range (`100.64.0.0/10`) and IPv6 ULA prefix
/// (`fd7a:115c:a1e0::/48`).
pub fn is_tailnet_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            octets[0] == 100 && (64..=127).contains(&octets[1])
        }
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            segments[0] == 0xfd7a && segments[1] == 0x115c && segments[2] == 0xa1e0
        }
    }
}

/// Where the split proxy forwards tailnet traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocksUpstream {
    pub addr: SocketAddr,
}

/// A running split proxy. Dropping the handle stops accepting; in-flight
/// connections finish on their own.
pub struct SplitProxy {
    local_addr: SocketAddr,
    routes: Arc<RwLock<RouteTable>>,
    accept_task: JoinHandle<()>,
}

impl SplitProxy {
    /// Bind on an OS-assigned loopback port and start serving.
    pub async fn start(socks: SocksUpstream, routes: RouteTable) -> std::io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let local_addr = listener.local_addr()?;
        let routes = Arc::new(RwLock::new(routes));
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let accept_routes = routes.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    // Transient accept failures (EMFILE, ECONNABORTED) are
                    // retried after a short pause instead of ending the
                    // proxy for every future connection.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                };
                let Ok(permit) = permits.clone().acquire_owned().await else {
                    return;
                };
                let routes = accept_routes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    // A client that hangs up mid-request is not an error the
                    // daemon owner can act on.
                    let _ = serve_connection(client, socks, routes).await;
                });
            }
        });
        Ok(Self {
            local_addr,
            routes,
            accept_task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn set_routes(&self, routes: RouteTable) {
        *self.routes.write().await = routes;
    }
}

impl Drop for SplitProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

#[derive(Debug)]
enum ProxyRequest {
    Connect {
        host: String,
        port: u16,
    },
    /// Absolute-form plain HTTP request; the whole head is forwarded as-is.
    Forward {
        host: String,
        port: u16,
    },
}

async fn serve_connection(
    mut client: TcpStream,
    socks: SocksUpstream,
    routes: Arc<RwLock<RouteTable>>,
) -> std::io::Result<()> {
    let (head, leftover) = read_head(&mut client).await?;
    let request = match parse_request(&head) {
        Ok(request) => request,
        Err(reason) => {
            let body = format!("{reason}\n");
            let response = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            client.write_all(response.as_bytes()).await?;
            return Ok(());
        }
    };
    let (host, port) = match &request {
        ProxyRequest::Connect { host, port } | ProxyRequest::Forward { host, port } => {
            (host.clone(), *port)
        }
    };
    let by_name = routes.read().await.is_tailnet_host(&host);
    let route = if by_name {
        Route::Tailnet(host.clone())
    } else {
        match tokio::net::lookup_host((host.as_str(), port)).await {
            Ok(resolved) => route_for_resolved(resolved.collect()),
            // Unresolvable here; let the direct connect produce the error.
            Err(_) => Route::Direct(Vec::new()),
        }
    };
    let via_tailnet = matches!(route, Route::Tailnet(_));
    let upstream = match &route {
        Route::Tailnet(target) => connect_via_socks5(socks.addr, target, port).await,
        Route::Direct(addrs) if addrs.is_empty() => TcpStream::connect((host.as_str(), port)).await,
        Route::Direct(addrs) => TcpStream::connect(addrs.as_slice()).await,
    };
    let mut upstream = match upstream {
        Ok(stream) => stream,
        Err(error) => {
            let route = if via_tailnet { "tailnet" } else { "direct" };
            let body = format!("tailnet proxy: {route} connect to {host}:{port} failed: {error}\n");
            let response = format!(
                "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            client.write_all(response.as_bytes()).await?;
            return Ok(());
        }
    };
    match request {
        ProxyRequest::Connect { .. } => {
            client
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await?;
            if !leftover.is_empty() {
                upstream.write_all(&leftover).await?;
            }
        }
        ProxyRequest::Forward { .. } => {
            upstream.write_all(&head).await?;
            if !leftover.is_empty() {
                upstream.write_all(&leftover).await?;
            }
        }
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
    Ok(())
}

/// Where one proxied connection goes after classification.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    /// Hand this target (a name tailscaled resolves, or a tailnet IP literal)
    /// to the tailscaled SOCKS5 listener.
    Tailnet(String),
    /// Connect directly to these already-resolved addresses. Empty means the
    /// name could not be resolved here; the caller falls back to a plain
    /// by-name connect so the client sees the resolver's own error.
    Direct(Vec<SocketAddr>),
}

/// A name the route table does not know may still be a tailnet destination:
/// operators commonly publish `internal.example.com -> 100.x.y.z` in public
/// or split DNS. Without a TUN device the host cannot reach that address
/// directly, so prefer the tailnet whenever resolution yields one.
fn route_for_resolved(resolved: Vec<SocketAddr>) -> Route {
    match resolved.iter().find(|addr| is_tailnet_ip(addr.ip())) {
        Some(addr) => Route::Tailnet(addr.ip().to_string()),
        None => Route::Direct(resolved),
    }
}

/// Read up to and including the blank line that ends the request head.
/// Returns the head bytes and any bytes read past it.
async fn read_head(stream: &mut TcpStream) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before sending a request head",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_head_end(&buffer) {
            let leftover = buffer.split_off(end);
            return Ok((buffer, leftover));
        }
        if buffer.len() > MAX_HEAD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request head exceeds limit",
            ));
        }
    }
}

fn find_head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn parse_request(head: &[u8]) -> Result<ProxyRequest, &'static str> {
    let text = std::str::from_utf8(head).map_err(|_| "request head is not UTF-8")?;
    let request_line = text.lines().next().ok_or("empty request")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("missing method")?;
    let target = parts.next().ok_or("missing request target")?;
    if method.eq_ignore_ascii_case("CONNECT") {
        let (host, port) = split_host_port(target, 443).ok_or("invalid CONNECT target")?;
        return Ok(ProxyRequest::Connect { host, port });
    }
    let rest = target
        .strip_prefix("http://")
        .ok_or("only CONNECT and absolute http:// requests are supported")?;
    let authority = rest.split('/').next().unwrap_or("");
    let (host, port) = split_host_port(authority, 80).ok_or("invalid request authority")?;
    Ok(ProxyRequest::Forward { host, port })
}

/// Split `host:port`, `[v6]:port`, or a bare host (using `default_port`).
fn split_host_port(target: &str, default_port: u16) -> Option<(String, u16)> {
    let target = target.trim();
    if target.is_empty() {
        return None;
    }
    if let Some(rest) = target.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let port = match &rest[end + 1..] {
            "" => default_port,
            tail => tail.strip_prefix(':')?.parse().ok()?,
        };
        return Some((host.to_string(), port));
    }
    match target.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => Some((host.to_string(), port.parse().ok()?)),
        Some(_) | None => Some((target.to_string(), default_port)),
    }
}

/// Minimal SOCKS5 client: no auth, domain-name addressing so tailscaled
/// resolves MagicDNS names itself.
pub async fn connect_via_socks5(
    socks: SocketAddr,
    host: &str,
    port: u16,
) -> std::io::Result<TcpStream> {
    let invalid =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string());
    let mut stream = TcpStream::connect(socks).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut greeting = [0u8; 2];
    stream.read_exact(&mut greeting).await?;
    if greeting != [0x05, 0x00] {
        return Err(invalid("socks5 upstream rejected no-auth handshake"));
    }
    let mut request = Vec::with_capacity(host.len() + 7);
    request.extend_from_slice(&[0x05, 0x01, 0x00]);
    match host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    {
        Ok(IpAddr::V4(v4)) => {
            request.push(0x01);
            request.extend_from_slice(&v4.octets());
        }
        Ok(IpAddr::V6(v6)) => {
            request.push(0x04);
            request.extend_from_slice(&v6.octets());
        }
        Err(_) => {
            let length =
                u8::try_from(host.len()).map_err(|_| invalid("hostname too long for socks5"))?;
            request.push(0x03);
            request.push(length);
            request.extend_from_slice(host.as_bytes());
        }
    }
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut reply = [0u8; 4];
    stream.read_exact(&mut reply).await?;
    if reply[0] != 0x05 {
        return Err(invalid("socks5 upstream sent a non-socks5 reply"));
    }
    if reply[1] != 0x00 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!(
                "socks5 upstream refused connection (reply code {})",
                reply[1]
            ),
        ));
    }
    let bound_len = match reply[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            len[0] as usize
        }
        _ => return Err(invalid("socks5 upstream sent an unknown address type")),
    };
    let mut bound = vec![0u8; bound_len + 2];
    stream.read_exact(&mut bound).await?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RouteTable {
        RouteTable {
            magic_dns_suffix: Some("jerboa-altered.ts.net".into()),
            dns_names: vec!["stage-node-dcdn-cache-7.jerboa-altered.ts.net".into()],
            host_names: vec!["stage-node-dcdn-cache".into()],
        }
    }

    #[test]
    fn routes_magicdns_peers_and_cgnat_ips_to_tailnet() {
        let table = table();
        assert!(table.is_tailnet_host("stage-node-dcdn-cache-7.jerboa-altered.ts.net"));
        assert!(table.is_tailnet_host("Stage-Node-DCDN-Cache-7.jerboa-altered.ts.net."));
        assert!(table.is_tailnet_host("anything.jerboa-altered.ts.net"));
        assert!(table.is_tailnet_host("other.some-other.ts.net"));
        assert!(table.is_tailnet_host("stage-node-dcdn-cache"));
        assert!(table.is_tailnet_host("100.111.159.111"));
        assert!(table.is_tailnet_host("[fd7a:115c:a1e0::3401:9f70]"));
    }

    #[test]
    fn routes_public_destinations_direct() {
        let table = table();
        assert!(!table.is_tailnet_host("api.anthropic.com"));
        assert!(!table.is_tailnet_host("github.com"));
        assert!(!table.is_tailnet_host("localhost"));
        assert!(!table.is_tailnet_host("127.0.0.1"));
        assert!(!table.is_tailnet_host("100.200.1.1"));
        assert!(!table.is_tailnet_host("10.0.0.5"));
        assert!(!table.is_tailnet_host(""));
        assert!(!RouteTable::default().is_tailnet_host("stage-node-dcdn-cache"));
    }

    #[test]
    fn custom_dns_names_resolving_to_tailnet_addresses_route_via_tailnet() {
        let public: SocketAddr = "93.184.216.34:443".parse().unwrap();
        let cgnat: SocketAddr = "100.126.62.28:443".parse().unwrap();
        let ula: SocketAddr = "[fd7a:115c:a1e0::3401:9f70]:443".parse().unwrap();
        // A public name behind a Tailscale IP (split or public DNS record).
        assert_eq!(
            route_for_resolved(vec![cgnat]),
            Route::Tailnet("100.126.62.28".into())
        );
        assert_eq!(
            route_for_resolved(vec![ula]),
            Route::Tailnet("fd7a:115c:a1e0::3401:9f70".into())
        );
        // Dual answers: the tailnet address wins, since the host cannot
        // reach it any other way and the public one may be a decoy/NAT.
        assert_eq!(
            route_for_resolved(vec![public, cgnat]),
            Route::Tailnet("100.126.62.28".into())
        );
        // Ordinary public destinations keep their resolved addresses.
        assert_eq!(
            route_for_resolved(vec![public]),
            Route::Direct(vec![public])
        );
        assert_eq!(route_for_resolved(Vec::new()), Route::Direct(Vec::new()));
    }

    #[test]
    fn parses_connect_and_absolute_form_requests() {
        match parse_request(
            b"CONNECT api.example.com:443 HTTP/1.1\r\nHost: api.example.com:443\r\n\r\n",
        )
        .unwrap()
        {
            ProxyRequest::Connect { host, port } => {
                assert_eq!(host, "api.example.com");
                assert_eq!(port, 443);
            }
            other @ ProxyRequest::Forward { .. } => panic!("unexpected {other:?}"),
        }
        match parse_request(b"GET http://grafana.jerboa-altered.ts.net/api HTTP/1.1\r\n\r\n")
            .unwrap()
        {
            ProxyRequest::Forward { host, port } => {
                assert_eq!(host, "grafana.jerboa-altered.ts.net");
                assert_eq!(port, 80);
            }
            other @ ProxyRequest::Connect { .. } => panic!("unexpected {other:?}"),
        }
        assert!(parse_request(b"GET /relative HTTP/1.1\r\n\r\n").is_err());
        assert_eq!(
            split_host_port("[fd7a::1]:8443", 443),
            Some(("fd7a::1".into(), 8443))
        );
        assert_eq!(split_host_port("host", 443), Some(("host".into(), 443)));
    }

    async fn echo_server() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buffer = [0u8; 1024];
                    loop {
                        let read = socket.read(&mut buffer).await.unwrap_or(0);
                        if read == 0 {
                            break;
                        }
                        if socket.write_all(&buffer[..read]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    /// Fake SOCKS5 server that records the requested domain and then bridges
    /// to `target`.
    async fn fake_socks5(target: SocketAddr) -> (SocketAddr, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = seen.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let record = record.clone();
                tokio::spawn(async move {
                    let mut greeting = [0u8; 3];
                    socket.read_exact(&mut greeting).await.unwrap();
                    socket.write_all(&[0x05, 0x00]).await.unwrap();
                    let mut header = [0u8; 5];
                    socket.read_exact(&mut header).await.unwrap();
                    assert_eq!(header[3], 0x03, "expected domain addressing");
                    let mut name = vec![0u8; header[4] as usize];
                    socket.read_exact(&mut name).await.unwrap();
                    let mut port = [0u8; 2];
                    socket.read_exact(&mut port).await.unwrap();
                    record
                        .lock()
                        .unwrap()
                        .push(String::from_utf8(name).unwrap());
                    socket
                        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                        .await
                        .unwrap();
                    let mut upstream = TcpStream::connect(target).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                });
            }
        });
        (addr, seen)
    }

    async fn connect_through(proxy: SocketAddr, target: &str) -> TcpStream {
        let mut stream = TcpStream::connect(proxy).await.unwrap();
        stream
            .write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = [0u8; 39];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &response[..],
            b"HTTP/1.1 200 Connection Established\r\n\r\n"
        );
        stream
    }

    #[tokio::test]
    async fn connect_to_public_host_goes_direct_and_tailnet_host_goes_through_socks() {
        let echo = echo_server().await;
        let (socks, seen) = fake_socks5(echo).await;
        let proxy = SplitProxy::start(SocksUpstream { addr: socks }, table())
            .await
            .unwrap();

        // Direct: the echo server is reached by its loopback address.
        let mut direct = connect_through(proxy.local_addr(), &echo.to_string()).await;
        direct.write_all(b"ping").await.unwrap();
        let mut reply = [0u8; 4];
        direct.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"ping");
        assert!(seen.lock().unwrap().is_empty());

        // Tailnet: a MagicDNS name is handed to SOCKS unresolved.
        let mut via_tailnet =
            connect_through(proxy.local_addr(), "grafana.jerboa-altered.ts.net:443").await;
        via_tailnet.write_all(b"pong").await.unwrap();
        via_tailnet.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["grafana.jerboa-altered.ts.net".to_string()]
        );
    }

    #[tokio::test]
    async fn unreachable_tailnet_upstream_yields_502_not_a_hang() {
        let unused = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let dead = unused.local_addr().unwrap();
        drop(unused);
        let proxy = SplitProxy::start(SocksUpstream { addr: dead }, table())
            .await
            .unwrap();
        let mut stream = TcpStream::connect(proxy.local_addr()).await.unwrap();
        stream
            .write_all(b"CONNECT box.jerboa-altered.ts.net:22 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 502 Bad Gateway"),
            "{response}"
        );
        assert!(response.contains("tailnet connect to box.jerboa-altered.ts.net:22 failed"));
    }

    #[tokio::test]
    async fn malformed_request_is_rejected_with_400() {
        let proxy = SplitProxy::start(
            SocksUpstream {
                addr: "127.0.0.1:1".parse().unwrap(),
            },
            RouteTable::default(),
        )
        .await
        .unwrap();
        let mut stream = TcpStream::connect(proxy.local_addr()).await.unwrap();
        stream
            .write_all(b"GET /index.html HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400 Bad Request"),
            "{response}"
        );
    }
}

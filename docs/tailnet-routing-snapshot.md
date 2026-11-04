# Read-only tailnet routing snapshots

`TailnetDaemon::routing_snapshot().await` observes the SDK-owned cached routes.
It starts no process and performs no CLI or DNS request. `available` is false,
`routes` is empty and `socks_addr` is absent before login, after a failed status
refresh, while stopped, or after a process restart until a successful refresh
observes the new process. An opaque `generation` changes when the process is
restarted or the daemon object is replaced; do not parse it or use it as a secret.

The returned `TailnetRoutingSnapshot` owns its `RouteTable`. Changing the copy
cannot change routing in the daemon. `RouteTable::is_tailnet_host` shares the
existing split-proxy classification (including peer short names). It identifies
a route, not permission to access a peer; Tailscale ACLs still apply.

This observation is not a connection lease or a confinement boundary. A host
building container ingress must authenticate each request, enforce destination
policy, handle daemon changes and revoke its own in-flight connections. Do not
expose the existing host split proxy or control socket to a container. The
snapshot contains neither control socket paths nor credentials.

# Embedded Hysteria2 outbound

[Laz Hysteria](https://github.com/ro-gri/laz-hysteria) adds an outbound to the existing server. Run one process with two
ordinary configuration files:

```sh
hysteria server -c server.yaml --client-config client.yaml
```

In `server.yaml`, retain the server's TLS, authentication, statistics and other
settings. Add an outbound and select it using the existing ACL:

```yaml
outbounds:
  - name: exit_b
    type: hysteria2
  - name: direct
    type: direct
acl:
  rules:
    - outbound: direct
      match: geoip("ru")
    - outbound: exit_b
```

`client.yaml` uses the normal client format:

```yaml
server: exit.example.com:443
auth: REPLACE_WITH_EXIT_CREDENTIAL
lazy: true
tls:
  sni: exit.example.com
```

The client is shared by the Hysteria2 outbound and any configured client modes
(SOCKS5, HTTP, forwarding, TUN, transparent proxy). No mode is silently disabled.
Existing outbounds and ACL ordering are unchanged. Closing a stream or UDP
session does not close the shared client. No automatic DIRECT fallback is added.
With `lazy: true`, an unavailable exit does not prevent startup; connection is
attempted when needed. Without it, normal eager client initialization applies.

A domain is passed to the exit for resolution, like the existing SOCKS5
outbound; local and remote DNS answers can differ. With ACL enabled, a single
UDP session can route different destinations through different outbounds.

The standalone `hysteria client` command and servers without `--client-config`
retain their existing behavior. The new outbound requires `--client-config`.
Standalone clients check for updates after the first successful upstream
connection. Combined mode uses only the server update checker; the embedded
client check is disabled. `--disable-update-check` disables the server check.

Existing Hysteria2 client applications do not need changes to use this server.
Update checks use the stable GitHub Releases of `ro-gri/laz-hysteria`.

## UDP routing within a session

ACL rules select an outbound for each destination. Outgoing connections are
created on first use and reused within the session. DIRECT may use separate
IPv4 and IPv6 sockets when bound to specific IP addresses.

The last destination and its DNS result are cached for the session. Changing
the destination triggers resolution using the configured resolver; returning to
an earlier destination resolves it again. The cached result is not refreshed
by TTL while the destination remains unchanged. ACL rules use the identity of
the client that created the session and support destination address rewrites.

A read error is queued after earlier replies. When the server receives the
error, it closes the entire UDP session, including all its outgoing connections.
There is no automatic recovery or DIRECT fallback. A write error drops the
affected packet without terminating the session.

The client's 4096-byte UDP serialization buffer includes the protocol header
and address, so the maximum payload is smaller than 4096 bytes.

## Tests

```sh
go test -race ./app/cmd ./extras/outbounds -count=1
go test ./core/... -timeout=180s
```

To check concurrent server, SOCKS5 and HTTP traffic through a shared client:

```sh
go test -race ./app/cmd -run '^TestServerSOCKSAndHTTPShareClientConcurrently$' -count=1
```

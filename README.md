# Laz Hysteria

[![License][1]][2] [![Release][3]][4] [![Build][5]][6] [![Issues][7]][8]

[1]: https://img.shields.io/badge/license-MIT-blue
[2]: LICENSE.md
[3]: https://img.shields.io/github/v/release/ro-gri/laz-hysteria?style=flat-square
[4]: https://github.com/ro-gri/laz-hysteria/releases
[5]: https://github.com/ro-gri/laz-hysteria/actions/workflows/master.yml/badge.svg
[6]: https://github.com/ro-gri/laz-hysteria/actions/workflows/master.yml
[7]: https://img.shields.io/github/issues/ro-gri/laz-hysteria?style=flat-square
[8]: https://github.com/ro-gri/laz-hysteria/issues

Laz Hysteria is a fork of [Hysteria 2](https://github.com/apernet/hysteria)
with support for cascading proxies. The server can route traffic directly or
through another Hysteria2 server according to routing rules, without running
a separate Hysteria2 client.

It also fixes UDP routing: rules are applied to each destination within a
session, rather than only to the first destination.

See the [configuration guide](docs/hysteria2-outbound.md) for details.

The executable remains `hysteria`. Existing command names, configuration paths,
systemd service names and APIs are retained; the new outbound is opt-in.
Existing Hysteria2 clients can connect without changes.

Prebuilt releases target **Linux only**, retaining the upstream Linux architecture
variants and artifact names. Downloads and update checks use this repository's
GitHub Releases. To install using this fork's server installer:

```sh
bash <(curl -fsSL https://raw.githubusercontent.com/ro-gri/laz-hysteria/master/scripts/install_server.sh)
```

For fork-specific bugs and requests, use [our Issues][8]. General configuration
and protocol documentation remain available on the upstream documentation site.
The original MIT license and copyright notices are preserved.

## Versioning

The first fork release is `v2.0.0+hy.2.14.0`: `2.0.0` is the Laz Hysteria
version and `hy.2.14.0` records its upstream base. Release tags use
`app/v2.0.0+hy.2.14.0`. Every new release increments the fork version, including
when the upstream base changes. Build metadata does not affect update ordering.

<h2 style="text-align: center;">Hysteria is a powerful, lightning-fast, and censorship-resistant proxy.</h2>

### [Upstream documentation](https://v2.hysteria.network/)

### [中文文档](https://v2.hysteria.network/zh/)

### [Hysteria 1.x (legacy)](https://v1.hysteria.network/)

---

<div class="feature-grid">
  <div>
    <h3>🛠️ Jack of all trades</h3>
    <p>Wide range of modes including SOCKS5, HTTP Proxy, TCP/UDP Forwarding, Linux TProxy, TUN - with more features being added constantly.</p>
  </div>

  <div>
    <h3>⚡ Blazing fast</h3>
    <p>Powered by a customized QUIC protocol, Hysteria is designed to deliver unparalleled performance over unreliable and lossy networks.</p>
  </div>

  <div>
    <h3>✊ Censorship resistant</h3>
    <p>The protocol masquerades as standard HTTP/3 traffic, making it very difficult for censors to detect and block without widespread collateral damage.</p>
  </div>
  
  <div>
    <h3>💻 Linux releases</h3>
    <p>This fork publishes Linux binaries. Cross-platform source support is retained.</p>
  </div>

  <div>
    <h3>🔗 Easy integration</h3>
    <p>With built-in support for custom authentication, traffic statistics & access control, Hysteria is easy to integrate into your infrastructure.</p>
  </div>
  
  <div>
    <h3>🤗 Chill and supportive</h3>
    <p>We have well-documented specifications and code for developers to contribute and/or build their own apps. And a helpful community, too.</p>
  </div>
</div>

---

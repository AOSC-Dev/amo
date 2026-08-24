amo
===

> Run oma as D-Bus service.

amo is a project similar to PackageKit - it runs as a D-Bus/systemd service
and serves as a backend to oma.

This project currently aids in accelerating [oma](https://github.com/AOSC-Dev/oma)
functions and will serve as backend for [aoska](https://github.com/AOSC-Dev/aoska)
(application store for AOSC OS) down the line.

Features
---

- Cache for Package catalogues (`Contents`, `Packages`, etc.), enabling
  accelerated queries for package names, descriptions, contents, and
  transaction history, etc. in oma.
- Initiating metadata refresh and package installation/removal/upgrade
  via oma API.

Dependencies
---

To build amo, the following are needed:

- libapt-pkg (part of [APT](https://salsa.debian.org/apt-team/apt.git))
- [LLVM and Clang](https://llvm.org/)
- [Nettle](https://www.lysator.liu.se/~nisse/nettle/) (recommended) or [OpenSSL](https://openssl.org/)
- [Rustc](https://www.rust-lang.org/) and [Cargo](https://crates.io/)
- [pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config/) or [pkgconf](http://pkgconf.org/)

During runtime, D-Bus and systemd are required to launch it as a service.

Installation and Usage
---

1. Install build dependencies:

   ```bash
   apt install build-essential zlib1g-dev libssl-dev pkgconf nettle-dev libapt-pkg-dev curl xz-utils clang libbz2-dev liblzma-dev
   curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
   cargo install cargo-deb
   ```

2. Build the binary:

   ```bash
   cargo build --release
   ```

3. Install the binary and configurations:

   ```bash
   install -Dvm755 ./target/release/amo \
       /usr/libexec/amo
   install -Dvm644 ./data/amo.service \
       /etc/systemd/system/amo.service
   install -Dvm644 ./data/io.aosc.Amo.conf \
       /usr/share/dbus-1/system.d/io.aosc.Amo.conf
   install -Dvm644 ./data/io.aosc.amo.apply.policy \
       /usr/share/polkit-1/actions/io.aosc.amo.apply.policy
   install -Dvm644 ./data/20amo \
       /etc/apt/apt.conf.d/20amo
   ```

4. Enable and start `amo.service`:

   ```bash
   systemctl enable amo --now
   ```

## License

oma is licensed under the GNU General Public License v3.0. See the
[COPYING](./COPYING) file for details.

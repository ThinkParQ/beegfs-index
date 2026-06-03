# BeeGFS Index

BeeGFS Index provides metadata indexing for BeeGFS, built on top of the
[Grand Unified File-Index (GUFI)](https://github.com/mar-file-system/GUFI). It
indexes BeeGFS entry, stripe, and Remote Storage Target metadata into SQLite
databases that can be queried with the bundled `gufi_query` tool through the
BeeGFS index and query plugins.

Homepage: https://www.beegfs.io

Documentation: https://doc.beegfs.io/

# Getting Started with BeeGFS Index (this repo)

This directory (`beegfs_deploy`) is the CMake entry point for building the
binaries and producing packaged (`.deb` / `.rpm`) or unpackaged binaries.

## Prerequisites

Before building BeeGFS Index, install the following dependency packages. SQLite3,
the SQLite3 PCRE extension, and jemalloc are built automatically from
`contrib/deps/` during the build and do not need system packages.

### Red Hat / CentOS

```
$ sudo dnf install gcc gcc-c++ make cmake pkgconf-pkg-config python3 \
  pcre2-devel libattr-devel zlib-devel rpm-build dpkg
```

The `rpm-build` package is only required to build RPM packages and `dpkg` is
only required to build DEB packages. Install whichever matches the package type
you intend to produce.

### Debian and Ubuntu

```
$ sudo apt install --no-install-recommends build-essential cmake pkg-config \
  python3 libpcre2-dev libattr1-dev zlib1g-dev rpm dpkg-dev
```

The `rpm` package is only required to build RPM packages and `dpkg-dev` is only
required to build DEB packages. Install whichever matches the package type you
intend to produce.

### Optional: portable and cross-architecture builds

To pin the glibc version (so packages run on older distributions) or to
cross-compile for a foreign architecture, install [`zig`](https://ziglang.org/download/)
at `/opt/zig/zig`. When present, the packaging script uses it automatically.

## Building Packages

BeeGFS Index comes with the `.ci/packages.sh` helper, which wraps the CMake
configure, build, and `cpack` steps. Run it from the repository root. The
generated packages are written to the repository-root `packages/` directory
(`CPACK_OUTPUT_FILE_PREFIX`).

### For development systems

To build an RPM package, run
```
$ bash .ci/packages.sh rpm
```

For a DEB package use
```
$ bash .ci/packages.sh deb
```

### For production systems, or from source snapshots

By default the packaging system generates version numbers suitable only for
development packages. Packages intended for installation on production systems,
or builds from a source snapshot without Git tags, must set the version
explicitly. This is done by passing `BEEGFS_INDEX_VERSION=<version>`, e.g.
```
$ BEEGFS_INDEX_VERSION=8.0.0 bash .ci/packages.sh deb
```

Setting the version explicitly raises the Debian epoch to 20, generating
packages that can be easily upgraded with the system package manager.

### Portable / cross-architecture packages

With `zig` installed at `/opt/zig/zig`, set the target before invoking the
helper:
```
$ TARGET_ARCH=aarch64 GLIBC_VERSION=2.28 bash .ci/packages.sh deb
```

This links the binaries against the requested glibc version and supports
building for a foreign architecture.

## Building without packaging

To build the binaries without generating any packages, configure with CMake and
run `make` from the repository root:
```
$ mkdir build && cd build
$ cmake ../beegfs_deploy -DBEEGFS_INDEX_VERSION=8.0.0
$ make -j"$(nproc)"
```

`BEEGFS_INDEX_VERSION` defaults to the current `git describe` tag if omitted.
The BeeGFS plugins are built as
`build/contrib/plugins/beegfs/libbeegfs_indexing.so` and
`libbeegfs_querying.so`.

Useful options (append to the `cmake` line):
- `-DCMAKE_BUILD_TYPE=Debug` — debug build (or `export BEEGFS_DEBUG=1`).
- `-DDEP_USE_JEMALLOC=OFF` — do not link with jemalloc.

# Install prefix

Packages install BeeGFS Index under `/opt/beegfs`
(`CPACK_PACKAGING_INSTALL_PREFIX`).

# Setup Instructions
Detailed guides on how to configure BeeGFS can be found at
[doc.beegfs.io](https://doc.beegfs.io/latest/index.html)

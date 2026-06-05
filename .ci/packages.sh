#!/bin/sh
# Usage: [TARGET_ARCH=<arch>] [GLIBC_VERSION=<x.y>] bash .ci/packages.sh <deb|rpm>
# Requires zig at /opt/zig/zig for glibc pinning and cross-compilation.

set -e -u -x

pkgtype="${1:-}"
CONCURRENCY="$(nproc)"

# Only forward an explicit version. When BEEGFS_INDEX_VERSION is unset, CMake
# derives it from `git describe` and marks the build as a development build
# (Debian epoch 19). Setting it explicitly marks a production build (epoch 20).
if [ -n "${BEEGFS_INDEX_VERSION:-}" ]; then
    VERSION_ARG="-DBEEGFS_INDEX_VERSION=${BEEGFS_INDEX_VERSION}"
else
    VERSION_ARG=""
fi

if [ "${pkgtype}" = "deb" ]; then
    generator="DEB"
elif [ "${pkgtype}" = "rpm" ]; then
    generator="RPM"
else
    echo "Invalid package type: $pkgtype"; exit 1
fi

NATIVE_ARCH="$(uname -m)"
TARGET_ARCH="${TARGET_ARCH:-${NATIVE_ARCH}}"
GLIBC_VERSION="${GLIBC_VERSION:-2.28}"

if [ -x "/opt/zig/zig" ]; then
    ZIG_WRAPPER="$(mktemp -d)/zigcc-${TARGET_ARCH}"
    printf '#!/bin/sh\nexec /opt/zig/zig cc -target %s-linux-gnu.%s -fno-sanitize=all "$@"\n' \
        "${TARGET_ARCH}" "${GLIBC_VERSION}" > "${ZIG_WRAPPER}"
    chmod +x "${ZIG_WRAPPER}"
    export CC="${ZIG_WRAPPER}"
fi

# Inject a wrapper features.h to pin __GLIBC_MINOR__ to the target version.
# Without this, the host glibc headers redirect sscanf to a versioned symbol
# (e.g. __isoc23_sscanf on glibc >= 2.38) that is absent from the older target
# glibc we link against under -D_GNU_SOURCE.
if [ -n "${CC:-}" ] && "${CC}" --version 2>/dev/null | grep -qi "zig"; then
    GLIBC_FIX_DIR="$(mktemp -d)"
    GLIBC_MINOR=$(echo "${GLIBC_VERSION}" | cut -d. -f2)
    printf '#include_next <features.h>\n#undef __GLIBC_MINOR__\n#define __GLIBC_MINOR__ %s\n' \
        "${GLIBC_MINOR}" > "${GLIBC_FIX_DIR}/features.h"
    export CFLAGS="${CFLAGS:+${CFLAGS} }-I${GLIBC_FIX_DIR} -fno-sanitize=all"
fi

case "${TARGET_ARCH}" in
  x86_64)  DEB_ARCH="amd64" ;;
  aarch64) DEB_ARCH="arm64" ;;
  *)       DEB_ARCH="${TARGET_ARCH}" ;;
esac
RPM_ARCH="${TARGET_ARCH}"

EXTRA_CMAKE_ARGS=""
if [ "${TARGET_ARCH}" != "${NATIVE_ARCH}" ]; then
    if command -v apt-get >/dev/null 2>&1; then
        # Restrict existing sources to native arch before adding the foreign one.
        # Ubuntu 24.04+ uses deb822 format; older releases use sources.list.
        NATIVE_DEB_ARCH=$(dpkg --print-architecture)
        if [ -f "/etc/apt/sources.list.d/ubuntu.sources" ]; then
            if ! grep -q '^Architectures:' /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null; then
                sudo sed -i "/^Types: deb/a Architectures: ${NATIVE_DEB_ARCH}" \
                    /etc/apt/sources.list.d/ubuntu.sources
            fi
        elif ! grep -q '\[arch=' /etc/apt/sources.list 2>/dev/null; then
            sudo sed -i "s|^deb |deb [arch=${NATIVE_DEB_ARCH}] |g" /etc/apt/sources.list
        fi

        sudo dpkg --add-architecture "${DEB_ARCH}"

        CODENAME=$(. /etc/os-release 2>/dev/null && echo "${VERSION_CODENAME:-noble}")
        case "${DEB_ARCH}" in
          amd64) CROSS_MIRROR="http://archive.ubuntu.com/ubuntu" ;;
          arm64) CROSS_MIRROR="http://ports.ubuntu.com/ubuntu-ports" ;;
          *)     CROSS_MIRROR="" ;;
        esac
        if [ -n "${CROSS_MIRROR}" ]; then
            APT_SRC="/etc/apt/sources.list.d/beegfs-cross-${DEB_ARCH}.list"
            if [ ! -f "${APT_SRC}" ]; then
                printf 'deb [arch=%s] %s %s main restricted universe\n' \
                    "${DEB_ARCH}" "${CROSS_MIRROR}" "${CODENAME}" | sudo tee "${APT_SRC}" > /dev/null
                printf 'deb [arch=%s] %s %s-updates main restricted universe\n' \
                    "${DEB_ARCH}" "${CROSS_MIRROR}" "${CODENAME}" | sudo tee -a "${APT_SRC}" > /dev/null
                printf 'deb [arch=%s] %s %s-security main restricted universe\n' \
                    "${DEB_ARCH}" "${CROSS_MIRROR}" "${CODENAME}" | sudo tee -a "${APT_SRC}" > /dev/null
            fi
        fi

        sudo apt-get update -q
        sudo apt-get install -y "libpcre2-dev:${DEB_ARCH}" "libattr1-dev:${DEB_ARCH}" "zlib1g-dev:${DEB_ARCH}"
    fi
    export PKG_CONFIG_PATH="/usr/lib/${TARGET_ARCH}-linux-gnu/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
    EXTRA_CMAKE_ARGS="-DDEP_USE_JEMALLOC=OFF -DCROSS_COMPILE=ON"
fi

rm -rf build
mkdir build
cd build
# shellcheck disable=SC2086
cmake ../beegfs_deploy \
    -DPACKAGE_TYPE="${pkgtype}" \
    -DCMAKE_BUILD_TYPE=Release \
    ${VERSION_ARG} \
    -DDEB_ARCH="${DEB_ARCH}" \
    -DRPM_ARCH="${RPM_ARCH}" \
    ${EXTRA_CMAKE_ARGS}
make -j"${CONCURRENCY}"
cpack -G "${generator}"

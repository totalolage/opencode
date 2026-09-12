# Fork CLI releases

This document records the release contract for the fork's CLI.

## Distribution scope

The fork distributes CLI binaries for these targets only:

- Linux with glibc x64
- Linux with glibc arm64
- macOS x64
- macOS arm64

The fork does not distribute Windows binaries, musl Linux binaries, Desktop builds, or package-manager packages.

Stable releases use the public GitHub repository [`totalolage/opencode`](https://github.com/totalolage/opencode) and tags in the `vX.Y.Z` form. The fork version is explicit and independent of the upstream npm version.

## Build identity and versions

The default build identity is `totalolage/opencode`.

Set `OPENCODE_UPSTREAM_BUILD=1` only when a maintainer needs to preserve the upstream build path. Do not use this opt-out as a normal user action. `OPENCODE_UPSTREAM_BUILD=1` cannot be combined with `OPENCODE_FORK_RELEASE=1`.

For a stable fork release, set an explicit `OPENCODE_VERSION=X.Y.Z` without the `v` prefix and `OPENCODE_CHANNEL=latest` with `OPENCODE_FORK_RELEASE=1`. The release tag adds the `v` prefix, so the tag is `vX.Y.Z`. A non-`latest` channel is a preview build and is outside the stable update policy.

## Release workflow

The fork release workflow is `.github/workflows/fork-release.yml`. A manual run accepts `version` and `ref` inputs. The `publish` input defaults to `false`.

The workflow:

1. Resolves `ref` to a commit.
2. Builds artifacts on four native runners for Linux x64, Linux arm64, macOS x64, and macOS arm64.
3. Produces workflow artifacts without a release when `publish=false`.
4. Creates a draft GitHub release only with explicit `publish=true`. A maintainer must promote the draft manually.

The publication step requests `contents:write` and uses only `GITHUB_TOKEN`. The existing upstream `.github/workflows/publish.yml` workflow is not the fork release path.

## Pull request verification

`.github/workflows/fork-update-test.yml` runs on pull requests. It runs the focused installation tests and the real upgrade verifier on four native runners: Linux x64, Linux arm64, macOS x64, and macOS arm64. It excludes Windows. The separate `.github/workflows/fork-release.yml` workflow remains manual.

From `packages/opencode`, the workflow runs:

```sh
bun test test/installation --timeout 30000
bun run script/verify-fork-upgrade.ts
```

## Build artifacts

Build the CLI from the repository root with the explicit fork version. Replace `...` with that version.

```sh
OPENCODE_FORK_RELEASE=1 \
OPENCODE_VERSION=... \
OPENCODE_CHANNEL=latest \
bun run --cwd packages/opencode build --single --skip-install --skip-embed-web-ui
```

Run the build on each native runner. The command writes native binaries under `packages/opencode/dist/`. It does not create release archives. The workflow's package step creates the archives, and its verify step generates and checks `SHA256SUMS`. The `--skip-install` flag assumes that dependencies are already installed.

The `--baseline` option is a boolean flag. Add `--baseline` when building x64.

For x64, package `packages/opencode/dist/opencode-PLATFORM-x64-baseline/bin/opencode`. Use the normal x64 archive name without the `-baseline` suffix. For arm64, package `packages/opencode/dist/opencode-PLATFORM-arm64/bin/opencode`. `PLATFORM` is `linux` or `darwin`.

The workflow archives the binaries with these names:

- Linux x64: `opencode-linux-x64.tar.gz`
- Linux arm64: `opencode-linux-arm64.tar.gz`
- macOS x64: `opencode-darwin-x64.zip`
- macOS arm64: `opencode-darwin-arm64.zip`

Generate `SHA256SUMS` for the release archives. A checksum served over HTTPS detects corruption. It is not an independent signature. The trust root is GitHub and the repository maintainers. The workflow uses no signing secrets.

## Install a stable release

The root `install` script accepts these options:

- `--version X.Y.Z` or `--version vX.Y.Z` to install a specific stable version
- `--binary /path/to/opencode` to install a local binary
- `--no-modify-path` to leave shell configuration files unchanged

The default target is `$HOME/.opencode/bin/opencode`.

The remote installer entry point is `https://raw.githubusercontent.com/totalolage/opencode/dev/install`. Download it before execution so you can inspect the script. Do not pipe the remote script directly into `bash`.

```sh
(
  set -e
  temp=$(mktemp -d) || exit
  trap 'rm -rf -- "$temp"' EXIT
  curl --proto '=https' --proto-redir '=https' -fSL \
    -o "$temp/install" \
    https://raw.githubusercontent.com/totalolage/opencode/dev/install
  less "$temp/install"
  bash "$temp/install" --version X.Y.Z
)
```

Download mode checks for `curl`, `python3`, `uname`, and `stat`, plus either `sha256sum` or `shasum`. The installer uses Python's `tarfile` and `zipfile` modules for extraction. It does not invoke `tar` or `unzip` for extraction.

`--binary` installs an existing regular local file. Treat that file as a locally trusted executable. The installer does not sign it or verify an integrity value for it, so this option provides no integrity or provenance guarantee.

## Update rules

The updater honors existing settings and flags. `autoupdate=false` and `OPENCODE_DISABLE_AUTOUPDATE` disable automatic update handling. `autoupdate="notify"` and `OPENCODE_ALWAYS_NOTIFY_UPDATE` notify instead of installing.

- When settings permit automatic updates, install a newer patch version automatically.
- Notify about a larger version change instead of installing it automatically.
- Never install an equal or older version automatically.
- Permit an explicit manual downgrade with `--version` or `--binary`.
- Do not switch an unknown installation or a package-manager installation to upstream. Provide manual instructions for those cases.

## Verification

From `packages/opencode`, run the end-to-end proof command when checking an update:

```sh
bun run script/verify-fork-upgrade.ts
```

The proof builds two versions under an isolated temporary `HOME` and a local fixture. It never uses a real installed CLI.

Test builds may combine `OPENCODE_FORK_RELEASE=1` with `--fork-update-test`. Set `OPENCODE_FORK_TEST_ORIGIN` only for that test build. The test origin is embedded at build time, not read as a runtime setting. Keep `OPENCODE_RELEASE` unset. The test flag cannot be combined with release mode, and fixture binaries are never published.

import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import { createHash } from "node:crypto"
import os from "os"
import path from "path"

const repositoryRoot = path.resolve(import.meta.dir, "../../../..")
const installer = path.join(repositoryRoot, "install")

async function run(command: string[], options?: { cwd?: string; env?: Record<string, string> }) {
  const process = Bun.spawn(command, {
    cwd: options?.cwd,
    env: options?.env,
    stdout: "pipe",
    stderr: "pipe",
  })
  const [code, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
  ])
  return { code, stdout, stderr }
}

async function writeExecutable(file: string, contents: string) {
  await Bun.write(file, contents)
  await fs.chmod(file, 0o755)
}

async function sha256(file: string) {
  const contents = new Uint8Array(await Bun.file(file).arrayBuffer())
  return createHash("sha256").update(contents).digest("hex")
}

async function makeFixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-installer-test-"))
  const home = path.join(root, "home")
  const fixtureBin = path.join(root, "bin")
  const fixtureData = path.join(root, "data")
  const curlArgs = path.join(root, "curl-args")
  await fs.mkdir(home, { recursive: true })
  await fs.mkdir(fixtureBin, { recursive: true })
  await fs.mkdir(fixtureData, { recursive: true })

  await writeExecutable(
    path.join(fixtureBin, "uname"),
    `#!/usr/bin/env bash
set -eu
case "\${1:-}" in
  -s) printf '%s\n' "\${UNAME_S:-Linux}" ;;
  -m) printf '%s\n' "\${UNAME_M:-x86_64}" ;;
  *) exit 1 ;;
esac
`,
  )
  await writeExecutable(
    path.join(fixtureBin, "curl"),
    `#!/usr/bin/env bash
set -eu

printf 'ARGS' >> "$CURL_ARGS_LOG"
for arg in "$@"; do printf '\t%s' "$arg" >> "$CURL_ARGS_LOG"; done
printf '\n' >> "$CURL_ARGS_LOG"

output=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o)
      output="$2"
      shift 2
      ;;
    --proto|--proto-redir)
      shift 2
      ;;
    -fSL)
      shift
      ;;
  *)
      url="$1"
      shift
      ;;
  esac
done

case "$url" in
  https://api.github.com/repos/totalolage/opencode/releases\\?per_page=100\\&page=*)
    page="\${url##*page=}"
    case "\$page" in
      ''|*[!0-9]*)
        printf 'unexpected URL: %s\n' "\$url" >&2
        exit 1
        ;;
    esac
    source="\$CURL_FIXTURE_ROOT/releases-\$page.json"
    ;;
  https://api.github.com/repos/totalolage/opencode/releases/tags/*)
    source="$CURL_FIXTURE_ROOT/pinned.json"
    ;;
  https://github.com/totalolage/opencode/releases/download/*/SHA256SUMS)
    source="$CURL_FIXTURE_ROOT/SHA256SUMS"
    ;;
  https://github.com/totalolage/opencode/releases/download/*/*)
    source="$CURL_FIXTURE_ROOT/\${url##*/}"
    ;;
  *)
    printf 'unexpected URL: %s\n' "$url" >&2
    exit 1
    ;;
esac

if [ -n "\${CURL_MUTATE_TARGET:-}" ] && [[ "$url" == https://github.com/totalolage/opencode/releases/download/*/opencode-* ]]; then
  if [ -n "\${CURL_MUTATE_CONTENT+x}" ]; then
    printf '%s' "$CURL_MUTATE_CONTENT" > "$CURL_MUTATE_TARGET"
  else
    printf 'externally changed\\n' > "$CURL_MUTATE_TARGET"
  fi
fi

if [ -n "\${CURL_READY_MARKER:-}" ] && [[ "$url" == https://github.com/totalolage/opencode/releases/download/*/opencode-* ]]; then
  : > "$CURL_READY_MARKER"
  if [ -n "\${CURL_BLOCK_FIFO:-}" ]; then
    read _ < "$CURL_BLOCK_FIFO"
  fi
fi

cp "$source" "$output"
`,
  )

  const env: Record<string, string> = {
    ...Object.fromEntries(
      Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    HOME: home,
    PATH: `${fixtureBin}:${process.env.PATH ?? ""}`,
    GITHUB_PATH: path.join(root, "github-path"),
    CURL_ARGS_LOG: curlArgs,
    CURL_FIXTURE_ROOT: fixtureData,
    SHELL: "/bin/bash",
  }

  return {
    root,
    home,
    fixtureBin,
    fixtureData,
    curlArgs,
    env,
  }
}

type Fixture = Awaited<ReturnType<typeof makeFixture>>

async function makeArchive(
  fixture: Fixture,
  options: { platform: "linux" | "darwin"; arch: "x64" | "arm64"; contents: Record<string, string> },
) {
  const source = path.join(fixture.root, `source-${options.platform}-${options.arch}`)
  await fs.mkdir(source, { recursive: true })
  await Promise.all(
    Object.entries(options.contents).map(([name, contents]) => Bun.write(path.join(source, name), contents)),
  )

  const extension = options.platform === "linux" ? "tar.gz" : "zip"
  const filename = `opencode-${options.platform}-${options.arch}.${extension}`
  const archive = path.join(fixture.fixtureData, filename)
  const result =
    options.platform === "linux"
      ? await run(["tar", "-czf", archive, "-C", source, ...Object.keys(options.contents)])
      : await run(["zip", "-q", archive, ...Object.keys(options.contents)], { cwd: source })
  if (result.code !== 0) throw new Error(result.stderr)
  const digest = await sha256(archive)
  await Bun.write(path.join(fixture.fixtureData, "SHA256SUMS"), `${digest}  ${filename}\n`)
  return { filename, archive }
}

async function writeRelease(fixture: Fixture, name: string, release: unknown) {
  await Bun.write(path.join(fixture.fixtureData, `${name}.json`), JSON.stringify(release))
}

const stableRelease = (version: string) => ({ tag_name: `v${version}`, draft: false, prerelease: false })
const suffixOf = (version: string) => version.includes("-f8y-")

async function defaultList(fixture: Fixture, version: string) {
  await writeRelease(fixture, "releases-1", [stableRelease(version)])
}

async function targetPath(fixture: Fixture) {
  return path.join(fixture.home, ".opencode", "bin", "opencode")
}

async function stagePath(fixture: Fixture) {
  const directory = path.join(fixture.home, ".opencode", "bin")
  return fs
    .readdir(directory)
    .then((entries) => entries.filter((entry) => /^\.opencode-install\.[A-Za-z0-9]{6}$/.test(entry)))
    .catch(() => [])
}

async function seedTarget(fixture: Fixture, contents: string) {
  const target = await targetPath(fixture)
  await fs.mkdir(path.dirname(target), { recursive: true })
  await Bun.write(target, contents)
  await fs.chmod(target, 0o755)
}

async function writeVersionTarget(fixture: Fixture, version: string) {
  const target = await targetPath(fixture)
  await fs.mkdir(path.dirname(target), { recursive: true })
  await writeExecutable(
    target,
    `#!/usr/bin/env bash
if [ "\$1" = "--version" ]; then
  printf '%s\\n' '${version}'
  exit 0
fi
exit 1
`,
  )
}

async function injectCommandFailure(fixture: Fixture, command: "cp" | "chmod" | "mv") {
  await writeExecutable(
    path.join(fixture.fixtureBin, command),
    `#!/usr/bin/env bash
printf 'INJECTED ${command} failure\\n' >&2
exit 97
`,
  )
}

async function waitForFile(file: string, timeout = 5_000) {
  const deadline = Date.now() + timeout
  while (Date.now() <= deadline) {
    if (
      await fs
        .stat(file)
        .then(() => true)
        .catch(() => false)
    )
      return
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error(`timed out waiting for ${file}`)
}

async function releaseFifo(file: string) {
  const writer = Bun.spawn(["bash", "-c", "printf '\\n' > \"$1\"", "release", file], {
    stdout: "ignore",
    stderr: "ignore",
  })
  const timer = setTimeout(() => writer.kill(), 2_000)
  await writer.exited
  clearTimeout(timer)
}

async function curlCalls(fixture: Fixture) {
  const log = await fs.readFile(fixture.curlArgs, "utf8").catch(() => "")
  return log
    .split("\n")
    .filter(Boolean)
    .map((line) => line.split("\t").slice(1))
}

async function expectInstallFailurePreservesTarget(fixture: Fixture, args: string[]) {
  await seedTarget(fixture, "previous binary")
  const result = await run(["bash", installer, ...args], { env: fixture.env })
  expect(result.code).not.toBe(0)
  expect(await Bun.file(await targetPath(fixture)).text()).toBe("previous binary")
  expect(await stagePath(fixture)).toEqual([])
  return result
}

describe("root installer", () => {
  test("help points at the fork-owned root installer", async () => {
    const fixture = await makeFixture()
    try {
      const result = await run(["bash", installer, "--help"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(result.stdout).toContain("https://raw.githubusercontent.com/totalolage/opencode/dev/install")
      expect(result.stdout).not.toContain("https://opencode.ai/install")
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("installs the latest Linux release from the fixed GitHub repository", async () => {
    const fixture = await makeFixture()
    try {
      await defaultList(fixture, "1.2.3")
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "latest binary" } })

      const result = await run(["bash", installer, "--no-modify-path"], {
        env: {
          ...fixture.env,
          OPENCODE_INSTALL_REPOSITORY: "attacker/override",
          OPENCODE_INSTALL_BASE_URL: "https://attacker.invalid",
        },
      })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("latest binary")
      expect((await fs.stat(await targetPath(fixture))).mode & 0o777).toBe(0o755)

      const calls = await curlCalls(fixture)
      expect(calls.map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
        "https://github.com/totalolage/opencode/releases/download/v1.2.3/opencode-linux-x64.tar.gz",
        "https://github.com/totalolage/opencode/releases/download/v1.2.3/SHA256SUMS",
      ])
      for (const args of calls) {
        expect(args).toContain("--proto")
        expect(args).toContain("=https")
        expect(args).toContain("--proto-redir")
        expect(args).toContain("=https")
        expect(args).toContain("-fSL")
      }
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("normalizes a pinned v-prefixed version and selects the Darwin arm64 asset", async () => {
    const fixture = await makeFixture()
    try {
      fixture.env.UNAME_S = "Darwin"
      fixture.env.UNAME_M = "arm64"
      await writeRelease(fixture, "pinned", { tag_name: "v2.3.4", draft: false, prerelease: false })
      await makeArchive(fixture, { platform: "darwin", arch: "arm64", contents: { opencode: "pinned binary" } })

      const result = await run(["bash", installer, "--version", "v2.3.4", "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("pinned binary")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases/tags/v2.3.4",
        "https://github.com/totalolage/opencode/releases/download/v2.3.4/opencode-darwin-arm64.zip",
        "https://github.com/totalolage/opencode/releases/download/v2.3.4/SHA256SUMS",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("accepts strict suffix versions including boundary timestamps when pinned", async () => {
    for (const version of [
      "1.18.30-f8y-20260913140000",
      "1.18.30-f8y-20240229235959",
      "1.18.30-f8y-00011231000000",
      "9007199254740991.0.0",
    ]) {
      const fixture = await makeFixture()
      try {
        await writeRelease(fixture, "pinned", {
          tag_name: `v${version}`,
          draft: false,
          prerelease: suffixOf(version),
        })
        await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: `suffix ${version}` } })

        const result = await run(["bash", installer, "--version", version, "--no-modify-path"], { env: fixture.env })

        expect(result.code, version).toBe(0)
        expect(await Bun.file(await targetPath(fixture)).text(), version).toBe(`suffix ${version}`)
        expect((await curlCalls(fixture)).map((args) => args.at(-1)), version).toEqual([
          `https://api.github.com/repos/totalolage/opencode/releases/tags/v${version}`,
          `https://github.com/totalolage/opencode/releases/download/v${version}/opencode-linux-x64.tar.gz`,
          `https://github.com/totalolage/opencode/releases/download/v${version}/SHA256SUMS`,
        ])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("rejects pinned releases with mismatched draft or prerelease flags", async () => {
    const cases = [
      { version: "1.2.3", release: { tag_name: "v1.2.3", draft: false, prerelease: true } },
      { version: "1.2.3", release: { tag_name: "v1.2.3", draft: true, prerelease: false } },
      { version: "1.2.3-f8y-20260913140000", release: { tag_name: "v1.2.3-f8y-20260913140000", draft: false, prerelease: false } },
      { version: "1.2.3-f8y-20260913140000", release: { tag_name: "v1.2.3-f8y-20260913140000", draft: true, prerelease: true } },
    ]

    for (const item of cases) {
      const fixture = await makeFixture()
      try {
        await writeRelease(fixture, "pinned", item.release)
        const result = await expectInstallFailurePreservesTarget(fixture, [
          "--version",
          item.version,
          "--no-modify-path",
        ])
        expect(result.stderr, item.version).toContain("release metadata")
        expect((await curlCalls(fixture)).map((args) => args.at(-1)), item.version).toEqual([
          `https://api.github.com/repos/totalolage/opencode/releases/tags/v${item.version}`,
        ])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("skips an implicit downgrade when the latest release is equal to or older than the target", async () => {
    for (const currentVersion of ["1.2.3", "1.2.4"]) {
      const fixture = await makeFixture()
      try {
        await defaultList(fixture, "1.2.3")
        await writeVersionTarget(fixture, currentVersion)

        const previousTarget = await Bun.file(await targetPath(fixture)).text()
        const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

        expect(result.code, currentVersion).toBe(0)
        expect(await Bun.file(await targetPath(fixture)).text(), currentVersion).toBe(previousTarget)
        expect(
          (await curlCalls(fixture)).map((args) => args.at(-1)),
          currentVersion,
        ).toEqual(["https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1"])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("compares numeric version components before an implicit upgrade", async () => {
    const fixture = await makeFixture()
    try {
      await defaultList(fixture, "2.10.0")
      await writeVersionTarget(fixture, "2.9.0")
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "numeric upgrade" } })

      const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("numeric upgrade")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toHaveLength(3)
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("orders stable above suffix and suffixes by timestamp during implicit upgrades", async () => {
    const cases = [
      {
        name: "upgrades a suffix to a stable release on the same base",
        latest: "1.18.30",
        current: "1.18.30-f8y-20260913140000",
        installs: true,
      },
      {
        name: "upgrades to a suffix with a newer timestamp",
        latest: "1.18.30-f8y-20260913150000",
        current: "1.18.30-f8y-20260913140000",
        installs: true,
      },
      {
        name: "keeps a suffix with a newer timestamp",
        latest: "1.18.30-f8y-20260913130000",
        current: "1.18.30-f8y-20260913140000",
        installs: false,
      },
      {
        name: "keeps a stable release over a suffix on the same base",
        latest: "1.18.30-f8y-20260913140000",
        current: "1.18.30",
        installs: false,
      },
    ]

    for (const item of cases) {
      const fixture = await makeFixture()
      try {
        await writeRelease(fixture, "releases-1", [
          { tag_name: `v${item.latest}`, draft: false, prerelease: suffixOf(item.latest) },
        ])
        await writeVersionTarget(fixture, item.current)
        if (item.installs) {
          await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "upgraded binary" } })
        }

        const previousTarget = await Bun.file(await targetPath(fixture)).text()
        const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

        expect(result.code, `${item.name}: ${result.stderr}`).toBe(0)
        if (item.installs) {
          expect(await Bun.file(await targetPath(fixture)).text(), item.name).toBe("upgraded binary")
          expect((await curlCalls(fixture)).map((args) => args.at(-1)), item.name).toHaveLength(3)
        } else {
          expect(await Bun.file(await targetPath(fixture)).text(), item.name).toBe(previousTarget)
          expect((await curlCalls(fixture)).map((args) => args.at(-1)), item.name).toEqual([
            "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
          ])
        }
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("rejects an in-place target modification during archive download", async () => {
    const fixture = await makeFixture()
    try {
      await defaultList(fixture, "2.0.0")
      await writeVersionTarget(fixture, "1.0.0")
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "downloaded binary" } })
      const target = await targetPath(fixture)
      const originalTarget = await fs.readFile(target)
      await fs.utimes(target, 1, 1)
      const beforeMutation = await fs.stat(target)
      fixture.env.CURL_MUTATE_TARGET = target
      fixture.env.CURL_MUTATE_CONTENT = "X".repeat(originalTarget.byteLength)

      const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

      expect(result.code).not.toBe(0)
      expect((await fs.stat(target)).size).toBe(beforeMutation.size)
      expect((await fs.stat(target)).mtimeMs).toBeGreaterThan(beforeMutation.mtimeMs)
      expect(await fs.readFile(target, "utf8")).toBe(fixture.env.CURL_MUTATE_CONTENT)
      expect(await stagePath(fixture)).toEqual([])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("fails an implicit install when the existing target has no strict stable version", async () => {
    const fixture = await makeFixture()
    try {
      await defaultList(fixture, "1.2.3")
      const result = await expectInstallFailurePreservesTarget(fixture, ["--no-modify-path"])

      expect(result.stderr).toContain("--version")
      expect(result.stderr).toContain("--binary")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("allows an explicit pinned downgrade despite the existing target version", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "pinned", { tag_name: "v1.2.3", draft: false, prerelease: false })
      await writeVersionTarget(fixture, "2.0.0")
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "explicit downgrade" } })

      const result = await run(["bash", installer, "--version", "1.2.3", "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("explicit downgrade")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases/tags/v1.2.3",
        "https://github.com/totalolage/opencode/releases/download/v1.2.3/opencode-linux-x64.tar.gz",
        "https://github.com/totalolage/opencode/releases/download/v1.2.3/SHA256SUMS",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("installs a local binary atomically without remote verification", async () => {
    const fixture = await makeFixture()
    try {
      const source = path.join(fixture.root, "local-opencode")
      const shellConfig = path.join(fixture.home, ".bashrc")
      await Bun.write(source, "local binary")
      await Bun.write(shellConfig, "export PATH=/usr/bin\n")
      const result = await run(["bash", installer, "--binary", source, "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("local binary")
      expect(await Bun.file(source).text()).toBe("local binary")
      expect((await fs.stat(await targetPath(fixture))).mode & 0o777).toBe(0o755)
      expect(await Bun.file(shellConfig).text()).toBe("export PATH=/usr/bin\n")
      expect(await curlCalls(fixture)).toEqual([])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("writes the GitHub Actions path only inside the fixture", async () => {
    const fixture = await makeFixture()
    try {
      const source = path.join(fixture.root, "local-opencode")
      const githubPath = path.join(fixture.root, "github-path")
      expect(fixture.env.GITHUB_PATH).toBe(githubPath)
      await Bun.write(source, "local binary")

      const result = await run(["bash", installer, "--binary", source, "--no-modify-path"], {
        env: { ...fixture.env, GITHUB_ACTIONS: "true" },
      })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("local binary")
      expect(await Bun.file(source).text()).toBe("local binary")
      expect(await Bun.file(githubPath).text()).toBe(`${path.dirname(await targetPath(fixture))}\n`)
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("installs when the local source is the existing target without truncating bytes", async () => {
    const fixture = await makeFixture()
    try {
      const target = await targetPath(fixture)
      const original = Uint8Array.from([0, 17, 34, 128, 200, 255, 10, 0, 42])
      await fs.mkdir(path.dirname(target), { recursive: true })
      await Bun.write(target, original)
      await fs.chmod(target, 0o755)

      const result = await run(["bash", installer, "--binary", target, "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(new Uint8Array(await Bun.file(target).arrayBuffer())).toEqual(original)
      expect(await curlCalls(fixture)).toEqual([])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  for (const command of ["cp", "chmod", "mv"] as const) {
    test(`preserves files and removes staging on injected ${command} failure`, async () => {
      const fixture = await makeFixture()
      try {
        const source = path.join(fixture.root, "local-opencode")
        await seedTarget(fixture, "previous binary")
        await Bun.write(source, "local binary")
        await injectCommandFailure(fixture, command)

        const result = await run(["bash", installer, "--binary", source, "--no-modify-path"], { env: fixture.env })

        expect(result.code).not.toBe(0)
        expect(result.stderr).toContain(`INJECTED ${command} failure`)
        expect(await Bun.file(await targetPath(fixture)).text()).toBe("previous binary")
        expect(await Bun.file(source).text()).toBe("local binary")
        expect(await stagePath(fixture)).toEqual([])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    })
  }

  test("preserves files when the install directory denies writes", async () => {
    const fixture = await makeFixture()
    const installDirectory = path.join(fixture.home, ".opencode", "bin")
    try {
      const source = path.join(fixture.root, "local-opencode")
      await seedTarget(fixture, "previous binary")
      await Bun.write(source, "local binary")
      await fs.chmod(installDirectory, 0o555)

      const result = await run(["bash", installer, "--binary", source, "--no-modify-path"], { env: fixture.env })

      expect(result.code).not.toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("previous binary")
      expect(await Bun.file(source).text()).toBe("local binary")
      expect(await stagePath(fixture)).toEqual([])
    } finally {
      try {
        await fs.chmod(installDirectory, 0o755)
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("rejects malformed, prerelease, build, suffix, and leading-zero versions before network access", async () => {
    const invalidVersions = [
      "1.2",
      "1.2.3.4",
      "1.02.3",
      "v01.2.3",
      "1.2.3-beta.1",
      "1.2.3+build",
      "vv1.2.3",
      " 1.2.3",
      "1.2.3 ",
      "9007199254740992.0.0",
      "1.9007199254740992.0",
      "1.2.9007199254740992",
      "1.2.3-f8y-2026091314000",
      "1.2.3-f8y-202609131400001",
      "1.2.3-f8y-20230229120000",
      "1.2.3-f8y-20260931235959",
      "1.2.3-f8y-00001231000000",
      "1.2.3-f8y-20261313140000",
      "1.2.3-f8y-2026091314000a",
      "1.2.3-f8y-20260913140000-x",
      "1.2.3-f8y-20260913140000-f8y-20260913140000",
      "1.2.3-f8x-20260913140000",
      "1.2.3-f8y-",
      "1.2.3+f8y-20260913140000",
      "1.2.3-f8y-20260913140000\n1.2.4",
      "1.2.3\n",
      "1.2.3-f8y-2026-0913140000",
    ]

    for (const version of invalidVersions) {
      const fixture = await makeFixture()
      try {
        const result = await run(["bash", installer, "--version", version, "--no-modify-path"], { env: fixture.env })
        expect(result.code, JSON.stringify(version)).not.toBe(0)
        expect(await curlCalls(fixture), JSON.stringify(version)).toEqual([])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("fails closed on malformed release list pages", async () => {
    const releases: unknown[] = [
      [],
      ["v1.2.3"],
      { tag_name: "v1.2.3", draft: false, prerelease: false },
      [{ tag_name: "v1.2.3", draft: false }],
      [{ tag_name: "v1.2.3", prerelease: false }],
      [{ tag_name: "v1.2.3", draft: "false", prerelease: false }],
      [{ tag_name: "v1.2.3", draft: false, prerelease: "false" }],
    ]

    for (const release of releases) {
      const fixture = await makeFixture()
      try {
        await writeRelease(fixture, "releases-1", release)
        const result = await expectInstallFailurePreservesTarget(fixture, ["--no-modify-path"])
        expect(result.stderr).toContain("release list")
        expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
          "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
        ])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("skips drafts, unsupported tags, and mismatched flags while discovering the latest release", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "releases-1", [
        { tag_name: "v9.9.9", draft: true, prerelease: false },
        { tag_name: "v9.9.9-f8y-20260913140000", draft: false, prerelease: false },
        { tag_name: "v8.0.0", draft: false, prerelease: true },
        { tag_name: "v1.2", draft: false, prerelease: false },
        { tag_name: "1.2.3", draft: false, prerelease: false },
        { tag_name: "v1.2.3-beta.1", draft: false, prerelease: true },
        { tag_name: "v01.2.3", draft: false, prerelease: false },
      ])

      const result = await expectInstallFailurePreservesTarget(fixture, ["--no-modify-path"])
      expect(result.stderr).toContain("no supported release")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("discovers the highest supported release across paginated lists", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(
        fixture,
        "releases-1",
        Array.from({ length: 100 }, () => stableRelease("1.0.0")),
      )
      await writeRelease(fixture, "releases-2", [
        { tag_name: "v1.2.3", draft: true, prerelease: false },
        stableRelease("9.9.9"),
      ])

      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "paginated binary" } })
      await writeVersionTarget(fixture, "1.0.0")

      const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

      expect(result.code).toBe(0)
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("paginated binary")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=1",
        "https://api.github.com/repos/totalolage/opencode/releases?per_page=100&page=2",
        "https://github.com/totalolage/opencode/releases/download/v9.9.9/opencode-linux-x64.tar.gz",
        "https://github.com/totalolage/opencode/releases/download/v9.9.9/SHA256SUMS",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("fails closed when the release list reaches the 100 page bound", async () => {
    const fixture = await makeFixture()
    try {
      for (let page = 1; page <= 100; page++) {
        await writeRelease(
          fixture,
          `releases-${page}`,
          Array.from({ length: 100 }, () => stableRelease("1.0.0")),
        )
      }
      await seedTarget(fixture, "previous binary")

      const result = await run(["bash", installer, "--no-modify-path"], { env: fixture.env })

      expect(result.code).not.toBe(0)
      expect(result.stderr).toContain("100 pages")
      expect(await Bun.file(await targetPath(fixture)).text()).toBe("previous binary")
      expect(await stagePath(fixture)).toEqual([])
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toHaveLength(100)
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("requires a pinned release tag to match the requested stable version", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "pinned", { tag_name: "v1.2.4", draft: false, prerelease: false })
      const result = await expectInstallFailurePreservesTarget(fixture, ["--version", "1.2.3", "--no-modify-path"])
      expect(result.stderr).toContain("release metadata")
      expect((await curlCalls(fixture)).map((args) => args.at(-1))).toEqual([
        "https://api.github.com/repos/totalolage/opencode/releases/tags/v1.2.3",
      ])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("preserves the installed binary when the archive digest is corrupt", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "pinned", { tag_name: "v1.2.3", draft: false, prerelease: false })
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "new binary" } })
      await Bun.write(path.join(fixture.fixtureData, "SHA256SUMS"), `${"0".repeat(64)}  opencode-linux-x64.tar.gz\n`)

      const result = await expectInstallFailurePreservesTarget(fixture, ["--version", "1.2.3", "--no-modify-path"])
      expect(result.stderr).toContain("checksum")
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("preserves the installed binary when the archive is corrupt", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "pinned", { tag_name: "v1.2.3", draft: false, prerelease: false })
      const archive = path.join(fixture.fixtureData, "opencode-linux-x64.tar.gz")
      await Bun.write(archive, "not a gzip archive")
      await Bun.write(
        path.join(fixture.fixtureData, "SHA256SUMS"),
        `${await sha256(archive)}  opencode-linux-x64.tar.gz\n`,
      )

      const result = await expectInstallFailurePreservesTarget(fixture, ["--version", "1.2.3", "--no-modify-path"])
      expect(result.stderr).toContain("archive")
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("rejects archives containing anything other than a regular opencode file", async () => {
    const fixture = await makeFixture()
    try {
      await writeRelease(fixture, "pinned", { tag_name: "v1.2.3", draft: false, prerelease: false })
      await makeArchive(fixture, {
        platform: "linux",
        arch: "x64",
        contents: { opencode: "new binary", unexpected: "unexpected payload" },
      })

      const result = await expectInstallFailurePreservesTarget(fixture, ["--version", "1.2.3", "--no-modify-path"])
      expect(result.stderr).toContain("regular file named opencode")
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("rejects unsupported Windows, musl, and CPU platforms with release instructions", async () => {
    const cases = [
      { os: "MINGW64_NT-10.0", arch: "x86_64" },
      { os: "Linux", arch: "x86_64", musl: true },
      { os: "Linux", arch: "riscv64" },
    ]

    for (const item of cases) {
      const fixture = await makeFixture()
      try {
        fixture.env.UNAME_S = item.os
        fixture.env.UNAME_M = item.arch
        if (item.musl) {
          await writeExecutable(
            path.join(fixture.fixtureBin, "ldd"),
            `#!/usr/bin/env bash
printf 'musl libc (x86_64)\n'
`,
          )
        }

        const result = await expectInstallFailurePreservesTarget(fixture, ["--no-modify-path"])
        expect(result.stderr).toContain("https://github.com/totalolage/opencode/releases")
        expect(await curlCalls(fixture)).toEqual([])
      } finally {
        await fs.rm(fixture.root, { recursive: true, force: true })
      }
    }
  })

  test("does not replace a symlink destination on failure", async () => {
    const fixture = await makeFixture()
    try {
      const target = await targetPath(fixture)
      const destination = path.join(fixture.root, "destination")
      await fs.mkdir(path.dirname(target), { recursive: true })
      await Bun.write(destination, "protected binary")
      await fs.symlink(destination, target)

      const result = await run(["bash", installer, "--binary", destination, "--no-modify-path"], { env: fixture.env })

      expect(result.code).not.toBe(0)
      expect((await fs.lstat(target)).isSymbolicLink()).toBe(true)
      expect(await Bun.file(destination).text()).toBe("protected binary")
      expect(await curlCalls(fixture)).toEqual([])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("rejects symlinked install parents before staging a local binary", async () => {
    const fixture = await makeFixture()
    try {
      const source = path.join(fixture.root, "local-opencode")
      const external = path.join(fixture.root, "external")
      await Bun.write(source, "local binary")
      await fs.mkdir(external, { recursive: true })
      await fs.symlink(external, path.join(fixture.home, ".opencode"))

      const result = await run(["bash", installer, "--binary", source, "--no-modify-path"], { env: fixture.env })

      expect(result.code).not.toBe(0)
      expect(result.stderr).toContain("parent directories")
      expect(await fs.readdir(external)).toEqual([])
      expect(await curlCalls(fixture)).toEqual([])
    } finally {
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })

  test("handles SIGTERM while downloading and removes only its own staging directory", async () => {
    const fixture = await makeFixture()
    const fifo = path.join(fixture.root, "curl-block")
    let child: ReturnType<typeof Bun.spawn> | undefined
    let childOutput: Promise<[number, string, string]> | undefined
    let markerReady = false
    let released = false
    try {
      await defaultList(fixture, "2.0.0")
      await writeVersionTarget(fixture, "1.0.0")
      await makeArchive(fixture, { platform: "linux", arch: "x64", contents: { opencode: "downloaded binary" } })
      const target = await targetPath(fixture)
      const originalTarget = await fs.readFile(target, "utf8")
      const sentinel = path.join(fixture.home, ".opencode", "bin", ".opencode-install.sentinel")
      await Bun.write(sentinel, "sentinel")

      const fifoResult = await run(["mkfifo", fifo])
      expect(fifoResult.code).toBe(0)
      fixture.env.CURL_READY_MARKER = path.join(fixture.root, "curl-ready")
      fixture.env.CURL_BLOCK_FIFO = fifo
      const running = Bun.spawn(["bash", installer, "--no-modify-path"], {
        env: fixture.env,
        stdout: "pipe",
        stderr: "pipe",
      })
      child = running
      childOutput = Promise.all([
        running.exited,
        new Response(running.stdout).text(),
        new Response(running.stderr).text(),
      ])

      await waitForFile(fixture.env.CURL_READY_MARKER)
      markerReady = true
      child.kill("SIGTERM")
      await releaseFifo(fifo)
      released = true
      const [code] = await childOutput

      expect(code).toBe(143)
      expect(await fs.readFile(target, "utf8")).toBe(originalTarget)
      expect(await fs.readFile(sentinel, "utf8")).toBe("sentinel")
      expect(await stagePath(fixture)).toEqual([])
    } finally {
      if (child !== undefined) {
        if (markerReady && !released) await releaseFifo(fifo)
        child.kill()
        await child.exited
        if (childOutput !== undefined) await childOutput
      }
      await fs.rm(fixture.root, { recursive: true, force: true })
    }
  })
})

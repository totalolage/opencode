#!/usr/bin/env bun
import { createHash } from "node:crypto"
import { appendFile, mkdtemp } from "node:fs/promises"
import { join, resolve } from "node:path"

const VERSION = "15.1.0"
const runnerTemp = process.env.RUNNER_TEMP
const githubPath = process.env.GITHUB_PATH

if (!runnerTemp) throw new Error("RUNNER_TEMP is required")
if (!githubPath) throw new Error("GITHUB_PATH is required")

const config =
  process.platform === "linux" && process.arch === "x64"
    ? {
        platform: "x86_64-unknown-linux-musl",
        extension: "tar.gz",
        checksum: "1c9297be4a084eea7ecaedf93eb03d058d6faae29bbc57ecdaf5063921491599",
      }
    : process.platform === "win32" && process.arch === "x64"
      ? {
          platform: "x86_64-pc-windows-msvc",
          extension: "zip",
          checksum: "124510b94b6baa3380d051fdf4650eaa80a302c876d611e9dba0b2e18d87493a",
        }
      : undefined

if (!config) throw new Error(`unsupported platform: ${process.platform}/${process.arch}`)

const directory = await mkdtemp(join(resolve(runnerTemp), "ripgrep-"))
const filename = `ripgrep-${VERSION}-${config.platform}.${config.extension}`
const archive = join(directory, filename)
const url = `https://github.com/BurntSushi/ripgrep/releases/download/${VERSION}/${filename}`
const response = await fetch(url)

if (!response.ok) throw new Error(`failed to download ripgrep: ${response.status} ${response.statusText}`)

const bytes = new Uint8Array(await response.arrayBuffer())
const checksum = createHash("sha256").update(bytes).digest("hex")
if (checksum !== config.checksum) throw new Error(`ripgrep checksum mismatch: ${checksum}`)

await Bun.write(archive, bytes)

if (process.platform === "linux") {
  await run(["tar", "-xzf", archive, "-C", directory])
}

if (process.platform === "win32") {
  await run([
    "powershell.exe",
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    `Expand-Archive -LiteralPath '${archive.replaceAll("'", "''")}' -DestinationPath '${directory.replaceAll("'", "''")}' -Force -ErrorAction Stop`,
  ])
}

const executableDirectory = resolve(join(directory, `ripgrep-${VERSION}-${config.platform}`))
const executable = join(executableDirectory, process.platform === "win32" ? "rg.exe" : "rg")
const version = await run([executable, "--version"])
if (!version.startsWith(`ripgrep ${VERSION}`)) throw new Error(`unexpected ripgrep version: ${version.trim()}`)

await appendFile(githubPath, `${executableDirectory}\n`)
console.log(`ripgrep ${VERSION} installed at ${executableDirectory}`)

async function run(command: string[]) {
  const process = Bun.spawn(command, { stdin: "ignore", stdout: "pipe", stderr: "pipe" })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ])
  if (exitCode !== 0) throw new Error(`${command[0]} exited with code ${exitCode}: ${stderr.trim() || stdout.trim()}`)
  return stdout
}

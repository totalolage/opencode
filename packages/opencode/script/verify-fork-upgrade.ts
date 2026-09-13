#!/usr/bin/env bun

import { createHash } from "node:crypto"
import { spawn } from "node:child_process"
import { chmod, copyFile, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Readable } from "node:stream"
import { ForkVersion } from "@opencode-ai/script/version"

const OLD_VERSION = "1.18.30-f8y-20260913130000"
const NEW_VERSION = "1.18.30-f8y-20260913140000"
const BUILD_TIMEOUT_MS = 10 * 60 * 1000
const COMMAND_TIMEOUT_MS = 60 * 1000
const VERSION_TIMEOUT_MS = 20 * 1000
const PROCESS_GROUP_GRACE_MS = 1000
const PROCESS_GROUP_WATCHDOG_MS = PROCESS_GROUP_GRACE_MS * 3
const ACTIONABLE_FORK_DIAGNOSTIC =
  /Install the totalolage\/opencode fork from https:\/\/github\.com\/totalolage\/opencode\/releases and place the opencode binary at/i
const CHECKSUM_MISMATCH_DIAGNOSTIC = /Checksum mismatch for .*expected [0-9a-f]{64}, received [0-9a-f]{64}/i
const TRUNCATED_DOWNLOAD_DIAGNOSTIC =
  /(Checksum mismatch for .*expected [0-9a-f]{64}, received [0-9a-f]{64}|content[- ]length|truncat|incomplete|premature|unexpected end|body.*(short|size))/i
const MISSING_RELEASE_DIAGNOSTIC = /(HTTP 404|release.*not found|request failed with HTTP 404)/i
const INVALID_RELEASE_DIAGNOSTIC = /No eligible fork release found/i
const PERMISSION_DIAGNOSTIC =
  /(EACCES|EPERM|permission denied|permissiondenied|not writable|read-only|operation not permitted)/i

type FixtureMode = "success" | "checksum-mismatch" | "truncated-download" | "missing-release" | "malformed-metadata"

type ReleasePaths = {
  latest: string
  tag: string
  archive: string
  checksums: string
}

type FixtureRequest = {
  scenario: string
  method: string
  url: string
  path: string
  status: number
  known: boolean
}

type ReleaseFixture = {
  archiveBytes: Uint8Array
  archiveHash: string
  checksumBytes: Uint8Array
  archiveName: string
  checksumName: string
}

type Fixture = {
  origin: string
  mode: FixtureMode
  scenario: string
  paths: ReleasePaths
  release?: ReleaseFixture
  requests: FixtureRequest[]
}

type CommandResult = {
  command: string[]
  exitCode: number | null
  signal: NodeJS.Signals | null
  stdout: string
  stderr: string
  timedOut: boolean
  cleanupError?: string
}

type EvidenceError = {
  stage: string
  message: string
  command?: string[]
  exitCode?: number | null
  signal?: NodeJS.Signals | null
  timedOut?: boolean
  stdout?: string
  stderr?: string
}

type BuildEvidence = {
  version: string
  command: string[]
  binaryPath: string
  exitCode: number | null
  signal?: NodeJS.Signals | null
  timedOut: boolean
  hash?: string
}

type ScenarioDefinition = {
  name: string
  mode: FixtureMode
  args: string[]
  executable: "installed" | "unknown"
  expectSuccess: boolean
  expectedVersion: string
  requiredRoutes: Array<{ path: string; status: number }>
  diagnostic?: RegExp
  requiresNonzero?: boolean
  permission?: "parent-unwritable" | "binary-read-only"
}

type ScenarioEvidence = {
  name: string
  mode: FixtureMode
  status: "passed" | "failed"
  executable: string
  command: string[]
  expected: "success" | "failure"
  expectedVersion: string
  before?: { version: string; hash: string }
  after?: { version?: string; hash?: string }
  exitCode?: number | null
  signal?: NodeJS.Signals | null
  timedOut?: boolean
  stdout?: string
  stderr?: string
  requests: FixtureRequest[]
  packageManagerCalls: string[]
  message?: string
  cleanupError?: string
}

type Evidence = {
  tool: string
  ok: boolean
  platform: {
    os: string
    arch: string
    libc: "glibc" | undefined
    baseline: boolean
  }
  versions: { old: string; new: string }
  hashes: {
    oldBinary?: string
    newBinary?: string
    archive?: string
  }
  release?: {
    origin: string
    archiveName: string
    checksumName: string
    paths: ReleasePaths
    archivePath: string
    checksumsPath: string
  }
  builds: BuildEvidence[]
  scenarios: ScenarioEvidence[]
  requestRoutes: FixtureRequest[]
  errors: EvidenceError[]
  artifactsRoot?: string
}

type BuildArtifact = {
  version: string
  path: string
  hash: string
}

type RuntimeContext = {
  root: string
  oldVersion: string
  oldArtifact: BuildArtifact
  newArtifact: BuildArtifact
  installedPath: string
  unknownPath: string
  runtimeEnv: Record<string, string>
  shimLog: string
  fixture: Fixture
  evidence: Evidence
}

function messageOf(error: unknown) {
  if (error instanceof Error) return error.stack ?? error.message
  return String(error)
}

function sha256(bytes: Uint8Array) {
  return createHash("sha256").update(bytes).digest("hex")
}

async function hashFile(filePath: string) {
  return sha256(new Uint8Array(await Bun.file(filePath).arrayBuffer()))
}

function commandSucceeded(result: CommandResult) {
  return result.exitCode === 0 && !result.timedOut && result.cleanupError === undefined
}

export function requireCommandCleanup(result: Pick<CommandResult, "cleanupError">) {
  if (result.cleanupError) throw new Error(`command cleanup failed: ${result.cleanupError}`)
}

function runningOnGlibc() {
  if (process.platform !== "linux") return true
  if (typeof process.report?.getReport !== "function") return false
  const report = process.report.getReport()
  if (!("header" in report) || typeof report.header !== "object" || report.header === null) return false
  return "glibcVersionRuntime" in report.header && typeof report.header.glibcVersionRuntime === "string"
}

const activeProcessGroups = new Set<number>()
const processGroupTerminations = new Map<number, Promise<void>>()
let interruptedBy: NodeJS.Signals | undefined
let interruptionCleanup: Promise<void> | undefined
let interruptionCleanupError: unknown

function hasErrorCode(error: unknown, code: string) {
  return typeof error === "object" && error !== null && "code" in error && error.code === code
}

function isProcessGroupId(pid: number | undefined): pid is number {
  return pid !== undefined && Number.isSafeInteger(pid) && pid > 1 && pid !== process.pid
}

function signalProcessGroup(pid: number, signal: NodeJS.Signals | 0) {
  try {
    process.kill(-pid, signal)
    return true
  } catch (error) {
    if (hasErrorCode(error, "ESRCH")) return false
    throw error
  }
}

function processGroupExists(pid: number) {
  try {
    process.kill(-pid, 0)
    return true
  } catch (error) {
    if (hasErrorCode(error, "ESRCH")) return false
    if (hasErrorCode(error, "EPERM")) return true
    throw error
  }
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
}

async function waitForProcessGroupExit(pid: number, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs
  while (processGroupExists(pid)) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) return false
    await delay(Math.min(25, remaining))
  }
  return true
}

async function terminateProcessGroup(pid: number) {
  if (!signalProcessGroup(pid, "SIGTERM")) return
  if (await waitForProcessGroupExit(pid, PROCESS_GROUP_GRACE_MS)) return
  signalProcessGroup(pid, "SIGKILL")
  if (!(await waitForProcessGroupExit(pid, PROCESS_GROUP_WATCHDOG_MS))) {
    throw new Error(`process group ${pid} survived SIGKILL`)
  }
}

function ensureProcessGroupTerminated(pid: number) {
  const existing = processGroupTerminations.get(pid)
  if (existing) return existing
  const termination = terminateProcessGroup(pid).finally(() => processGroupTerminations.delete(pid))
  processGroupTerminations.set(pid, termination)
  return termination
}

async function terminateActiveProcessGroups() {
  const groups = [...activeProcessGroups]
  const results = await Promise.allSettled(groups.map((pid) => ensureProcessGroupTerminated(pid)))
  const errors = results.filter((result): result is PromiseRejectedResult => result.status === "rejected")
  if (errors.length > 0) throw new Error(errors.map((result) => messageOf(result.reason)).join("; "))
  groups.forEach((pid) => activeProcessGroups.delete(pid))
}

function requestInterruption(signal: NodeJS.Signals) {
  interruptedBy ??= signal
  interruptionCleanup ??= terminateActiveProcessGroups().catch((error) => {
    interruptionCleanupError = error
  })
}

export function installSignalHandlers() {
  const onInterrupt = () => requestInterruption("SIGINT")
  const onTerminate = () => requestInterruption("SIGTERM")
  process.on("SIGINT", onInterrupt)
  process.on("SIGTERM", onTerminate)
  return () => {
    process.off("SIGINT", onInterrupt)
    process.off("SIGTERM", onTerminate)
  }
}

function throwIfInterrupted() {
  if (interruptedBy) throw new Error(`process interrupted by ${interruptedBy}`)
}

function captureOutput(stream: Readable | null) {
  if (!stream) return Promise.resolve("")
  stream.setEncoding("utf8")
  return new Promise<string>((resolve) => {
    let output = ""
    let settled = false
    const finish = () => {
      if (settled) return
      settled = true
      resolve(output)
    }
    stream.on("data", (chunk: string) => {
      output += chunk
    })
    stream.once("end", finish)
    stream.once("close", finish)
    stream.once("error", (error) => {
      output += `${output.length > 0 ? "\n" : ""}${messageOf(error)}`
      finish()
    })
  })
}

function waitForChild(child: ReturnType<typeof spawn>) {
  return new Promise<{ exitCode: number | null; signal: NodeJS.Signals | null; error?: unknown }>((resolve) => {
    let settled = false
    const finish = (result: { exitCode: number | null; signal: NodeJS.Signals | null; error?: unknown }) => {
      if (settled) return
      settled = true
      resolve(result)
    }
    child.once("error", (error) => finish({ exitCode: null, signal: null, error }))
    child.once("exit", (exitCode, signal) => finish({ exitCode, signal }))
  })
}

function commandFailure(stage: string, result: CommandResult, message: string): EvidenceError {
  return {
    stage,
    message,
    command: result.command,
    exitCode: result.exitCode,
    signal: result.signal,
    timedOut: result.timedOut,
    stdout: result.stdout,
    stderr: result.stderr,
  }
}

export async function runCommand(
  command: string[],
  options: { cwd: string; env: Record<string, string>; timeoutMs: number },
): Promise<CommandResult> {
  throwIfInterrupted()
  let child: ReturnType<typeof spawn>
  try {
    child = spawn(command[0], command.slice(1), {
      cwd: options.cwd,
      env: options.env,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    })
  } catch (error) {
    return {
      command,
      exitCode: null,
      signal: null,
      stdout: "",
      stderr: messageOf(error),
      timedOut: false,
    }
  }

  const childExit = waitForChild(child)
  const stdout = captureOutput(child.stdout)
  const stderr = captureOutput(child.stderr)
  if (!isProcessGroupId(child.pid)) {
    child.kill("SIGKILL")
    child.stdout?.destroy()
    child.stderr?.destroy()
    const result = await childExit
    return {
      command,
      exitCode: result.exitCode,
      signal: result.signal,
      stdout: await stdout,
      stderr: [await stderr, result.error ? messageOf(result.error) : "", "child did not provide a safe process group"]
        .filter(Boolean)
        .join("\n"),
      timedOut: false,
      cleanupError: "child did not provide a safe process group",
    }
  }

  const pid = child.pid
  activeProcessGroups.add(pid)
  let timedOut = false
  let cleanupError: string | undefined
  let termination: Promise<void> | undefined
  let outputWatchdogTimer: ReturnType<typeof setTimeout> | undefined
  const terminate = () => {
    termination ??= ensureProcessGroupTerminated(pid)
    return termination
  }
  const timeoutTimer = setTimeout(() => {
    timedOut = true
    void terminate().catch((error) => {
      cleanupError = messageOf(error)
    })
  }, options.timeoutMs)
  const watchdogTimer = setTimeout(() => {
    if (!timedOut) return
    child.kill("SIGKILL")
    child.stdout?.destroy()
    child.stderr?.destroy()
  }, options.timeoutMs + PROCESS_GROUP_WATCHDOG_MS)

  try {
    const result = await childExit
    if (!timedOut) clearTimeout(timeoutTimer)
    outputWatchdogTimer = setTimeout(() => {
      child.stdout?.destroy()
      child.stderr?.destroy()
    }, PROCESS_GROUP_WATCHDOG_MS)
    try {
      await terminate()
    } catch (error) {
      cleanupError = messageOf(error)
    }
    return {
      command,
      exitCode: result.exitCode,
      signal: result.signal,
      stdout: await stdout,
      stderr: [
        await stderr,
        result.error ? messageOf(result.error) : "",
        timedOut ? `process timed out after ${options.timeoutMs}ms` : "",
        cleanupError ? `process group cleanup failed: ${cleanupError}` : "",
      ]
        .filter(Boolean)
        .join("\n"),
      timedOut,
      cleanupError,
    }
  } finally {
    clearTimeout(timeoutTimer)
    clearTimeout(watchdogTimer)
    if (outputWatchdogTimer) clearTimeout(outputWatchdogTimer)
    if (cleanupError === undefined) activeProcessGroups.delete(pid)
  }
}

function inheritedEnvironment(overrides: Record<string, string> = {}) {
  const env = { ...Bun.env, ...overrides }
  delete env.OPENCODE_RELEASE
  delete env.NODE_OPTIONS
  delete env.BUN_OPTIONS
  return env
}

function buildEnvironment(origin: string, version: string, modelsSnapshot: string) {
  const env = inheritedEnvironment()
  delete env.OPENCODE_BUMP
  delete env.OPENCODE_CONFIG
  delete env.OPENCODE_CONFIG_DIR
  delete env.OPENCODE_CONFIG_CONTENT
  delete env.OPENCODE_TEST_HOME
  delete env.OPENCODE_FORK_TEST_ORIGIN
  delete env.OPENCODE_FORK_RELEASE
  delete env.OPENCODE_UPSTREAM_BUILD
  delete env.OPENCODE_CHANNEL
  delete env.OPENCODE_VERSION
  return {
    ...env,
    OPENCODE_FORK_RELEASE: "1",
    OPENCODE_CHANNEL: "latest",
    OPENCODE_VERSION: version,
    OPENCODE_FORK_TEST_ORIGIN: origin,
    MODELS_DEV_API_JSON: modelsSnapshot,
  }
}

async function prepareShims(root: string) {
  // Keep package-manager probes local so a policy failure can never reach the host package manager.
  const directory = path.join(root, "package-manager-shims")
  const log = path.join(root, "package-manager-calls.log")
  await mkdir(directory, { recursive: true })
  await writeFile(log, "")
  const script = ["#!/bin/sh", 'printf \'%s\\n\' "$0 $*" >> "$OPENCODE_FORK_SHIM_LOG"', "exit 127", ""].join("\n")
  for (const name of ["npm", "yarn", "pnpm", "bun", "brew", "scoop", "choco"]) {
    const shim = path.join(directory, name)
    await writeFile(shim, script)
    await chmod(shim, 0o755)
  }
  return { directory, log }
}

async function runtimeEnvironment(root: string, shims: { directory: string; log: string }) {
  const env = inheritedEnvironment()
  delete env.OPENCODE_FORK_TEST_ORIGIN
  delete env.OPENCODE_FORK_RELEASE
  delete env.OPENCODE_UPSTREAM_BUILD
  delete env.OPENCODE_RELEASE
  delete env.OPENCODE_VERSION
  delete env.OPENCODE_CHANNEL
  delete env.OPENCODE_BUMP
  delete env.OPENCODE_CONFIG
  delete env.OPENCODE_CONFIG_DIR
  delete env.OPENCODE_CONFIG_CONTENT
  delete env.OPENCODE_TEST_HOME
  delete env.OPENCODE_DISABLE_AUTOUPDATE
  delete env.OPENCODE_ALWAYS_NOTIFY_UPDATE
  const home = path.join(root, "home")
  const config = path.join(root, "xdg-config")
  const data = path.join(root, "xdg-data")
  const cache = path.join(root, "xdg-cache")
  const state = path.join(root, "xdg-state")
  const temp = path.join(root, "tmp")
  await Promise.all([home, config, data, cache, state, temp].map((directory) => mkdir(directory, { recursive: true })))
  return {
    ...env,
    HOME: home,
    OPENCODE_TEST_HOME: home,
    XDG_CONFIG_HOME: config,
    XDG_DATA_HOME: data,
    XDG_CACHE_HOME: cache,
    XDG_STATE_HOME: state,
    TMPDIR: temp,
    TMP: temp,
    TEMP: temp,
    PATH: `${shims.directory}${path.delimiter}${env.PATH ?? ""}`,
    OPENCODE_CONFIG_CONTENT: JSON.stringify({ autoupdate: false }),
    OPENCODE_PURE: "1",
    OPENCODE_FORK_SHIM_LOG: shims.log,
  }
}

function makeReleasePaths(archiveName: string, checksumName: string): ReleasePaths {
  return {
    // The updater discovers the latest release through the paginated list
    // endpoint; the fixture pins the exact expected query so wrong discovery
    // shows up as an unknown fixture route.
    latest: `/repos/totalolage/opencode/releases?per_page=100&page=1`,
    tag: `/repos/totalolage/opencode/releases/tags/v${NEW_VERSION}`,
    archive: `/totalolage/opencode/releases/download/v${NEW_VERSION}/${archiveName}`,
    checksums: `/totalolage/opencode/releases/download/v${NEW_VERSION}/${checksumName}`,
  }
}

function fullPath(origin: string, pathname: string) {
  return `${origin}${pathname}`
}

function fixtureUser(origin: string, nodeId: string) {
  return {
    login: "fork-fixture",
    id: 123,
    node_id: nodeId,
    avatar_url: `${origin}/avatars/fixture.png`,
    gravatar_id: "",
    url: `${origin}/users/fork-fixture`,
    html_url: `${origin}/fork-fixture`,
    followers_url: `${origin}/users/fork-fixture/followers`,
    following_url: `${origin}/users/fork-fixture/following{/other_user}`,
    gists_url: `${origin}/users/fork-fixture/gists{/gist_id}`,
    starred_url: `${origin}/users/fork-fixture/starred{/owner}{/repo}`,
    subscriptions_url: `${origin}/users/fork-fixture/subscriptions`,
    organizations_url: `${origin}/users/fork-fixture/orgs`,
    repos_url: `${origin}/users/fork-fixture/repos`,
    events_url: `${origin}/users/fork-fixture/events{/privacy}`,
    received_events_url: `${origin}/users/fork-fixture/received_events`,
    type: "User",
    site_admin: false,
  }
}

function releaseMetadata(fixture: Fixture) {
  if (!fixture.release) throw new Error("release fixture is not ready")
  const release = fixture.release
  const archiveUrl = fullPath(fixture.origin, fixture.paths.archive)
  const checksumUrl = fullPath(fixture.origin, fixture.paths.checksums)
  const asset = (name: string, url: string, size: number, contentType: string, id: number, digest: string) => ({
    url: `${fixture.origin}/repos/totalolage/opencode/releases/123456/assets/${id}`,
    id,
    node_id: `fixture-asset-${id}`,
    name,
    label: null,
    uploader: fixtureUser(fixture.origin, "fixture-uploader"),
    content_type: contentType,
    state: "uploaded",
    size,
    digest: `sha256:${digest}`,
    download_count: 0,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    browser_download_url: url,
  })
  return {
    url: fullPath(fixture.origin, fixture.paths.tag),
    assets_url: `${fixture.origin}/repos/totalolage/opencode/releases/123456/assets`,
    upload_url: `${fixture.origin}/repos/totalolage/opencode/releases/123456/assets{?name,label}`,
    html_url: `${fixture.origin}/totalolage/opencode/releases/tag/v${NEW_VERSION}`,
    id: 123456,
    author: fixtureUser(fixture.origin, "fixture-author"),
    node_id: "fixture-release",
    tag_name: fixture.mode === "malformed-metadata" ? "not-a-version" : `v${NEW_VERSION}`,
    target_commitish: "dev",
    name: `v${NEW_VERSION}`,
    draft: false,
    // Timestamped fork releases are published as prereleases; the updater
    // requires prerelease === isTimestamped(version) for eligibility.
    prerelease: true,
    created_at: "2026-01-01T00:00:00Z",
    published_at: "2026-01-01T00:00:00Z",
    tarball_url: `${fixture.origin}/totalolage/opencode/archive/refs/tags/v${NEW_VERSION}.tar.gz`,
    zipball_url: `${fixture.origin}/totalolage/opencode/archive/refs/tags/v${NEW_VERSION}.zip`,
    body: "fork upgrade fixture",
    assets: [
      asset(
        release.archiveName,
        archiveUrl,
        release.archiveBytes.byteLength,
        release.archiveName.endsWith(".zip") ? "application/zip" : "application/gzip",
        1,
        release.archiveHash,
      ),
      asset(
        release.checksumName,
        checksumUrl,
        release.checksumBytes.byteLength,
        "text/plain",
        2,
        sha256(release.checksumBytes),
      ),
    ],
  }
}

function jsonResponse(value: unknown, status = 200) {
  const body = JSON.stringify(value)
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "application/json",
      "Content-Length": String(new TextEncoder().encode(body).byteLength),
    },
  })
}

function textResponse(body: string, status: number) {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain", "Content-Length": String(new TextEncoder().encode(body).byteLength) },
  })
}

function binaryResponse(bytes: Uint8Array, headers: Record<string, string>) {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    },
  })
  return new Response(body, { status: 200, headers })
}

function recordRequest(fixture: Fixture, request: Request, url: URL, status: number, known: boolean) {
  fixture.requests.push({
    scenario: fixture.scenario,
    method: request.method,
    url: request.url,
    path: `${url.pathname}${url.search}`,
    status,
    known,
  })
}

function serveFixture(request: Request, fixture: Fixture) {
  const url = new URL(request.url)
  const knownPath = [fixture.paths.latest, fixture.paths.tag, fixture.paths.archive, fixture.paths.checksums].includes(
    `${url.pathname}${url.search}`,
  )
  const known = request.method === "GET" && url.origin === fixture.origin && knownPath

  if (!known) {
    recordRequest(fixture, request, url, 404, false)
    return textResponse(`unknown fixture route: ${request.method} ${url.pathname}${url.search}\n`, 404)
  }

  if (`${url.pathname}${url.search}` === fixture.paths.latest || url.pathname === fixture.paths.tag) {
    if (fixture.mode === "missing-release") {
      recordRequest(fixture, request, url, 404, true)
      return textResponse("release not found\n", 404)
    }
    if (!fixture.release) {
      recordRequest(fixture, request, url, 503, true)
      return textResponse("release fixture is not ready\n", 503)
    }
    recordRequest(fixture, request, url, 200, true)
    if (url.pathname === fixture.paths.tag) return jsonResponse(releaseMetadata(fixture))
    // The list endpoint returns a single-entry page; a page smaller than the
    // updater's page size ends pagination after this request.
    return jsonResponse([releaseMetadata(fixture)])
  }

  if (fixture.mode === "missing-release" || !fixture.release) {
    recordRequest(fixture, request, url, 404, true)
    return textResponse("release asset not found\n", 404)
  }

  const release = fixture.release
  if (url.pathname === fixture.paths.archive) {
    const bytes =
      fixture.mode === "truncated-download"
        ? release.archiveBytes.slice(0, Math.max(0, Math.floor((release.archiveBytes.byteLength - 1) / 2)))
        : release.archiveBytes
    recordRequest(fixture, request, url, 200, true)
    return binaryResponse(bytes, {
      "Content-Type": release.archiveName.endsWith(".zip") ? "application/zip" : "application/gzip",
      "Content-Length": String(release.archiveBytes.byteLength),
    })
  }

  const checksum =
    fixture.mode === "checksum-mismatch"
      ? `${"0".repeat(64)}  ${release.archiveName}\n`
      : new TextDecoder().decode(release.checksumBytes)
  const checksumBytes = new TextEncoder().encode(checksum)
  recordRequest(fixture, request, url, 200, true)
  return binaryResponse(checksumBytes, {
    "Content-Type": "text/plain",
    "Content-Length": String(checksumBytes.byteLength),
  })
}

function validateReleasePaths(fixture: Fixture) {
  const allowed = new Set(Object.values(fixture.paths))
  const metadata = releaseMetadata(fixture)
  for (const asset of metadata.assets) {
    const url = new URL(asset.browser_download_url)
    if (url.origin !== fixture.origin || url.search !== "" || !allowed.has(url.pathname)) {
      throw new Error(`release asset escaped the fixed fork: ${asset.browser_download_url}`)
    }
  }
}

async function buildVersion(
  packageDir: string,
  root: string,
  origin: string,
  version: string,
  binaryPath: string,
  modelsSnapshot: string,
  runtimeEnv: Record<string, string>,
  evidence: Evidence,
) {
  const command = [
    process.execPath,
    "run",
    "script/build.ts",
    "--single",
    "--skip-install",
    "--skip-embed-web-ui",
    "--fork-update-test",
    ...(process.arch === "x64" ? ["--baseline"] : []),
  ]
  const result = await runCommand(command, {
    cwd: packageDir,
    env: buildEnvironment(origin, version, modelsSnapshot),
    timeoutMs: BUILD_TIMEOUT_MS,
  })
  throwIfInterrupted()
  const build: BuildEvidence = {
    version,
    command,
    binaryPath,
    exitCode: result.exitCode,
    signal: result.signal,
    timedOut: result.timedOut,
  }
  evidence.builds.push(build)
  if (!commandSucceeded(result)) {
    evidence.errors.push(commandFailure(`build:${version}`, result, `build for ${version} failed`))
    throw new Error(`build for ${version} failed`)
  }

  const target = `opencode-${process.platform}-${process.arch}${process.arch === "x64" ? "-baseline" : ""}`
  const source = path.join(packageDir, "dist", target, "bin", "opencode")
  if (!(await Bun.file(source).exists())) {
    throw new Error(`build for ${version} did not produce ${source}`)
  }
  await mkdir(path.dirname(binaryPath), { recursive: true })
  await copyFile(source, binaryPath)
  await chmod(binaryPath, 0o755)
  const hash = await hashFile(binaryPath)
  build.hash = hash

  const versionResult = await runCommand([binaryPath, "--version"], {
    cwd: root,
    env: runtimeEnv,
    timeoutMs: VERSION_TIMEOUT_MS,
  })
  if (!commandSucceeded(versionResult) || versionResult.stdout.trim() !== version) {
    evidence.errors.push(
      commandFailure(
        `version:${version}`,
        versionResult,
        `compiled ${version} binary did not report its actual version`,
      ),
    )
    throw new Error(`compiled ${version} binary did not report its actual version`)
  }
  return { version, path: binaryPath, hash } satisfies BuildArtifact
}

async function packageRelease(
  root: string,
  newArtifact: BuildArtifact,
  archiveName: string,
  checksumName: string,
  runtimeEnv: Record<string, string>,
  evidence: Evidence,
) {
  const releaseDir = path.join(root, "release")
  const packageDir = path.join(releaseDir, "package")
  const packageBinary = path.join(packageDir, "opencode")
  const archivePath = path.join(releaseDir, archiveName)
  const checksumsPath = path.join(releaseDir, checksumName)
  await mkdir(packageDir, { recursive: true })
  await copyFile(newArtifact.path, packageBinary)
  await chmod(packageBinary, 0o755)

  const packageCommand =
    process.platform === "linux"
      ? ["tar", "-czf", archivePath, "-C", packageDir, "opencode"]
      : ["zip", "-q", "-j", archivePath, "opencode"]
  const packageResult = await runCommand(packageCommand, {
    cwd: packageDir,
    env: runtimeEnv,
    timeoutMs: COMMAND_TIMEOUT_MS,
  })
  throwIfInterrupted()
  if (!commandSucceeded(packageResult)) {
    evidence.errors.push(commandFailure("package", packageResult, "release archive packaging failed"))
    throw new Error("release archive packaging failed")
  }

  const archiveHash = await hashFile(archivePath)
  const checksumLine = `${archiveHash}  ${archiveName}\n`
  await writeFile(checksumsPath, checksumLine)
  const archiveBytes = new Uint8Array(await Bun.file(archivePath).arrayBuffer())
  const checksumBytes = new TextEncoder().encode(checksumLine)

  const extractDir = path.join(releaseDir, "verify-extract")
  await mkdir(extractDir, { recursive: true })
  const verifyCommand =
    process.platform === "linux"
      ? ["tar", "-xzf", archivePath, "-C", extractDir]
      : ["unzip", "-q", archivePath, "-d", extractDir]
  const verifyResult = await runCommand(verifyCommand, {
    cwd: root,
    env: runtimeEnv,
    timeoutMs: COMMAND_TIMEOUT_MS,
  })
  throwIfInterrupted()
  if (!commandSucceeded(verifyResult)) {
    evidence.errors.push(commandFailure("package-verify", verifyResult, "release archive could not be extracted"))
    throw new Error("release archive could not be extracted")
  }
  const extractedBinary = path.join(extractDir, "opencode")
  if (!(await Bun.file(extractedBinary).exists()) || (await hashFile(extractedBinary)) !== newArtifact.hash) {
    throw new Error("release archive did not contain the new compiled binary")
  }

  evidence.hashes.archive = archiveHash
  return { archivePath, checksumsPath, archiveBytes, checksumBytes, archiveHash }
}

async function resetInstallation(ctx: RuntimeContext, executable: string) {
  const parent = path.dirname(executable)
  await mkdir(parent, { recursive: true })
  await chmod(parent, 0o755)
  if (await Bun.file(executable).exists()) await chmod(executable, 0o755)
  await rm(executable, { force: true })
  await copyFile(ctx.oldArtifact.path, executable)
  await chmod(executable, 0o755)
}

async function readVersion(ctx: RuntimeContext, executable: string) {
  const result = await runCommand([executable, "--version"], {
    cwd: ctx.root,
    env: ctx.runtimeEnv,
    timeoutMs: VERSION_TIMEOUT_MS,
  })
  return { result, value: result.stdout.trim() }
}

async function packageManagerCalls(log: string) {
  const text = await Bun.file(log).text()
  return text.split(/\r?\n/).filter(Boolean)
}

function assertRoutes(scenario: ScenarioDefinition, requests: FixtureRequest[]) {
  const unknown = requests.find((request) => !request.known)
  if (unknown) throw new Error(`unknown fixture route: ${unknown.method} ${unknown.url}`)
  for (const expected of scenario.requiredRoutes) {
    const request = requests.find((candidate) => candidate.path === expected.path)
    if (!request) throw new Error(`required fixture route was not requested: ${expected.path}`)
    if (request.status !== expected.status) {
      throw new Error(`fixture route ${expected.path} returned ${request.status}, expected ${expected.status}`)
    }
  }
}

async function runScenario(ctx: RuntimeContext, scenario: ScenarioDefinition) {
  const executable = scenario.executable === "installed" ? ctx.installedPath : ctx.unknownPath
  const command = [executable, ...scenario.args]
  const record: ScenarioEvidence = {
    name: scenario.name,
    mode: scenario.mode,
    status: "failed",
    executable,
    command,
    expected: scenario.expectSuccess ? "success" : "failure",
    expectedVersion: scenario.expectedVersion,
    requests: [],
    packageManagerCalls: [],
  }
  const start = ctx.fixture.requests.length

  ctx.fixture.mode = scenario.mode
  ctx.fixture.scenario = scenario.name
  let result: CommandResult | undefined
  let restorePermissions: (() => Promise<void>) | undefined

  try {
    await resetInstallation(ctx, executable)
    const beforeHash = await hashFile(executable)
    const beforeVersion = await readVersion(ctx, executable)
    if (beforeHash !== ctx.oldArtifact.hash) {
      throw new Error("restored baseline binary hash does not match the built baseline")
    }
    if (!commandSucceeded(beforeVersion.result) || beforeVersion.value !== ctx.oldVersion) {
      ctx.evidence.errors.push(
        commandFailure(
          `scenario:${scenario.name}:before-version`,
          beforeVersion.result,
          "old binary did not report the baseline version",
        ),
      )
      throw new Error("old binary did not report the baseline version")
    }
    record.before = { version: beforeVersion.value, hash: beforeHash }

    if (scenario.permission === "parent-unwritable") {
      const parent = path.dirname(executable)
      const originalMode = (await stat(parent)).mode & 0o777
      await chmod(parent, originalMode & ~0o222)
      restorePermissions = async () => chmod(parent, originalMode)
    }
    if (scenario.permission === "binary-read-only") {
      const originalMode = (await stat(executable)).mode & 0o777
      await chmod(executable, originalMode & ~0o222)
      restorePermissions = async () => chmod(executable, originalMode)
    }

    result = await runCommand(command, {
      cwd: ctx.root,
      env: ctx.runtimeEnv,
      timeoutMs: COMMAND_TIMEOUT_MS,
    })
    record.cleanupError = result.cleanupError
    requireCommandCleanup(result)
    throwIfInterrupted()
    record.exitCode = result.exitCode
    record.signal = result.signal
    record.timedOut = result.timedOut
    record.stdout = result.stdout
    record.stderr = result.stderr
    const succeeded = commandSucceeded(result)
    const output = `${result.stdout}\n${result.stderr}`
    const diagnosticMatched = scenario.diagnostic?.test(output) ?? true
    const nonzeroFailure = result.exitCode !== null && result.exitCode !== 0
    const requiresNonzero = scenario.requiresNonzero ?? !scenario.expectSuccess
    const statusError = result.timedOut
      ? "scenario command timed out"
      : scenario.expectSuccess && !succeeded
        ? "successful upgrade exited unsuccessfully"
        : requiresNonzero && !nonzeroFailure
          ? "negative upgrade scenario did not fail with a nonzero exit code"
          : scenario.diagnostic && !diagnosticMatched
            ? "scenario output did not contain the expected diagnostic"
            : undefined
    if (statusError) ctx.evidence.errors.push(commandFailure(`scenario:${scenario.name}`, result, statusError))
    const calls = await packageManagerCalls(ctx.shimLog)
    if (calls.length > 0) {
      const message = `fork upgrade invoked a package manager: ${calls.join(", ")}`
      ctx.evidence.errors.push({
        stage: `scenario:${scenario.name}:package-manager`,
        message,
      })
      throw new Error(message)
    }

    const afterHash = await hashFile(executable)
    const afterVersion = await readVersion(ctx, executable)
    record.after = { hash: afterHash, version: afterVersion.value }
    const afterVersionFailed = !commandSucceeded(afterVersion.result)
    if (afterVersionFailed) {
      ctx.evidence.errors.push(
        commandFailure(
          `scenario:${scenario.name}:after-version`,
          afterVersion.result,
          "updated binary could not report its version",
        ),
      )
    }
    if (scenario.expectSuccess && afterHash !== ctx.newArtifact.hash) {
      throw new Error("successful upgrade did not install the new binary")
    }
    if (!scenario.expectSuccess && afterHash !== beforeHash) {
      throw new Error("failed upgrade changed the installed binary")
    }
    if (afterVersionFailed) {
      throw new Error("updated binary could not report its version")
    }
    if (afterVersion.value !== scenario.expectedVersion) {
      ctx.evidence.errors.push(
        commandFailure(
          `scenario:${scenario.name}:after-version`,
          afterVersion.result,
          `expected version ${scenario.expectedVersion}, got ${afterVersion.value}`,
        ),
      )
      throw new Error(`expected version ${scenario.expectedVersion}, got ${afterVersion.value}`)
    }

    if (statusError) throw new Error(statusError)
    record.packageManagerCalls = await packageManagerCalls(ctx.shimLog)
    record.status = "passed"
  } catch (error) {
    record.status = "failed"
    record.message = messageOf(error)
    if (result && !ctx.evidence.errors.some((entry) => entry.stage === `scenario:${scenario.name}`)) {
      ctx.evidence.errors.push(commandFailure(`scenario:${scenario.name}`, result, "scenario assertions failed"))
    }
  } finally {
    if (restorePermissions) {
      try {
        await restorePermissions()
      } catch (error) {
        record.status = "failed"
        record.message = `${record.message ? `${record.message}; ` : ""}could not restore permissions: ${messageOf(error)}`
      }
    }
    record.requests = ctx.fixture.requests.slice(start)
    record.packageManagerCalls = await packageManagerCalls(ctx.shimLog)
    try {
      assertRoutes(scenario, record.requests)
    } catch (error) {
      record.status = "failed"
      record.message = `${record.message ? `${record.message}; ` : ""}${messageOf(error)}`
      if (result && !ctx.evidence.errors.some((entry) => entry.stage === `scenario:${scenario.name}`)) {
        ctx.evidence.errors.push(
          commandFailure(`scenario:${scenario.name}`, result, "scenario route assertions failed"),
        )
      }
      ctx.evidence.errors.push({ stage: `scenario:${scenario.name}:routes`, message: messageOf(error) })
    }
  }

  return record
}

function scenarioDefinitions(paths: ReleasePaths): ScenarioDefinition[] {
  const successfulRoutes = [
    { path: paths.latest, status: 200 },
    { path: paths.archive, status: 200 },
    { path: paths.checksums, status: 200 },
  ]
  return [
    {
      name: "latest-success",
      mode: "success",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: true,
      expectedVersion: NEW_VERSION,
      requiredRoutes: successfulRoutes,
    },
    {
      name: "explicit-version-success",
      mode: "success",
      args: ["upgrade", NEW_VERSION],
      executable: "installed",
      expectSuccess: true,
      expectedVersion: NEW_VERSION,
      requiredRoutes: [
        { path: paths.tag, status: 200 },
        { path: paths.archive, status: 200 },
        { path: paths.checksums, status: 200 },
      ],
    },
    {
      name: "checksum-mismatch",
      mode: "checksum-mismatch",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: successfulRoutes,
      diagnostic: CHECKSUM_MISMATCH_DIAGNOSTIC,
    },
    {
      name: "truncated-download",
      mode: "truncated-download",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: successfulRoutes,
      diagnostic: TRUNCATED_DOWNLOAD_DIAGNOSTIC,
    },
    {
      name: "missing-release-404",
      mode: "missing-release",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [{ path: paths.latest, status: 404 }],
      diagnostic: MISSING_RELEASE_DIAGNOSTIC,
    },
    {
      name: "malformed-metadata",
      mode: "malformed-metadata",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [{ path: paths.latest, status: 200 }],
      diagnostic: INVALID_RELEASE_DIAGNOSTIC,
    },
    {
      name: "unsupported-method-npm",
      mode: "success",
      args: ["upgrade", NEW_VERSION, "--method", "npm"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [],
      diagnostic: ACTIONABLE_FORK_DIAGNOSTIC,
    },
    {
      name: "unknown-install-location",
      mode: "success",
      args: ["upgrade"],
      executable: "unknown",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [],
      diagnostic: ACTIONABLE_FORK_DIAGNOSTIC,
    },
    {
      name: "permission-parent-unwritable",
      mode: "success",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [],
      diagnostic: PERMISSION_DIAGNOSTIC,
      permission: "parent-unwritable",
    },
    {
      name: "permission-binary-read-only",
      mode: "success",
      args: ["upgrade"],
      executable: "installed",
      expectSuccess: false,
      expectedVersion: OLD_VERSION,
      requiredRoutes: [],
      diagnostic: PERMISSION_DIAGNOSTIC,
      permission: "binary-read-only",
    },
  ]
}

async function main() {
  const keep = process.argv.includes("--keep")
  const evidence: Evidence = {
    tool: "verify-fork-upgrade",
    ok: false,
    platform: {
      os: process.platform,
      arch: process.arch,
      libc: process.platform === "linux" && runningOnGlibc() ? "glibc" : undefined,
      baseline: process.arch === "x64",
    },
    versions: { old: OLD_VERSION, new: NEW_VERSION },
    hashes: {},
    builds: [],
    scenarios: [],
    requestRoutes: [],
    errors: [],
  }
  let root: string | undefined
  let stopServer: (() => Promise<void>) | undefined
  let removeSignalHandlers: (() => void) | undefined
  let fixture: Fixture | undefined
  let failed = false

  try {
    if (process.platform !== "linux" && process.platform !== "darwin") {
      throw new Error(`unsupported platform ${process.platform}; only Linux glibc and macOS are supported`)
    }
    if (!runningOnGlibc()) throw new Error("unsupported Linux libc; only glibc is supported")
    if (process.arch !== "x64" && process.arch !== "arm64") {
      throw new Error(`unsupported architecture ${process.arch}; only x64 and arm64 are supported`)
    }
    if (typeof process.getuid === "function" && process.getuid() === 0) {
      throw new Error("fork upgrade verification must run as a non-root user so permission scenarios are meaningful")
    }
    if (ForkVersion.parse(OLD_VERSION) !== OLD_VERSION || ForkVersion.parse(NEW_VERSION) !== NEW_VERSION) {
      throw new Error("verification versions must be supported fork release versions")
    }

    removeSignalHandlers = installSignalHandlers()
    root = await mkdtemp(path.join(os.tmpdir(), "opencode-fork-upgrade-"))
    const packageDir = path.resolve(import.meta.dir, "..")
    const archiveName =
      process.platform === "linux" ? `opencode-linux-${process.arch}.tar.gz` : `opencode-darwin-${process.arch}.zip`
    const checksumName = "SHA256SUMS"
    fixture = {
      origin: "",
      mode: "success",
      scenario: "setup",
      paths: { latest: "", tag: "", archive: "", checksums: "" },
      requests: [],
    }
    const serverFixture = fixture
    const server = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: (request) => serveFixture(request, serverFixture),
    })
    stopServer = async () => {
      await server.stop(true)
    }
    fixture.origin = `http://127.0.0.1:${server.port}`
    fixture.paths = makeReleasePaths(archiveName, checksumName)

    const shims = await prepareShims(root)
    // build.ts otherwise fetches models.dev while importing its generator.
    const modelsSnapshot = path.join(root, "models.dev.json")
    await writeFile(modelsSnapshot, "{}\n")
    const runtimeEnv = await runtimeEnvironment(root, shims)
    const oldPath = path.join(root, "built", `opencode-${OLD_VERSION}`)
    const newPath = path.join(root, "built", `opencode-${NEW_VERSION}`)
    const oldArtifact = await buildVersion(
      packageDir,
      root,
      fixture.origin,
      OLD_VERSION,
      oldPath,
      modelsSnapshot,
      runtimeEnv,
      evidence,
    )
    const newArtifact = await buildVersion(
      packageDir,
      root,
      fixture.origin,
      NEW_VERSION,
      newPath,
      modelsSnapshot,
      runtimeEnv,
      evidence,
    )
    evidence.hashes.oldBinary = oldArtifact.hash
    evidence.hashes.newBinary = newArtifact.hash

    const packaged = await packageRelease(root, newArtifact, archiveName, checksumName, runtimeEnv, evidence)
    fixture.release = {
      archiveBytes: packaged.archiveBytes,
      archiveHash: packaged.archiveHash,
      checksumBytes: packaged.checksumBytes,
      archiveName,
      checksumName,
    }
    validateReleasePaths(fixture)
    evidence.release = {
      origin: fixture.origin,
      archiveName,
      checksumName,
      paths: fixture.paths,
      archivePath: packaged.archivePath,
      checksumsPath: packaged.checksumsPath,
    }

    if (fixture.requests.length > 0) throw new Error("fixture received a release request during build or packaging")
    throwIfInterrupted()

    const context: RuntimeContext = {
      root,
      oldVersion: OLD_VERSION,
      oldArtifact,
      newArtifact,
      installedPath: path.join(root, "home", ".opencode", "bin", "opencode"),
      unknownPath: path.join(root, "unmanaged", "opencode"),
      runtimeEnv,
      shimLog: shims.log,
      fixture,
      evidence,
    }
    for (const scenario of scenarioDefinitions(fixture.paths)) {
      throwIfInterrupted()
      await writeFile(shims.log, "")
      const result = await runScenario(context, scenario)
      evidence.scenarios.push(result)
      if (result.cleanupError) throw new Error(`scenario process cleanup failed: ${result.cleanupError}`)
    }

    if (evidence.scenarios.some((scenario) => scenario.status === "failed")) {
      throw new Error("one or more fork upgrade scenarios failed")
    }
    throwIfInterrupted()
    if (fixture.requests.some((request) => !request.known)) {
      throw new Error("fixture received an unknown route")
    }
  } catch (error) {
    failed = true
    evidence.errors.push({ stage: "fatal", message: messageOf(error) })
  } finally {
    if (fixture) evidence.requestRoutes = fixture.requests
    let processGroupCleanupError: unknown
    if (interruptionCleanup) await interruptionCleanup
    try {
      await terminateActiveProcessGroups()
    } catch (error) {
      processGroupCleanupError = error
    }
    if (interruptionCleanupError) {
      processGroupCleanupError = processGroupCleanupError
        ? `${messageOf(processGroupCleanupError)}; ${messageOf(interruptionCleanupError)}`
        : interruptionCleanupError
    }
    if (processGroupCleanupError) {
      failed = true
      evidence.errors.push({ stage: "cleanup:process-groups", message: messageOf(processGroupCleanupError) })
    }
    if (removeSignalHandlers) removeSignalHandlers()
    if (stopServer) {
      try {
        await stopServer()
      } catch (error) {
        failed = true
        evidence.errors.push({ stage: "cleanup:server", message: messageOf(error) })
      }
    }
    if (root && keep) evidence.artifactsRoot = root
    if (root && !keep) {
      try {
        await rm(root, { recursive: true, force: true })
      } catch (error) {
        failed = true
        evidence.errors.push({ stage: "cleanup:artifacts", message: messageOf(error) })
      }
    }
  }

  evidence.ok =
    !failed && interruptedBy === undefined && evidence.scenarios.every((scenario) => scenario.status === "passed")
  process.stdout.write(`${JSON.stringify(evidence, null, 2)}\n`)
  if (!evidence.ok) process.exitCode = 1
}

if (import.meta.main) await main()

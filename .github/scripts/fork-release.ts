#!/usr/bin/env bun

import { mkdir, rm, stat } from "node:fs/promises"
import path from "node:path"
import { parse } from "../../packages/script/src/version"

const repoRoot = path.resolve(import.meta.dir, "../..")

export const RELEASE_TARGETS = [
  {
    platform: "linux",
    arch: "x64",
    binaryDirectory: "opencode-linux-x64-baseline",
    archiveName: "opencode-linux-x64.tar.gz",
    archiveFormat: "tar.gz",
  },
  {
    platform: "linux",
    arch: "arm64",
    binaryDirectory: "opencode-linux-arm64",
    archiveName: "opencode-linux-arm64.tar.gz",
    archiveFormat: "tar.gz",
  },
  {
    platform: "darwin",
    arch: "x64",
    binaryDirectory: "opencode-darwin-x64-baseline",
    archiveName: "opencode-darwin-x64.zip",
    archiveFormat: "zip",
  },
  {
    platform: "darwin",
    arch: "arm64",
    binaryDirectory: "opencode-darwin-arm64",
    archiveName: "opencode-darwin-arm64.zip",
    archiveFormat: "zip",
  },
] as const

export const CHECKSUMS_FILENAME = "SHA256SUMS"

export type ReleasePlatform = (typeof RELEASE_TARGETS)[number]["platform"]
export type ReleaseArch = (typeof RELEASE_TARGETS)[number]["arch"]
export type ReleaseTarget = (typeof RELEASE_TARGETS)[number]

export type PackageReleaseOptions = {
  version: string
  platform: ReleasePlatform
  arch: ReleaseArch
  distDirectory: string
  outputDirectory: string
}

export function validateVersion(version: string) {
  // parse() accepts a single leading "v" by normalizing it away, so the strict
  // round-trip here rejects it: release and build inputs have no prefix.
  if (parse(version) !== version) {
    throw new Error("version must be a stable X.Y.Z or X.Y.Z-f8y-<UTC timestamp> version")
  }
  return version
}

export function getReleaseTarget(platform: string, arch: string) {
  const target = RELEASE_TARGETS.find((item) => item.platform === platform && item.arch === arch)
  if (!target) throw new Error(`unsupported fork release target: ${platform}/${arch}`)
  return target
}

export async function packageRelease(options: PackageReleaseOptions) {
  const version = validateVersion(options.version)
  const target = getReleaseTarget(options.platform, options.arch)
  const binaryPath = path.resolve(options.distDirectory, target.binaryDirectory, "bin", "opencode")
  const outputDirectory = path.resolve(options.outputDirectory)
  const archivePath = path.join(outputDirectory, target.archiveName)

  await verifyExecutable(binaryPath)
  await smokeTest(binaryPath, version)
  await mkdir(outputDirectory, { recursive: true })
  await rm(archivePath, { force: true })
  await createArchive(binaryPath, archivePath, target.archiveFormat)
  await verifyArchive(archivePath, target.archiveFormat)
  return archivePath
}

export async function sha256File(filePath: string) {
  const hasher = new Bun.CryptoHasher("sha256")
  hasher.update(await Bun.file(filePath).arrayBuffer())
  return hasher.digest("hex")
}

export async function writeSha256Sums(directory: string) {
  const releaseDirectory = path.resolve(directory)
  await requireReleaseArchives(releaseDirectory)
  const lines = await Promise.all(
    [...RELEASE_TARGETS]
      .sort((left, right) => left.archiveName.localeCompare(right.archiveName))
      .map(
        async (target) => `${await sha256File(path.join(releaseDirectory, target.archiveName))}  ${target.archiveName}`,
      ),
  )
  const contents = `${lines.join("\n")}\n`
  await Bun.write(path.join(releaseDirectory, CHECKSUMS_FILENAME), contents)
  return contents
}

export async function verifyReleaseArtifacts(directory: string) {
  const releaseDirectory = path.resolve(directory)
  const files = await requireReleaseArchives(releaseDirectory)
  const sumsPath = path.join(releaseDirectory, CHECKSUMS_FILENAME)
  if (!files.includes(CHECKSUMS_FILENAME)) throw new Error(`${CHECKSUMS_FILENAME} is required`)

  const sums = parseSha256Sums(await Bun.file(sumsPath).text())
  await Promise.all(
    RELEASE_TARGETS.map(async (target) => {
      const archivePath = path.join(releaseDirectory, target.archiveName)
      await verifyArchive(archivePath, target.archiveFormat)
      const expected = sums.get(target.archiveName)
      if (expected === undefined) throw new Error(`missing checksum for ${target.archiveName}`)
      const actual = await sha256File(archivePath)
      if (actual !== expected) throw new Error(`checksum mismatch for ${target.archiveName}: ${actual}`)
    }),
  )
}

async function verifyExecutable(binaryPath: string) {
  const info = await stat(binaryPath)
  if (!info.isFile()) throw new Error(`binary is not a file: ${binaryPath}`)
  if ((info.mode & 0o111) === 0) throw new Error(`binary is not executable: ${binaryPath}`)
}

async function smokeTest(binaryPath: string, version: string) {
  const result = await run([binaryPath, "--version"])
  const actual = result.stdout.trim()
  if (actual !== version) throw new Error(`version smoke test failed: expected ${version}, got ${actual}`)
}

async function createArchive(binaryPath: string, archivePath: string, archiveFormat: ReleaseTarget["archiveFormat"]) {
  if (archiveFormat === "tar.gz") {
    await run(["tar", "-czf", archivePath, "-C", path.dirname(binaryPath), "opencode"])
    return
  }
  await run(["zip", "-q", "-j", archivePath, binaryPath])
}

async function verifyArchive(archivePath: string, archiveFormat: ReleaseTarget["archiveFormat"]) {
  const result =
    archiveFormat === "tar.gz" ? await run(["tar", "-tzf", archivePath]) : await run(["unzip", "-Z1", archivePath])
  const entries = result.stdout.split(/\r?\n/).filter(Boolean)
  if (entries.length !== 1 || entries[0] !== "opencode") {
    throw new Error(`${path.basename(archivePath)} must contain only the opencode executable`)
  }
}

async function requireReleaseArchives(directory: string) {
  const files = await listFiles(directory)
  const names = files.map((file) => path.basename(file))
  const duplicates = names.filter((name, index) => names.indexOf(name) !== index)
  if (duplicates.length > 0) throw new Error(`duplicate release artifacts: ${duplicates.join(", ")}`)

  const expectedArchives = RELEASE_TARGETS.map((target) => target.archiveName)
  const allowed = new Set([...expectedArchives, CHECKSUMS_FILENAME])
  const unexpected = files.filter((file) => !allowed.has(file))
  if (unexpected.length > 0) throw new Error(`unexpected release artifacts: ${unexpected.join(", ")}`)

  const missing = expectedArchives.filter((archive) => !files.includes(archive))
  if (missing.length > 0) throw new Error(`missing release artifacts: ${missing.join(", ")}`)
  return files
}

async function listFiles(directory: string) {
  const entries = await Array.fromAsync(new Bun.Glob("**/*").scan({ cwd: directory }))
  const files = await Promise.all(
    entries.map(async (entry) => {
      const info = await stat(path.join(directory, entry))
      return info.isFile() ? entry.replaceAll("\\", "/") : undefined
    }),
  )
  return files.filter((file): file is string => file !== undefined)
}

function parseSha256Sums(contents: string) {
  const lines = contents.split(/\r?\n/)
  if (lines.at(-1) === "") lines.pop()
  if (lines.length !== RELEASE_TARGETS.length || lines.some((line) => line.length === 0)) {
    throw new Error(`${CHECKSUMS_FILENAME} must contain exactly four checksum lines`)
  }

  const sums = new Map<string, string>()
  for (const line of lines) {
    const match = /^([a-f0-9]{64}) {2}(.+)$/.exec(line)
    if (!match) throw new Error(`invalid ${CHECKSUMS_FILENAME} line: ${line}`)
    const [, hash, filename] = match
    if (sums.has(filename)) throw new Error(`duplicate checksum for ${filename}`)
    if (!RELEASE_TARGETS.some((target) => target.archiveName === filename)) {
      throw new Error(`unexpected checksum for ${filename}`)
    }
    sums.set(filename, hash)
  }

  const missing = RELEASE_TARGETS.filter((target) => !sums.has(target.archiveName)).map((target) => target.archiveName)
  if (missing.length > 0) throw new Error(`missing checksums: ${missing.join(", ")}`)
  return sums
}

async function run(command: string[]) {
  const child = Bun.spawn(command, { stdin: "ignore", stdout: "pipe", stderr: "pipe" })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ])
  if (exitCode !== 0) throw new Error(`${command[0]} exited with code ${exitCode}: ${stderr.trim() || stdout.trim()}`)
  return { stdout, stderr }
}

function requiredEnvironment(name: string) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

async function main() {
  const command = process.argv[2]
  if (command === "validate") {
    console.log(validateVersion(requiredEnvironment("OPENCODE_VERSION")))
    return
  }

  if (command === "package") {
    if (process.env.OPENCODE_RELEASE !== undefined) throw new Error("OPENCODE_RELEASE must be unset for fork releases")
    const target = getReleaseTarget(
      requiredEnvironment("FORK_RELEASE_PLATFORM"),
      requiredEnvironment("FORK_RELEASE_ARCH"),
    )
    const distDirectory = path.resolve(repoRoot, process.env.FORK_RELEASE_DIST_DIRECTORY ?? "packages/opencode/dist")
    const outputDirectory = path.resolve(
      repoRoot,
      process.env.FORK_RELEASE_OUTPUT_DIRECTORY ?? "fork-release-artifacts",
    )
    const archivePath = await packageRelease({
      version: requiredEnvironment("OPENCODE_VERSION"),
      platform: target.platform,
      arch: target.arch,
      distDirectory,
      outputDirectory,
    })
    console.log(`created ${archivePath}`)
    return
  }

  if (command === "verify") {
    const directory = path.resolve(repoRoot, process.env.FORK_RELEASE_ARTIFACT_DIRECTORY ?? "release")
    await writeSha256Sums(directory)
    await verifyReleaseArtifacts(directory)
    console.log(`verified ${RELEASE_TARGETS.length} fork release archives`)
    return
  }

  throw new Error("usage: fork-release.ts <validate|package|verify>")
}

if (import.meta.main) await main()

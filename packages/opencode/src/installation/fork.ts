import { createHash } from "node:crypto"
import { lstat } from "node:fs/promises"
import { basename, dirname, join } from "node:path"
import { gunzipSync } from "node:zlib"
import type { Entry } from "@zip.js/zip.js"
import semver from "semver"
import { Effect, FileSystem, Schema } from "effect"
import type { AppProcess } from "@opencode-ai/core/process"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"

declare const OPENCODE_DISTRIBUTION: string | undefined
declare const OPENCODE_LIBC: string | undefined
declare const OPENCODE_UPDATE_TEST_ORIGIN: string | undefined

const distribution = typeof OPENCODE_DISTRIBUTION === "undefined" ? "totalolage/opencode" : OPENCODE_DISTRIBUTION
const testOrigin = typeof OPENCODE_UPDATE_TEST_ORIGIN === "undefined" ? "" : OPENCODE_UPDATE_TEST_ORIGIN
const libc = typeof OPENCODE_LIBC === "undefined" ? undefined : OPENCODE_LIBC

const repository = "totalolage/opencode"
const apiOrigin = "https://api.github.com"
const releaseOrigin = "https://github.com"
const releaseAssetOrigins = new Set([
  "https://github.com",
  "https://release-assets.githubusercontent.com",
  "https://objects.githubusercontent.com",
])
const maxRedirects = 10

export const IS_FORK: boolean = distribution !== "upstream"

export const INSTRUCTIONS =
  "Install the totalolage/opencode fork from https://github.com/totalolage/opencode/releases and place the opencode binary at ~/.opencode/bin/opencode or ~/.local/bin/opencode."

export class ForkUpdateError extends Schema.TaggedErrorClass<ForkUpdateError>()("ForkUpdateError", {
  message: Schema.String,
}) {}

const ReleaseMetadata = Schema.Struct({
  tag_name: Schema.String,
  draft: Schema.Boolean,
  prerelease: Schema.Boolean,
})

type RequestKind = "metadata" | "asset"
type ArchiveFormat = "tar.gz" | "zip"
type Asset = {
  readonly filename: string
  readonly format: ArchiveFormat
}

export function stableVersion(input: string): string | undefined {
  if (typeof input !== "string") return undefined
  const match = /^(?:v)?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/.exec(input)
  if (!match) return undefined

  const version = `${match[1]}.${match[2]}.${match[3]}`
  return semver.valid(version) === version ? version : undefined
}

export function method(execPath: string): "curl" | "unknown" {
  const binPath = dirname(execPath)
  if (basename(execPath) !== "opencode") return "unknown"
  if (basename(binPath) !== "bin") return "unknown"
  const installPath = dirname(binPath)
  if (basename(installPath) !== ".opencode" && basename(installPath) !== ".local") return "unknown"
  return "curl"
}

export function latest(): Effect.Effect<string, ForkUpdateError, HttpClient.HttpClient> {
  return Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient
    const response = yield* request(http, `${metadataOrigin()}/repos/${repository}/releases/latest`, "metadata")
    const release = yield* decodeRelease(response)
    const validation = validateRelease(release)
    if (validation instanceof ForkUpdateError) return yield* validation
    return validation
  }).pipe(Effect.mapError(toForkUpdateError))
}

export function upgrade(
  target: string,
  execPath: string,
): Effect.Effect<void, ForkUpdateError, HttpClient.HttpClient | FileSystem.FileSystem | AppProcess.Service> {
  return Effect.scoped(
    Effect.gen(function* () {
      if (method(execPath) !== "curl") {
        yield* new ForkUpdateError({ message: `Unsupported fork installation path: ${execPath}. ${INSTRUCTIONS}` })
      }

      const version = stableVersion(target)
      if (version === undefined) {
        return yield* new ForkUpdateError({ message: `Invalid fork release version: ${target}` })
      }

      const asset = currentAsset()
      if (asset instanceof ForkUpdateError) return yield* asset

      const initialTarget = yield* inspectTarget(execPath)
      const initialTargetError = validateTarget(initialTarget, execPath)
      if (initialTargetError) return yield* initialTargetError

      const http = yield* HttpClient.HttpClient
      const fs = yield* FileSystem.FileSystem
      const stage = yield* fs.makeTempDirectoryScoped({ directory: dirname(execPath), prefix: ".opencode-update-" })
      const stagedPath = join(stage, "opencode")
      yield* fetchRelease(http, version)
      const archiveUrl = `${assetOrigin()}/${repository}/releases/download/v${version}/${asset.filename}`
      const checksumUrl = `${assetOrigin()}/${repository}/releases/download/v${version}/SHA256SUMS`
      const archive = yield* fetchBytes(http, archiveUrl, "asset")
      const checksums = yield* fetchText(http, checksumUrl, "asset")
      const expected = expectedChecksum(checksums, asset.filename)
      if (expected instanceof ForkUpdateError) return yield* expected
      const actual = createHash("sha256").update(archive).digest("hex")
      if (actual !== expected) {
        yield* new ForkUpdateError({
          message: `Checksum mismatch for ${asset.filename}: expected ${expected}, received ${actual}`,
        })
      }

      const binary = yield* extract(archive, asset.format)
      yield* fs.writeFile(stagedPath, binary, { mode: 0o755 })
      yield* fs.chmod(stagedPath, 0o755)

      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const finalTarget = yield* inspectTarget(execPath)
          const finalTargetError = validateTarget(finalTarget, execPath)
          if (finalTargetError) return yield* finalTargetError
          yield* fs.rename(stagedPath, execPath)
        }),
      )
    }),
  ).pipe(Effect.mapError(toForkUpdateError))
}

function metadataOrigin() {
  return testOrigin || apiOrigin
}

function assetOrigin() {
  return testOrigin || releaseOrigin
}

function isStrictTestOrigin(origin: string) {
  const match = /^http:\/\/127\.0\.0\.1:([0-9]+)$/.exec(origin)
  if (!match) return false
  const port = Number(match[1])
  return Number.isSafeInteger(port) && port > 0 && port <= 65535 && String(port) === match[1]
}

function isAllowedUrl(input: string, kind: RequestKind) {
  if (!URL.canParse(input)) return false
  const url = new URL(input)
  if (url.username !== "" || url.password !== "") return false

  if (testOrigin) {
    return isStrictTestOrigin(testOrigin) && url.protocol === "http:" && url.origin === new URL(testOrigin).origin
  }
  if (url.protocol !== "https:") return false
  if (kind === "metadata") return url.origin === apiOrigin
  return releaseAssetOrigins.has(url.origin)
}

function request(
  http: HttpClient.HttpClient,
  url: string,
  kind: RequestKind,
  redirects = 0,
): Effect.Effect<HttpClientResponse.HttpClientResponse, ForkUpdateError> {
  return Effect.gen(function* () {
    if (!isAllowedUrl(url, kind)) {
      yield* new ForkUpdateError({ message: `Rejected insecure or untrusted update URL: ${url}` })
    }

    const input =
      kind === "metadata" ? HttpClientRequest.get(url).pipe(HttpClientRequest.acceptJson) : HttpClientRequest.get(url)
    const response = yield* http
      .execute(input)
      .pipe(
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
        Effect.mapError(toForkUpdateError),
      )

    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.location
      if (!location) {
        yield* new ForkUpdateError({ message: `Redirect from ${url} did not include a Location header` })
      }
      if (redirects >= maxRedirects) {
        yield* new ForkUpdateError({ message: `Too many redirects while requesting ${url}` })
      }
      if (!URL.canParse(location, url)) {
        yield* new ForkUpdateError({ message: `Invalid redirect from ${url}: ${location}` })
      }
      return yield* request(http, new URL(location, url).toString(), kind, redirects + 1)
    }

    if (response.status < 200 || response.status >= 300) {
      yield* new ForkUpdateError({ message: `Update request failed with HTTP ${response.status}: ${url}` })
    }
    return response
  })
}

function fetchBytes(
  http: HttpClient.HttpClient,
  url: string,
  kind: RequestKind,
): Effect.Effect<Uint8Array, ForkUpdateError> {
  return request(http, url, kind).pipe(
    Effect.flatMap((response) => response.arrayBuffer),
    Effect.map((body) => new Uint8Array(body)),
    Effect.mapError(toForkUpdateError),
  )
}

function fetchText(
  http: HttpClient.HttpClient,
  url: string,
  kind: RequestKind,
): Effect.Effect<string, ForkUpdateError> {
  return request(http, url, kind).pipe(
    Effect.flatMap((response) => response.text),
    Effect.mapError(toForkUpdateError),
  )
}

function decodeRelease(
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<Schema.Schema.Type<typeof ReleaseMetadata>, ForkUpdateError> {
  return HttpClientResponse.schemaBodyJson(ReleaseMetadata)(response).pipe(Effect.mapError(toForkUpdateError))
}

function fetchRelease(http: HttpClient.HttpClient, version: string) {
  return Effect.gen(function* () {
    const response = yield* request(
      http,
      `${metadataOrigin()}/repos/${repository}/releases/tags/v${version}`,
      "metadata",
    )
    const release = yield* decodeRelease(response)
    const validation = validateRelease(release, version)
    if (validation instanceof ForkUpdateError) return yield* validation
    return release
  })
}

function releaseVersion(tag: string) {
  const version = stableVersion(tag)
  if (version === undefined || tag !== `v${version}`) return undefined
  return version
}

function validateRelease(
  release: Schema.Schema.Type<typeof ReleaseMetadata>,
  requested?: string,
): string | ForkUpdateError {
  const version = releaseVersion(release.tag_name)
  if (version === undefined) {
    return new ForkUpdateError({ message: `Invalid release tag: ${release.tag_name}` })
  }
  if (release.draft !== false) {
    return new ForkUpdateError({ message: `Release ${release.tag_name} is a draft` })
  }
  if (release.prerelease !== false) {
    return new ForkUpdateError({ message: `Release ${release.tag_name} is a prerelease` })
  }
  if (requested !== undefined && release.tag_name !== `v${requested}`) {
    return new ForkUpdateError({
      message: `Release tag mismatch: requested v${requested}, received ${release.tag_name}`,
    })
  }
  return version
}

function currentAsset(): Asset | ForkUpdateError {
  if (process.platform === "win32") return unsupported("Windows is unsupported.")
  if (process.platform !== "linux" && process.platform !== "darwin") {
    return unsupported(`Unsupported platform: ${process.platform}.`)
  }
  if (process.arch !== "x64" && process.arch !== "arm64")
    return unsupported(`Unsupported architecture: ${process.arch}.`)
  if (process.platform === "linux") {
    if (libc === "musl") return unsupported("Linux musl is unsupported; glibc is required.")
    if (libc !== undefined && libc !== "glibc") {
      return unsupported(`Unsupported Linux libc: ${libc || "unknown"}; glibc is required.`)
    }
    if (libc === undefined && runtimeGlibcVersion() === undefined) {
      return unsupported("Linux glibc runtime could not be detected; glibc is required.")
    }
  }

  const platform = process.platform === "linux" ? "linux" : "darwin"
  const format = platform === "linux" ? "tar.gz" : "zip"
  return {
    filename: `opencode-${platform}-${process.arch}.${format}`,
    format,
  }
}

function unsupported(reason: string) {
  return new ForkUpdateError({ message: `${reason} ${INSTRUCTIONS}` })
}

function runtimeGlibcVersion() {
  if (typeof process.report?.getReport !== "function") return undefined
  try {
    const report: unknown = process.report.getReport()
    if (!isRecord(report) || !isRecord(report.header)) return undefined
    const version = report.header.glibcVersionRuntime
    if (typeof version !== "string" || version.trim() === "") return undefined
    return version
  } catch {
    return undefined
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

type TargetStats = Awaited<ReturnType<typeof lstat>>

function inspectTarget(execPath: string): Effect.Effect<TargetStats | undefined, unknown> {
  return Effect.tryPromise({
    try: () => lstat(execPath),
    catch: (cause) => cause,
  }).pipe(Effect.catchIf(isNotFound, () => Effect.succeed(undefined)))
}

function isNotFound(cause: unknown) {
  return typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT"
}

function validateTarget(target: TargetStats | undefined, execPath: string): ForkUpdateError | undefined {
  if (target === undefined) return undefined
  if (target.isSymbolicLink())
    return new ForkUpdateError({ message: `Refusing to replace symlink target: ${execPath}` })
  if (!target.isFile()) return new ForkUpdateError({ message: `Fork target is not a regular file: ${execPath}` })
  return undefined
}

function expectedChecksum(text: string, filename: string): string | ForkUpdateError {
  const records = text
    .split(/\r?\n/)
    .filter((line) => line.length > 0)
    .map(parseChecksumLine)
  if (records.some((record) => record === undefined)) {
    return new ForkUpdateError({ message: "Invalid SHA256SUMS format" })
  }

  const matches = records.filter(
    (record): record is { readonly digest: string; readonly filename: string } =>
      record !== undefined && record.filename === filename,
  )
  if (matches.length !== 1) {
    return new ForkUpdateError({ message: `SHA256SUMS did not contain exactly one entry for ${filename}` })
  }
  return matches[0].digest.toLowerCase()
}

function parseChecksumLine(line: string) {
  const match = /^([0-9a-fA-F]{64})(?: {2}| \*)(.+)$/.exec(line)
  if (!match) return undefined
  return { digest: match[1], filename: match[2] }
}

function extract(archive: Uint8Array, format: ArchiveFormat): Effect.Effect<Uint8Array, unknown> {
  return Effect.tryPromise({
    try: () => (format === "zip" ? extractZip(archive) : Promise.resolve(extractTarGzip(archive))),
    catch: (cause) => cause,
  })
}

async function extractZip(archive: Uint8Array) {
  const { ZipReader, Uint8ArrayReader } = await import("@zip.js/zip.js")
  const reader = new ZipReader(new Uint8ArrayReader(archive))
  try {
    const entries = await reader.getEntries()
    if (entries.length !== 1) throw new Error("Archive must contain exactly one entry")
    const entry = entries[0]
    if (entry.filename !== "opencode") throw new Error("Archive entry must be the root opencode file")
    if (!isRegularZipEntry(entry)) throw new Error("Archive entry is not a regular file")
    if (!entry.getData) throw new Error("Archive entry has no readable data")
    const chunks: Uint8Array[] = []
    // Collect chunks because zip.js preallocates from untrusted metadata and can return padding.
    await entry.getData(
      new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(new Uint8Array(chunk))
        },
      }),
      { checkSignature: true, useWebWorkers: false },
    )
    const actual = chunks.reduce((size, chunk) => size + chunk.byteLength, 0)
    if (actual !== entry.uncompressedSize) {
      throw new Error(`ZIP archive entry size mismatch: expected ${entry.uncompressedSize}, received ${actual}`)
    }
    const binary = new Uint8Array(actual)
    chunks.reduce((offset, chunk) => {
      binary.set(chunk, offset)
      return offset + chunk.byteLength
    }, 0)
    return binary
  } finally {
    await reader.close()
  }
}

function isRegularZipEntry(entry: Entry) {
  if (entry.directory || entry.encrypted) return false

  const operatingSystem = (entry.versionMadeBy >>> 8) & 0xff
  const mode = (entry.externalFileAttributes >>> 16) & 0xffff
  const modeType = mode & 0o170000
  if (modeType !== 0 && modeType !== 0o100000) return false
  if (operatingSystem === 3) return modeType === 0o100000
  if (operatingSystem !== 0 && operatingSystem !== 10 && operatingSystem !== 11) return false
  return (entry.externalFileAttributes & 0x10) === 0
}

function extractTarGzip(archive: Uint8Array): Uint8Array {
  const tar = gunzipSync(archive)
  if (tar.length % 512 !== 0) throw new Error("Tar archive is not aligned to 512-byte blocks")

  const files: Uint8Array[] = []
  let offset = 0
  let ended = false
  while (offset < tar.length) {
    const header = tar.subarray(offset, offset + 512)
    if (header.length !== 512) throw new Error("Tar archive has a truncated header")
    if (isZeroBlock(header)) {
      if (offset + 1024 > tar.length || !isZeroBlock(tar.subarray(offset + 512, offset + 1024))) {
        throw new Error("Tar archive is missing its end blocks")
      }
      if (!isZeroBlock(tar.subarray(offset + 1024))) throw new Error("Tar archive has trailing data")
      ended = true
      break
    }

    const storedChecksum = tarNumber(header.subarray(148, 156), "checksum")
    let checksum = 0
    for (let index = 0; index < header.length; index++) {
      checksum += index >= 148 && index < 156 ? 32 : header[index]
    }
    if (checksum !== storedChecksum) throw new Error("Tar archive has an invalid header checksum")

    const magic = new TextDecoder("ascii", { fatal: true }).decode(header.subarray(257, 263))
    if (magic !== "ustar\0" && magic !== "ustar ") throw new Error("Tar archive has an unsupported header format")

    const name = tarText(header.subarray(0, 100), "name")
    const prefix = tarText(header.subarray(345, 500), "prefix")
    const filename = prefix ? `${prefix}/${name}` : name
    const type = header[156]
    if (type !== 0 && type !== 48) throw new Error(`Tar archive entry is not a regular file: ${filename}`)
    const mode = tarNumber(header.subarray(100, 108), "mode")
    const modeType = mode & 0o170000
    if (modeType !== 0 && modeType !== 0o100000) throw new Error(`Tar archive entry is not regular: ${filename}`)
    if (filename !== "opencode") throw new Error(`Tar archive contains an unexpected entry: ${filename}`)

    const size = tarNumber(header.subarray(124, 136), "size")
    const dataStart = offset + 512
    const dataEnd = dataStart + size
    const paddedEnd = dataStart + Math.ceil(size / 512) * 512
    if (dataEnd > tar.length || paddedEnd > tar.length) throw new Error("Tar archive entry is truncated")
    files.push(new Uint8Array(tar.subarray(dataStart, dataEnd)))
    offset = paddedEnd
  }

  if (!ended) throw new Error("Tar archive has no end blocks")
  if (files.length !== 1) throw new Error("Tar archive must contain exactly one entry")
  return files[0]
}

function isZeroBlock(block: Uint8Array) {
  for (const byte of block) if (byte !== 0) return false
  return true
}

function tarText(field: Uint8Array, name: string) {
  const end = field.indexOf(0)
  const value = new TextDecoder("utf-8", { fatal: true }).decode(field.subarray(0, end === -1 ? field.length : end))
  if (value.length === 0 && name !== "prefix") throw new Error(`Tar archive has an empty ${name}`)
  return value
}

function tarNumber(field: Uint8Array, name: string) {
  const value = new TextDecoder("ascii", { fatal: true }).decode(field).replaceAll("\0", "").trim()
  if (!/^[0-7]+$/.test(value)) throw new Error(`Tar archive has an invalid ${name}`)
  const number = Number.parseInt(value, 8)
  if (!Number.isSafeInteger(number)) throw new Error(`Tar archive ${name} is too large`)
  return number
}

function toForkUpdateError(cause: unknown): ForkUpdateError {
  if (cause instanceof ForkUpdateError) return cause
  if (cause instanceof Error) return new ForkUpdateError({ message: cause.message })
  if (typeof cause === "object" && cause !== null && "message" in cause && typeof cause.message === "string") {
    return new ForkUpdateError({ message: cause.message })
  }
  return new ForkUpdateError({ message: String(cause) })
}

export * as Fork from "./fork"

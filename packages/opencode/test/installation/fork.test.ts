import { describe, expect, test } from "bun:test"
import { chmod, link, lstat, mkdir, readFile, readdir, rename, rm, symlink, utimes, writeFile } from "node:fs/promises"
import path from "node:path"
import { gzipSync } from "node:zlib"
import { Cause, Effect, Exit, Fiber, Layer } from "effect"
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http"
import { NodeFileSystem } from "@effect/platform-node"
import { TextReader, Uint8ArrayWriter, ZipWriter } from "@zip.js/zip.js"
import { AppProcess } from "@opencode-ai/core/process"
import { Fork } from "../../src/installation/fork"
import { makeForkFixture } from "./fork-fixture"
import { tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.mergeAll(FetchHttpClient.layer, NodeFileSystem.layer, Layer.mock(AppProcess.Service, {})))
const integration =
  (process.platform === "linux" || process.platform === "darwin") &&
  (process.arch === "x64" || process.arch === "arm64")
    ? it.live
    : it.live.skip
const packageDirectory = path.resolve(import.meta.dir, "../..")

const fixture = (options?: Parameters<typeof makeForkFixture>[0]) =>
  Effect.acquireRelease(
    Effect.promise(() => makeForkFixture(options)),
    (value) => Effect.promise(value.close),
  )

describe("installation fork", () => {
  test("accepts a strict stable version", () => {
    expect(Fork.supportedVersion("v1.2.3")).toBe("1.2.3")
  })

  test("accepts fork timestamped releases with an optional v prefix", () => {
    expect(Fork.supportedVersion("1.18.30-f8y-20260913140000")).toBe("1.18.30-f8y-20260913140000")
    expect(Fork.supportedVersion("v1.18.30-f8y-20260913140000")).toBe("1.18.30-f8y-20260913140000")
  })

  test("rejects prereleases, builds, leading zeroes, unsafe versions, and bad timestamps", () => {
    for (const input of [
      "1.2",
      "1.2.3.4",
      "1.02.3",
      "v1.2.03",
      "1.2.3-alpha.1",
      "1.2.3+build",
      "V1.2.3",
      "9007199254740992.0.0",
      "1.2.3-f8y-202609131400",
      "1.2.3-f8y-20261331000000",
    ]) {
      expect(Fork.supportedVersion(input), input).toBeUndefined()
    }
  })

  test("orders stable over same-core timestamps, higher cores first, timestamps lexicographically", () => {
    expect(Fork.compareVersions("1.2.3", "1.2.3-f8y-20991231000000")).toBe(1)
    expect(Fork.compareVersions("1.3.0-f8y-20200101000000", "1.2.9")).toBe(1)
    expect(Fork.compareVersions("1.2.3-f8y-20260913140000", "1.2.3-f8y-20250101000000")).toBe(1)
    expect(Fork.compareVersions("2.0.0", "1.999.999-f8y-20991231000000")).toBe(1)
    expect(Fork.compareVersions("1.2.3-f8y-20260913140000", "1.2.3-f8y-20260913140000")).toBe(0)
  })

  test("identifies only direct .opencode/bin and .local/bin binaries", () => {
    expect(Fork.method("/home/test/.opencode/bin/opencode")).toBe("curl")
    expect(Fork.method("/home/test/.local/bin/opencode")).toBe("curl")
    expect(Fork.method("/home/test/.opencode/share/bin/opencode")).toBe("unknown")
    expect(Fork.method("/home/test/.opencode/bin/other")).toBe("unknown")
    expect(Fork.method("/home/test/.opencode/bin/opencode/child")).toBe("unknown")
  })

  test("rejects POSIX paths containing literal backslashes", () => {
    if (process.platform !== "linux" && process.platform !== "darwin") return
    expect(Fork.method("/tmp/unmanaged/.opencode\\bin\\opencode")).toBe("unknown")
  })

  test("publishes fork identity and manual install instructions", () => {
    expect(Fork.IS_FORK).toBe(true)
    expect(Fork.INSTRUCTIONS).toContain("https://github.com/totalolage/opencode/releases")
    expect(Fork.INSTRUCTIONS.toLowerCase()).toContain("install")
  })

  integration("reads a valid latest release tag", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      expect(yield* value.compiled.latest()).toBe("1.2.3")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases?per_page=100&page=1"])
    }),
  )

  integration("selects the highest eligible release from an unordered mixed list", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases([
        { tag_name: "v2.0.0", draft: true, prerelease: false },
        { tag_name: "1.9.0", draft: false, prerelease: false },
        { tag_name: "v1.9.0", draft: false, prerelease: true },
        { tag_name: "v2.0.0", draft: false, prerelease: true },
        { tag_name: "v1.8.0-f8y-20260913140000", draft: false, prerelease: true },
        { tag_name: "v1.9.5", draft: false, prerelease: false },
      ])
      expect(yield* value.compiled.latest()).toBe("1.9.5")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases?per_page=100&page=1"])
    }),
  )

  integration("prefers a stable release over a timestamped one on the same core", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases([
        { tag_name: "v1.2.3-f8y-20991231000000", draft: false, prerelease: true },
        { tag_name: "v1.2.3", draft: false, prerelease: false },
      ])
      expect(yield* value.compiled.latest()).toBe("1.2.3")
    }),
  )

  integration("prefers a higher timestamped core over a lower stable one", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases([
        { tag_name: "v1.2.3", draft: false, prerelease: false },
        { tag_name: "v1.3.0-f8y-20200101000000", draft: false, prerelease: true },
      ])
      expect(yield* value.compiled.latest()).toBe("1.3.0-f8y-20200101000000")
    }),
  )

  integration("prefers the newer timestamp on the same core", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases([
        { tag_name: "v1.2.3-f8y-20250101000000", draft: false, prerelease: true },
        { tag_name: "v1.2.3-f8y-20260913140000", draft: false, prerelease: true },
      ])
      expect(yield* value.compiled.latest()).toBe("1.2.3-f8y-20260913140000")
    }),
  )

  integration("reads the second page when the first page is full", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      const firstPage = Array.from({ length: 100 }, (_, index) => ({
        tag_name: `v1.0.${index}`,
        draft: false,
        prerelease: false,
      }))
      value.setReleases([firstPage, [{ tag_name: "v3.0.0", draft: false, prerelease: false }]])
      expect(yield* value.compiled.latest()).toBe("3.0.0")
      expect(value.requests).toEqual([
        "/repos/totalolage/opencode/releases?per_page=100&page=1",
        "/repos/totalolage/opencode/releases?per_page=100&page=2",
      ])
    }),
  )

  integration("fails closed after 100 full pages without requesting page 101", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases(
        Array.from({ length: 100 * 100 }, () => ({ tag_name: "v1.0.0", draft: false, prerelease: false })),
      )

      const error = yield* value.compiled.latest().pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("exceeded 100 pages")
      expect(value.requests).toHaveLength(100)
      expect(value.requests[0]).toBe("/repos/totalolage/opencode/releases?per_page=100&page=1")
      expect(value.requests[99]).toBe("/repos/totalolage/opencode/releases?per_page=100&page=100")
      expect(value.requests.some((request) => request.includes("page=101"))).toBe(false)
    }),
  )

  integration("selects the accumulated highest when page 100 is short", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      const release = (tag: string) => ({ tag_name: tag, draft: false, prerelease: false })
      const firstPage = [release("v9.9.9"), ...Array.from({ length: 99 }, (_, index) => release(`v1.0.${index}`))]
      const fullPages = Array.from({ length: 98 }, (_, page) =>
        Array.from({ length: 100 }, (_, index) => release(`v2.0.${page * 100 + index}`)),
      )
      value.setReleases([firstPage, ...fullPages, [release("v1.0.999")]])

      expect(yield* value.compiled.latest()).toBe("9.9.9")
      expect(value.requests).toHaveLength(100)
      expect(value.requests[99]).toBe("/repos/totalolage/opencode/releases?per_page=100&page=100")
      expect(value.requests.some((request) => request.includes("page=101"))).toBe(false)
    }),
  )

  integration("fails closed when no eligible release exists", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setReleases([])
      const error = yield* value.compiled.latest().pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("No eligible fork release")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases?per_page=100&page=1"])
    }),
  )

  integration("fails closed on malformed release listing metadata", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      for (const pages of ["nope", [{ tag_name: "v1.2.3" }], [{ tag_name: 42, draft: false, prerelease: false }]]) {
        value.setReleases(pages as unknown[])
        const error = yield* value.compiled.latest().pipe(Effect.flip)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      }
    }),
  )

  integration("normalizes a strict compile-time origin before validating the logical URL", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ compileOrigin: "http://127.0.0.1:80" })
      const existingHttpClient = yield* HttpClient.HttpClient
      const client = HttpClient.mapRequest(existingHttpClient, (request) => {
        expect(request.url).toBe("http://127.0.0.1:80/repos/totalolage/opencode/releases?per_page=100&page=1")
        const url = new URL(request.url)
        return HttpClientRequest.setUrl(request, `${value.origin}${url.pathname}${url.search}`)
      })

      expect(yield* value.compiled.latest().pipe(Effect.provideService(HttpClient.HttpClient, client))).toBe("1.2.3")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases?per_page=100&page=1"])
    }),
  )

  integration("rejects non-exact compiled test origins before network access", () =>
    Effect.gen(function* () {
      for (const suffix of ["\n", "\r\n", " ", "\t"]) {
        const value = yield* fixture({ compileOrigin: `http://127.0.0.1:80${suffix}` })
        const existingHttpClient = yield* HttpClient.HttpClient
        const client = HttpClient.mapRequest(existingHttpClient, (request) =>
          HttpClientRequest.setUrl(request, `${value.origin}${new URL(request.url).pathname}`),
        )

        const error = yield* value.compiled
          .latest()
          .pipe(Effect.provideService(HttpClient.HttpClient, client), Effect.flip)
        expect(error, suffix).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(error.message, suffix).toContain("Rejected insecure or untrusted update URL")
        expect(value.requests, suffix).toEqual([])
      }
    }),
  )

  integration("rejects a pinned prerelease flag mismatch before download", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      value.setPinned({ tag_name: "v1.2.3", draft: false, prerelease: true })
      const stable = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
      expect(stable.message).toContain("prerelease flag")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")

      const timestamped = "1.18.30-f8y-20260913140000"
      value.setPinned({ tag_name: `v${timestamped}`, draft: false, prerelease: false })
      const mismatched = yield* value.compiled.upgrade(timestamped, value.target).pipe(Effect.flip)
      expect(mismatched.message).toContain("prerelease flag")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
    }),
  )

  integration("installs a fork timestamped release", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      const version = "1.18.30-f8y-20260913140000"
      value.setPinned({ tag_name: `v${version}`, draft: false, prerelease: true })
      yield* Effect.promise(() => value.seed("old binary"))
      yield* value.compiled.upgrade(version, value.target)

      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("new binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
      expect(value.requests).toEqual([
        `/repos/totalolage/opencode/releases/tags/v${version}`,
        `/totalolage/opencode/releases/download/v${version}/${value.archiveName}`,
        `/totalolage/opencode/releases/download/v${version}/SHA256SUMS`,
      ])
    }),
  )

  integration("rejects a pinned draft", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      value.setPinned({ tag_name: "v1.2.3", draft: true, prerelease: false })
      const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(error.message).toContain("is a draft")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases/tags/v1.2.3"])
    }),
  )

  integration("downloads, verifies, installs, and cleans a release", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      yield* value.compiled.upgrade("v1.2.3", value.target)

      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("new binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
      expect(value.requests).toEqual([
        "/repos/totalolage/opencode/releases/tags/v1.2.3",
        `/totalolage/opencode/releases/download/v1.2.3/${value.archiveName}`,
        "/totalolage/opencode/releases/download/v1.2.3/SHA256SUMS",
      ])
    }),
  )

  integration("rejects a non-writable existing target before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const original = yield* Effect.promise(() => readFile(value.target))
      yield* Effect.promise(() => chmod(value.target, 0o444))

      try {
        const error = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(yield* Effect.promise(() => readFile(value.target))).toEqual(original)
        expect((yield* Effect.promise(() => lstat(value.target))).mode & 0o777).toBe(0o444)
        expect(value.requests).toEqual([])
        expect(yield* Effect.promise(value.staging)).toEqual([])
      } finally {
        yield* Effect.promise(() => chmod(value.target, 0o755))
      }
    }),
  )

  integration("rejects a read-only installation directory before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const original = yield* Effect.promise(() => readFile(value.target))
      const bin = path.dirname(value.target)
      yield* Effect.promise(() => chmod(bin, 0o555))

      try {
        const error = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(yield* Effect.promise(() => readFile(value.target))).toEqual(original)
        expect(value.requests).toEqual([])
        expect(yield* Effect.promise(value.staging)).toEqual([])
      } finally {
        yield* Effect.promise(() => chmod(bin, 0o755))
      }
    }),
  )

  integration("installs a valid POSIX USTAR release archive", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "linux", libc: "glibc", format: "ustar" })
      yield* Effect.promise(() => value.seed("old binary"))
      yield* value.compiled.upgrade("v1.2.3", value.target)

      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("new binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
      expect(value.requests).toEqual([
        "/repos/totalolage/opencode/releases/tags/v1.2.3",
        `/totalolage/opencode/releases/download/v1.2.3/${value.archiveName}`,
        "/totalolage/opencode/releases/download/v1.2.3/SHA256SUMS",
      ])
    }),
  )

  integration("downloads and installs the Darwin x64 ZIP release", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      yield* Effect.promise(() => value.seed("old binary"))
      yield* value.compiled.upgrade("v1.2.3", value.target)

      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("new binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
      expect(value.requests).toEqual([
        "/repos/totalolage/opencode/releases/tags/v1.2.3",
        "/totalolage/opencode/releases/download/v1.2.3/opencode-darwin-x64.zip",
        "/totalolage/opencode/releases/download/v1.2.3/SHA256SUMS",
      ])
    }),
  )

  integration("installs a compressed Darwin ZIP payload", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      const payload = "compressed fork binary\n".repeat(1000)
      const archive = yield* Effect.promise(() =>
        zipArchive([{ name: "opencode", data: payload, externalFileAttributes: unixRegularAttributes }]),
      )
      const localHeader = new DataView(archive.buffer, archive.byteOffset, archive.byteLength)
      expect(localHeader.getUint16(8, true)).toBe(8)
      value.setArchive(archive)
      yield* Effect.promise(() => value.seed("old binary"))
      yield* value.compiled.upgrade("v1.2.3", value.target)

      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe(payload)
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects a Darwin ZIP with a corrupted central-directory CRC", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      const archive = yield* Effect.promise(() =>
        zipArchive([{ name: "opencode", data: "new binary", externalFileAttributes: unixRegularAttributes }]),
      )
      const centralDirectoryOffset = archive.findIndex(
        (byte, index) =>
          byte === 0x50 && archive[index + 1] === 0x4b && archive[index + 2] === 0x01 && archive[index + 3] === 0x02,
      )
      expect(centralDirectoryOffset).not.toBe(-1)
      const corrupted = new Uint8Array(archive)
      corrupted[centralDirectoryOffset + 16] ^= 0xff
      value.setArchive(corrupted)
      yield* Effect.promise(() => value.seed("old binary"))

      const exit = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const error = Cause.squash(exit.cause)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(error).toMatchObject({ message: expect.stringContaining("Invalid signature") })
      }
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects Darwin ZIP entries with incorrect declared uncompressed sizes", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      const payload = "declared size payload"
      const archive = yield* Effect.promise(() =>
        zipArchive([{ name: "opencode", data: payload, externalFileAttributes: unixRegularAttributes }]),
      )
      const centralDirectoryOffset = archive.findIndex(
        (byte, index) =>
          byte === 0x50 && archive[index + 1] === 0x4b && archive[index + 2] === 0x01 && archive[index + 3] === 0x02,
      )
      expect(centralDirectoryOffset).not.toBe(-1)
      const originalDirectory = new DataView(archive.buffer, archive.byteOffset, archive.byteLength)
      const originalSize = originalDirectory.getUint32(centralDirectoryOffset + 24, true)
      const originalCrc = originalDirectory.getUint32(centralDirectoryOffset + 16, true)
      expect(originalSize).toBe(new TextEncoder().encode(payload).byteLength)

      for (const delta of [-1, 1]) {
        const corrupted = new Uint8Array(archive)
        const directory = new DataView(corrupted.buffer, corrupted.byteOffset, corrupted.byteLength)
        directory.setUint32(centralDirectoryOffset + 24, originalSize + delta, true)
        expect(directory.getUint32(centralDirectoryOffset + 16, true)).toBe(originalCrc)
        value.setArchive(corrupted)
        yield* Effect.promise(() => value.seed("old binary"))

        const exit = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
          expect(error).toMatchObject({ message: expect.stringMatching(/size|length/i) })
        }
        expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
        expect(yield* Effect.promise(value.staging)).toEqual([])
      }
    }),
  )

  integration("requires the requested exact release tag", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      value.setPinned({ tag_name: "v1.2.4", draft: false, prerelease: false })

      const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(error.message).toContain("Release tag mismatch")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases/tags/v1.2.3"])
    }),
  )

  integration("keeps the old binary when the checksum does not match", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      value.setChecksums(`${"0".repeat(64)}  ${value.archiveName}\n`)

      const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(error.message).toContain("Checksum mismatch")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects extra, directory, symlink, and traversal tar entries", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "linux", libc: "glibc", format: "ustar" })
      for (const archive of [
        tarArchive([
          { name: "opencode", data: "new binary" },
          { name: "extra", data: "unexpected" },
        ]),
        tarArchive([
          { name: "opencode", data: "new binary" },
          { name: "extra", type: 53 },
        ]),
        tarArchive([{ name: "opencode", type: 50, linkname: "outside" }]),
        tarArchive([{ name: "../opencode", data: "new binary" }]),
      ]) {
        value.setArchive(archive)
        yield* Effect.promise(() => value.seed("old binary"))
        const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
        expect(yield* Effect.promise(value.staging)).toEqual([])
      }
    }),
  )

  integration("rejects a ZIP archive with an extra entry", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      value.setArchive(
        yield* Effect.promise(() =>
          zipArchive([
            { name: "opencode", data: "new binary", externalFileAttributes: unixRegularAttributes },
            { name: "extra", data: "unexpected", externalFileAttributes: unixRegularAttributes },
          ]),
        ),
      )
      yield* Effect.promise(() => value.seed("old binary"))

      const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects a ZIP symlink entry without changing the target", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "darwin", arch: "x64" })
      value.setArchive(
        yield* Effect.promise(() =>
          zipArchive([{ name: "opencode", data: "malicious target", externalFileAttributes: unixSymlinkAttributes }]),
        ),
      )
      yield* Effect.promise(() => value.seed("old binary"))

      const error = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects unknown and symlink installation paths without changing the target", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const unknown = `${value.root}/other/bin/opencode`
      const unknownError = yield* value.compiled.upgrade("1.2.3", unknown).pipe(Effect.flip)
      expect(unknownError.message).toContain("Unsupported fork installation path")
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")

      yield* Effect.promise(value.seedSymlink)
      const symlinkError = yield* value.compiled.upgrade("1.2.3", value.target).pipe(Effect.flip)
      expect(symlinkError.message).toContain("symlink")
      expect(value.requests).toEqual([])
    }),
  )

  integration("rejects symlinked installation parents before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      const installParent = path.dirname(path.dirname(value.target))
      const external = path.join(value.root, "external-install")
      yield* Effect.promise(() => mkdir(path.join(external, "bin"), { recursive: true }))
      yield* Effect.promise(() => mkdir(path.dirname(installParent), { recursive: true }))
      yield* Effect.promise(() => symlink(external, installParent))
      yield* Effect.promise(() => value.seed("old binary"))

      try {
        const error = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
        expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        expect(error.message).toMatch(/symlink|parent/i)
        expect(value.requests).toEqual([])
        expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
        expect(yield* Effect.promise(() => readdir(path.join(external, "bin")))).toEqual(["opencode"])
      } finally {
        yield* Effect.promise(() => rm(installParent, { force: true }))
      }
    }),
  )

  integration("rejects a POSIX installation path with literal backslashes before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      const unmanaged = path.join(value.root, "unmanaged")
      const literalName = ".opencode\\bin\\opencode"
      const literalPath = path.join(unmanaged, literalName)
      const original = Uint8Array.from([0, 17, 34, 128, 200, 255, 10, 0, 42])
      yield* Effect.promise(() => mkdir(unmanaged, { recursive: true }))
      yield* Effect.promise(() => writeFile(literalPath, original))

      const error = yield* value.compiled.upgrade("1.2.3", literalPath).pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("Unsupported fork installation path")
      expect(new Uint8Array(yield* Effect.promise(() => Bun.file(literalPath).arrayBuffer()))).toEqual(original)
      expect(value.requests).toEqual([])
      expect(yield* Effect.promise(() => readdir(unmanaged))).toEqual([literalName])
    }),
  )

  integration("rejects an insecure redirect before following it", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setRedirect("/repos/totalolage/opencode/releases?per_page=100&page=1", "https://evil.invalid/releases")

      const error = yield* value.compiled.latest().pipe(Effect.flip)
      expect(error.message).toContain("Rejected insecure or untrusted update URL")
      expect(value.requests).toEqual(["/repos/totalolage/opencode/releases?per_page=100&page=1"])
    }),
  )

  integration("returns a typed HTTP 404 for an unavailable latest release", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      value.setRedirect("/repos/totalolage/opencode/releases?per_page=100&page=1", `${value.origin}/missing-release`)

      const error = yield* value.compiled.latest().pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("HTTP 404")
      expect(value.requests).toEqual([
        "/repos/totalolage/opencode/releases?per_page=100&page=1",
        "/missing-release",
      ])
    }),
  )

  integration("rejects a compile-defined musl target before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "linux", libc: "musl", format: "ustar" })
      yield* Effect.promise(() => value.seed("old binary"))

      const error = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("musl")
      expect(error.message).toContain(value.compiled.INSTRUCTIONS)
      expect(value.requests).toEqual([])
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects a compile-defined Windows target before network access", () =>
    Effect.gen(function* () {
      const value = yield* fixture({ platform: "win32" })
      yield* Effect.promise(() => value.seed("old binary"))

      const error = yield* value.compiled.upgrade("v1.2.3", value.target).pipe(Effect.flip)
      expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
      expect(error.message).toContain("Windows is unsupported")
      expect(error.message).toContain(value.compiled.INSTRUCTIONS)
      expect(value.requests).toEqual([])
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("preserves a replacement target when it changes during archive download", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const blocked = value.blockArchive()
      const fiber = yield* value.compiled
        .upgrade("v1.2.3", value.target)
        .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
      yield* Effect.promise(() => blocked.ready)

      const replacement = path.join(value.root, "external-replacement")
      yield* Effect.promise(async () => {
        await writeFile(replacement, "external replacement")
        await chmod(replacement, 0o755)
        await rename(replacement, value.target)
      })
      blocked.release()

      const result = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result)) {
        const exit = result.value
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        }
      }
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("external replacement")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("preserves an in-place target change with the same size during archive download", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const before = yield* Effect.promise(() => lstat(value.target, { bigint: true }))
      const blocked = value.blockArchive()
      const fiber = yield* value.compiled
        .upgrade("v1.2.3", value.target)
        .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
      yield* Effect.promise(() => blocked.ready)

      yield* Effect.promise(() => writeFile(value.target, "bad binary"))
      const changedAt = new Date(Number(before.mtimeNs / 1_000_000n + 1_000n))
      yield* Effect.promise(() => utimes(value.target, changedAt, changedAt))
      const changed = yield* Effect.promise(() => lstat(value.target, { bigint: true }))
      expect(changed.size).toBe(before.size)
      expect(changed.mtimeNs).toBeGreaterThan(before.mtimeNs)
      blocked.release()

      const result = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result)) {
        const exit = result.value
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
        }
      }
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("bad binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("rejects a destination parent replacement without touching its replacement stage", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const blocked = value.blockArchive()
      const fiber = yield* value.compiled
        .upgrade("v1.2.3", value.target)
        .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
      yield* Effect.promise(() => blocked.ready)

      const stages = yield* Effect.promise(value.staging)
      expect(stages).toHaveLength(1)
      const stageName = stages[0]
      const originalBin = path.dirname(value.target)
      const backupBin = path.join(value.root, "original-bin")
      const sentinel = path.join(originalBin, stageName, "opencode")
      yield* Effect.promise(async () => {
        await rename(originalBin, backupBin)
        await mkdir(originalBin, { recursive: true })
        await link(path.join(backupBin, "opencode"), value.target)
        await mkdir(path.dirname(sentinel), { recursive: true })
        await writeFile(sentinel, "sentinel")
      })
      blocked.release()

      const result = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result)) {
        const exit = result.value
        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause)
          expect(error).toBeInstanceOf(value.compiled.ForkUpdateError)
          if (error instanceof value.compiled.ForkUpdateError)
            expect(error.message).toMatch(/parent|directory|changed/i)
        }
      }
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(() => readFile(sentinel, "utf8"))).toBe("sentinel")
    }),
  )

  integration("interrupts before rename and cleans the staged directory", () =>
    Effect.gen(function* () {
      const value = yield* fixture()
      yield* Effect.promise(() => value.seed("old binary"))
      const blocked = value.blockArchive()
      const fiber = yield* value.compiled
        .upgrade("1.2.3", value.target)
        .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }))
      yield* Effect.promise(() => blocked.ready)
      yield* Fiber.interrupt(fiber)
      const interrupted = yield* Fiber.await(fiber)
      blocked.release()

      expect(Exit.isFailure(interrupted)).toBe(true)
      expect(new TextDecoder().decode(yield* Effect.promise(value.readTarget))).toBe("old binary")
      expect(yield* Effect.promise(value.staging)).toEqual([])
    }),
  )

  integration("cleans fixture directories and generated modules when compilation fails", () =>
    Effect.gen(function* () {
      const parent = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (value) => Effect.promise(() => value[Symbol.asyncDispose]()),
      )
      const beforeModules = yield* Effect.promise(() => generatedForkModules())
      const entrypoint = path.join(parent.path, "missing-fork-entrypoint.ts")

      expect(yield* Effect.promise(() => readdir(parent.path))).toEqual([])
      const error = yield* Effect.tryPromise({
        try: () => makeForkFixture({ directory: parent.path, entrypoint }),
        catch: (cause) => cause,
      }).pipe(Effect.flip)

      expect(error).toBeInstanceOf(Error)
      expect(yield* Effect.promise(() => readdir(parent.path))).toEqual([])
      expect(yield* Effect.promise(() => generatedForkModules())).toEqual(beforeModules)
    }),
  )
})

const unixRegularAttributes = 0o100755 * 0x10000
const unixSymlinkAttributes = 0o120777 * 0x10000

type ZipEntry = {
  readonly data?: string
  readonly externalFileAttributes: number
  readonly name: string
}

async function zipArchive(entries: ReadonlyArray<ZipEntry>) {
  const writer = new ZipWriter(new Uint8ArrayWriter())
  for (const entry of entries) {
    await writer.add(entry.name, new TextReader(entry.data ?? ""), {
      externalFileAttributes: entry.externalFileAttributes,
      versionMadeBy: 3 * 0x100 + 20,
    })
  }
  return new Uint8Array(await writer.close())
}

async function generatedForkModules() {
  return (await readdir(packageDirectory))
    .filter((entry) => entry.startsWith(".fork-test-") && entry.endsWith(".mjs"))
    .sort()
}

type TarEntry = {
  readonly data?: string
  readonly linkname?: string
  readonly name: string
  readonly type?: number
}

function tarArchive(entries: ReadonlyArray<TarEntry>) {
  const sizes = entries.map((entry) => new TextEncoder().encode(entry.data ?? "").length)
  const length = entries.reduce((sum, _, index) => sum + 512 + Math.ceil(sizes[index] / 512) * 512, 1024)
  const raw = new Uint8Array(length)
  let offset = 0

  entries.forEach((entry, index) => {
    const header = new Uint8Array(512)
    putString(header, 0, 100, entry.name)
    putOctal(header, 100, 8, entry.type === 50 ? 0o777 : 0o755)
    putOctal(header, 108, 8, 0)
    putOctal(header, 116, 8, 0)
    putOctal(header, 124, 12, sizes[index])
    putOctal(header, 136, 12, 0)
    header[156] = entry.type ?? 48
    putString(header, 157, 100, entry.linkname ?? "")
    putString(header, 257, 6, "ustar\0")
    putString(header, 263, 2, "00")
    header.fill(32, 148, 156)
    const checksum = header.reduce((sum, byte) => sum + byte, 0)
    putString(header, 148, 6, checksum.toString(8).padStart(6, "0"))
    header[154] = 0
    header[155] = 32
    raw.set(header, offset)
    offset += 512
    const data = new TextEncoder().encode(entry.data ?? "")
    raw.set(data, offset)
    offset += Math.ceil(data.length / 512) * 512
  })

  return new Uint8Array(gzipSync(raw))
}

function putString(target: Uint8Array, start: number, length: number, value: string) {
  target.set(new TextEncoder().encode(value).subarray(0, length), start)
}

function putOctal(target: Uint8Array, start: number, length: number, value: number) {
  putString(target, start, length - 1, value.toString(8).padStart(length - 1, "0"))
}

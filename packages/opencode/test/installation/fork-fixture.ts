import { createHash } from "node:crypto"
import { chmod, mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { pathToFileURL } from "node:url"

type ForkModule = typeof import("../../src/installation/fork")

const repositoryPath = "/repos/totalolage/opencode"
const assetPath = "/totalolage/opencode/releases/download/v1.2.3"
const version = "1.2.3"

export type ForkFixtureOptions = {
  readonly platform?: "linux" | "darwin" | "win32"
  readonly arch?: "x64" | "arm64"
  readonly libc?: "glibc" | "musl"
  readonly format?: "gnu" | "ustar"
  readonly directory?: string
  readonly entrypoint?: string
  readonly compileOrigin?: string
}

export type ForkFixture = {
  readonly archiveName: string
  readonly compiled: ForkModule
  readonly origin: string
  readonly requests: string[]
  readonly root: string
  readonly target: string
  readonly close: () => Promise<void>
  readonly blockArchive: () => { readonly ready: Promise<void>; readonly release: () => void }
  readonly readTarget: () => Promise<Uint8Array>
  readonly seed: (contents: string) => Promise<void>
  readonly seedSymlink: () => Promise<void>
  readonly setArchive: (archive: Uint8Array) => void
  readonly setChecksums: (checksums: string) => void
  readonly setLatest: (release: unknown) => void
  readonly setPinned: (release: unknown) => void
  readonly setRedirect: (pathname: string, location: string) => void
  readonly staging: () => Promise<string[]>
}

type ArchiveBlock = {
  readonly done: Promise<void>
  readonly ready: Promise<void>
  readonly release: () => void
  readonly signalReady: () => void
}

export async function makeForkFixture(options: ForkFixtureOptions = {}): Promise<ForkFixture> {
  const root = await mkdtemp(path.join(options.directory ?? os.tmpdir(), "opencode-fork-test-"))
  const source = path.join(root, "source")
  const platform = options.platform ?? process.platform
  const arch = options.arch ?? process.arch
  const archivePlatform = platform === "darwin" ? "darwin" : "linux"
  const archiveArch = arch === "arm64" ? "arm64" : "x64"
  const archiveExtension = archivePlatform === "darwin" ? "zip" : "tar.gz"
  const archiveName = `opencode-${archivePlatform}-${archiveArch}.${archiveExtension}`
  const archivePath = path.join(root, archiveName)
  const target = path.join(root, "home", ".opencode", "bin", "opencode")
  const modulePath = path.join(path.resolve(import.meta.dir, "../.."), `.fork-test-${path.basename(root)}.mjs`)
  const requests: string[] = []
  const redirects = new Map<string, string>()
  let archive = new Uint8Array(0)
  let checksums = ""
  let latest: unknown = { tag_name: `v${version}`, draft: false, prerelease: false }
  let pinned: unknown = { tag_name: `v${version}`, draft: false, prerelease: false }
  let archiveBlock: ArchiveBlock | undefined
  let server: ReturnType<typeof Bun.serve> | undefined
  let moduleOwned = false

  const cleanup = async () => {
    archiveBlock?.release()
    try {
      server?.stop(true)
    } finally {
      try {
        await rm(root, { force: true, recursive: true })
      } finally {
        if (moduleOwned) await rm(modulePath, { force: true })
      }
    }
  }

  try {
    await mkdir(source, { recursive: true })
    const sourceFile = path.join(source, "opencode")
    await writeFile(sourceFile, "new binary")
    await chmod(sourceFile, 0o755)
    await createArchive(source, archivePath, archivePlatform, options.format)

    archive = new Uint8Array(await Bun.file(archivePath).arrayBuffer())
    checksums = checksum(archiveName, archive)

    server = Bun.serve({
      fetch(request) {
        const pathname = new URL(request.url).pathname
        requests.push(pathname)
        const redirect = redirects.get(pathname)
        if (redirect !== undefined) return new Response(null, { headers: { Location: redirect }, status: 302 })

        if (pathname === `${repositoryPath}/releases/latest`) return json(latest)
        if (pathname === `${repositoryPath}/releases/tags/v${version}`) return json(pinned)
        if (pathname === `${assetPath}/${archiveName}`) {
          const block = archiveBlock
          if (block !== undefined) {
            block.signalReady()
            let cancelled = false
            return new Response(
              new ReadableStream<Uint8Array>({
                cancel() {
                  cancelled = true
                },
                start(controller) {
                  controller.enqueue(archive)
                  void block.done.then(
                    () => {
                      if (cancelled) return
                      controller.close()
                    },
                    (cause) => controller.error(cause),
                  )
                },
              }),
            )
          }
          return new Response(archive)
        }
        if (pathname === `${assetPath}/SHA256SUMS`) return new Response(checksums)
        return new Response("not found", { status: 404 })
      },
      hostname: "127.0.0.1",
      port: 0,
    })

    const origin = new URL(server.url).origin
    const compiled = await compileFork(origin, root, modulePath, options, () => {
      moduleOwned = true
    })

    return {
      archiveName,
      compiled,
      origin,
      requests,
      root,
      target,
      close: cleanup,
      blockArchive() {
        archiveBlock?.release()
        archiveBlock = makeArchiveBlock()
        return archiveBlock
      },
      async readTarget() {
        return new Uint8Array(await Bun.file(target).arrayBuffer())
      },
      async seed(contents) {
        await rm(target, { force: true })
        await mkdir(path.dirname(target), { recursive: true })
        await writeFile(target, contents)
        await chmod(target, 0o755)
      },
      async seedSymlink() {
        await rm(target, { force: true })
        await mkdir(path.dirname(target), { recursive: true })
        const old = path.join(path.dirname(target), "old-opencode")
        await writeFile(old, "old binary")
        await chmod(old, 0o755)
        await symlink(old, target)
      },
      setArchive(next) {
        const copy = new Uint8Array(next.byteLength)
        copy.set(next)
        archive = copy
        checksums = checksum(archiveName, archive)
      },
      setChecksums(next) {
        checksums = next
      },
      setLatest(next) {
        latest = next
      },
      setPinned(next) {
        pinned = next
      },
      setRedirect(pathname, location) {
        redirects.set(pathname, location)
      },
      async staging() {
        return (await readdir(path.dirname(target))).filter((entry) => entry.startsWith(".opencode-update-"))
      },
    }
  } catch (cause) {
    await cleanup()
    throw cause
  }
}

function checksum(filename: string, bytes: Uint8Array) {
  return `${createHash("sha256").update(bytes).digest("hex")}  ${filename}\n`
}

function json(value: unknown) {
  return new Response(JSON.stringify(value), { headers: { "content-type": "application/json" } })
}

async function createArchive(
  source: string,
  archivePath: string,
  platform: "linux" | "darwin",
  format: ForkFixtureOptions["format"],
) {
  const command =
    platform === "darwin"
      ? ["/usr/bin/zip", "-q", archivePath, "opencode"]
      : [
          "/usr/bin/tar",
          ...(format === "gnu" ? ["--format=gnu"] : ["--format=ustar"]),
          "-czf",
          archivePath,
          "-C",
          source,
          "opencode",
        ]
  const archiveProcess = Bun.spawn(command, {
    cwd: platform === "darwin" ? source : undefined,
    stderr: "pipe",
    stdout: "pipe",
  })
  const [archiveCode, archiveStderr] = await Promise.all([
    archiveProcess.exited,
    new Response(archiveProcess.stderr).text(),
    new Response(archiveProcess.stdout).text(),
  ]).then(([code, stderr]) => [code, stderr] as const)
  if (archiveCode !== 0) throw new Error(`failed to create fixture archive: ${archiveStderr}`)
}

function makeArchiveBlock(): ArchiveBlock {
  let resolveDone: (() => void) | undefined
  let resolveReady: (() => void) | undefined
  let released = false
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve
  })
  const ready = new Promise<void>((resolve) => {
    resolveReady = resolve
  })
  return {
    done,
    ready,
    release() {
      if (released) return
      released = true
      resolveDone?.()
    },
    signalReady() {
      resolveReady?.()
    },
  }
}

async function compileFork(
  origin: string,
  root: string,
  modulePath: string,
  options: ForkFixtureOptions,
  ownModule: () => void,
): Promise<ForkModule> {
  const result = await Bun.build({
    entrypoints: [options.entrypoint ?? path.resolve(import.meta.dir, "../../src/installation/fork.ts")],
    define: {
      OPENCODE_DISTRIBUTION: JSON.stringify("totalolage/opencode"),
      OPENCODE_UPDATE_TEST_ORIGIN: JSON.stringify(options.compileOrigin ?? origin),
      ...(options.platform === undefined ? {} : { "process.platform": JSON.stringify(options.platform) }),
      ...(options.arch === undefined ? {} : { "process.arch": JSON.stringify(options.arch) }),
      ...(options.libc === undefined ? {} : { OPENCODE_LIBC: JSON.stringify(options.libc) }),
    },
    external: ["effect", "semver", "@zip.js/zip.js", "@opencode-ai/core"],
    format: "esm",
    outdir: root,
    target: "bun",
  })
  if (!result.success) throw new Error(result.logs.map((log) => log.message).join("\n"))
  if (await Bun.file(modulePath).exists()) throw new Error(`fork fixture module already exists: ${modulePath}`)
  ownModule()
  await Bun.write(modulePath, result.outputs[0])
  return (await import(pathToFileURL(modulePath).href)) as ForkModule
}

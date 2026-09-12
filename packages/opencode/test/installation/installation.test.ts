import { describe, expect, test } from "bun:test"
import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { Effect, Layer, Stream } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { Installation } from "../../src/installation"
import { InstallationChannel } from "@opencode-ai/core/installation/version"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { testEffect } from "../lib/effect"
import { Fork } from "../../src/installation/fork"
import path from "path"

const encoder = new TextEncoder()
const packageDirectory = path.resolve(import.meta.dir, "../..")
const upstreamSubprocessTimeout = 30_000

function mockHttpClient(handler: (request: HttpClientRequest.HttpClientRequest) => Response) {
  const client = HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, handler(request))))
  return Layer.succeed(HttpClient.HttpClient, client)
}

function mockSpawner(
  handler: (cmd: string, args: readonly string[]) => string | { code: number; stdout?: string; stderr?: string } = () =>
    "",
) {
  const spawner = ChildProcessSpawner.make((command) => {
    const std = ChildProcess.isStandardCommand(command) ? command : undefined
    const result = handler(std?.command ?? "", std?.args ?? [])
    const output = typeof result === "string" ? { code: 0, stdout: result, stderr: "" } : result
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(0),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(output.code)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: { [Symbol.for("effect/Sink/TypeId")]: Symbol.for("effect/Sink/TypeId") } as any,
        stdout: output.stdout ? Stream.make(encoder.encode(output.stdout)) : Stream.empty,
        stderr: output.stderr ? Stream.make(encoder.encode(output.stderr)) : Stream.empty,
        all: Stream.empty,
        getInputFd: () => ({ [Symbol.for("effect/Sink/TypeId")]: Symbol.for("effect/Sink/TypeId") }) as any,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      }),
    )
  })
  return Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)
}

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  })
}

function testLayer(
  httpHandler: (request: HttpClientRequest.HttpClientRequest) => Response,
  spawnHandler?: (cmd: string, args: readonly string[]) => string | { code: number; stdout?: string; stderr?: string },
) {
  const spawnerNode = makeGlobalNode({
    service: ChildProcessSpawner.ChildProcessSpawner,
    layer: mockSpawner(spawnHandler),
    deps: [],
  })
  return LayerNode.compile(Installation.node, [
    [httpClient, mockHttpClient(httpHandler)],
    [CrossSpawnSpawner.node, spawnerNode],
  ])
}

describe("installation", () => {
  if (Fork.IS_FORK) {
    test(
      "runs upstream installation assertions in an explicit upstream subprocess",
      async () => {
        const result = await runUpstreamInstallationTests()
        const output = `${result.stdout}\n${result.stderr}`
        expect(result.exitCode).toBe(0)
        expect(output).toContain("15 pass")
        expect(output).toContain("Ran 15 tests across 1 file")
      },
      { timeout: upstreamSubprocessTimeout + 5_000 },
    )
  }

  const forkUrls: string[] = []
  const forkProbes: string[] = []
  testEffect(
    testLayer(
      (request) => {
        forkUrls.push(request.url)
        return Fork.IS_FORK
          ? jsonResponse({ tag_name: "v2.0.0", draft: false, prerelease: false })
          : jsonResponse({ version: "2.0.0" })
      },
      (cmd, args) => {
        forkProbes.push([cmd, ...args].join(" "))
        return ""
      },
    ),
  ).effect("uses the fork release lookup instead of the requested package-manager method", () =>
    Effect.gen(function* () {
      const result = yield* Installation.use.latest("npm")
      expect(result).toBe("2.0.0")
      if (!Fork.IS_FORK) return
      expect(forkUrls).toEqual(["https://api.github.com/repos/totalolage/opencode/releases/latest"])
      expect(forkProbes).toEqual([])
    }),
  )

  testEffect(testLayer(() => new Response("{", { status: 200 }))).effect(
    "returns a typed error when the latest release cannot be decoded",
    () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(Installation.use.latest("curl"))
        expect(error).toBeInstanceOf(Installation.UpgradeFailedError)
        expect(error.stderr).toBe(error.message)
      }),
  )

  testEffect(testLayer(() => new Response("{", { status: 200 }))).effect(
    "returns a typed error when installation info cannot be loaded",
    () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(Installation.use.info())
        expect(error).toBeInstanceOf(Installation.UpgradeFailedError)
        expect(error.stderr).toBe(error.message)
      }),
  )

  if (!Fork.IS_FORK) {
    describe("latest", () => {
      testEffect(testLayer(() => jsonResponse({ tag_name: "v1.2.3" }))).effect(
        "reads release version from GitHub releases",
        () =>
          Effect.gen(function* () {
            const result = yield* Installation.use.latest("unknown")
            expect(result).toBe("1.2.3")
          }),
      )

      testEffect(testLayer(() => jsonResponse({ tag_name: "v4.0.0-beta.1" }))).effect(
        "strips v prefix from GitHub release tag",
        () =>
          Effect.gen(function* () {
            const result = yield* Installation.use.latest("curl")
            expect(result).toBe("4.0.0-beta.1")
          }),
      )

      const npmCalls: string[] = []
      testEffect(
        testLayer((request) => {
          npmCalls.push(request.url)
          return jsonResponse({ version: "1.5.0" })
        }),
      ).effect("reads npm versions via registry", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("npm")
          expect(result).toBe("1.5.0")
          expect(npmCalls).toContain(`https://registry.npmjs.org/opencode-ai/${InstallationChannel}`)
        }),
      )

      const bunCalls: string[] = []
      testEffect(
        testLayer((request) => {
          bunCalls.push(request.url)
          return jsonResponse({ version: "1.6.0" })
        }),
      ).effect("reads bun versions via registry", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("bun")
          expect(result).toBe("1.6.0")
          expect(bunCalls).toContain(`https://registry.npmjs.org/opencode-ai/${InstallationChannel}`)
        }),
      )

      const pnpmCalls: string[] = []
      testEffect(
        testLayer((request) => {
          pnpmCalls.push(request.url)
          return jsonResponse({ version: "1.7.0" })
        }),
      ).effect("reads pnpm versions via registry", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("pnpm")
          expect(result).toBe("1.7.0")
          expect(pnpmCalls).toContain(`https://registry.npmjs.org/opencode-ai/${InstallationChannel}`)
        }),
      )

      testEffect(testLayer(() => jsonResponse({ version: "2.3.4" }))).effect("reads scoop manifest versions", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("scoop")
          expect(result).toBe("2.3.4")
        }),
      )

      testEffect(testLayer(() => jsonResponse({ d: { results: [{ Version: "3.4.5" }] } }))).effect(
        "reads chocolatey feed versions",
        () =>
          Effect.gen(function* () {
            const result = yield* Installation.use.latest("choco")
            expect(result).toBe("3.4.5")
          }),
      )

      testEffect(
        testLayer(
          () => jsonResponse({ versions: { stable: "2.0.0" } }),
          (cmd, args) => {
            // getBrewFormula: return core formula (no tap)
            if (cmd === "brew" && args.includes("--formula") && args.includes("anomalyco/tap/opencode")) return ""
            if (cmd === "brew" && args.includes("--formula") && args.includes("opencode")) return "opencode"
            return ""
          },
        ),
      ).effect("reads brew formulae API versions", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("brew")
          expect(result).toBe("2.0.0")
        }),
      )

      const brewInfoJson = JSON.stringify({
        formulae: [{ versions: { stable: "2.1.0" } }],
      })
      testEffect(
        testLayer(
          () => jsonResponse({}), // HTTP not used for tap formula
          (cmd, args) => {
            if (cmd === "brew" && args.includes("anomalyco/tap/opencode") && args.includes("--formula"))
              return "opencode"
            if (cmd === "brew" && args.includes("--json=v2")) return brewInfoJson
            return ""
          },
        ),
      ).effect("reads brew tap info JSON via CLI", () =>
        Effect.gen(function* () {
          const result = yield* Installation.use.latest("brew")
          expect(result).toBe("2.1.0")
        }),
      )
    })

    describe("upgrade", () => {
      testEffect(
        testLayer(
          () => jsonResponse({}),
          (cmd) => {
            if (cmd === "npm") return { code: 1, stderr: "token=secret command output" }
            return ""
          },
        ),
      ).effect("returns sanitized typed errors for failed package upgrades", () =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(Installation.use.upgrade("npm", "9.9.9"))
          expect(error).toBeInstanceOf(Installation.UpgradeFailedError)
          expect(error.stderr).toBe("Upgrade failed for npm (exit code 1).")
          expect(error.message).toBe(error.stderr)
          expect(error.stderr).not.toContain("secret")
          expect(error.stderr).not.toContain("command output")
        }),
      )

      testEffect(
        testLayer(
          () => new Response("install script with token=secret", { status: 200 }),
          (cmd, args) => {
            if (cmd === "bash" && args[0] === "--version") return "GNU bash"
            if (cmd === "bash" || cmd === "sh") return { code: 1, stderr: "script output with token=secret" }
            return ""
          },
        ),
      ).effect("returns sanitized typed errors when the curl install script fails", () =>
        Effect.gen(function* () {
          const error = yield* Effect.flip(Installation.use.upgrade("curl", "9.9.9"))
          expect(error).toBeInstanceOf(Installation.UpgradeFailedError)
          expect(error.stderr).toBe("Upgrade failed for curl (exit code 1).")
          expect(error.message).toBe(error.stderr)
          expect(error.stderr).not.toContain("secret")
          expect(error.stderr).not.toContain("script output")
        }),
      )

      testEffect(
        testLayer(
          () => new Response("install script", { status: 200 }),
          (cmd, args) => {
            if (cmd === "bash" && args[0] === "--version") return { code: 1, stderr: "missing" }
            if (cmd === "bash") return { code: 1, stderr: "should not execute installer with bash" }
            if (cmd === "sh") return "ok"
            return ""
          },
        ),
      ).effect("falls back to sh when bash is unavailable during curl upgrade", () =>
        Effect.gen(function* () {
          yield* Installation.use.upgrade("curl", "9.9.9")
        }),
      )
    })
  }
})

async function runUpstreamInstallationTests() {
  const child = Bun.spawn(
    [process.execPath, "test", "--timeout=30000", "--define", 'OPENCODE_DISTRIBUTION="upstream"', import.meta.path],
    {
      cwd: packageDirectory,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    },
  )
  const output = Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
  let timer: ReturnType<typeof setTimeout> | undefined

  try {
    return await Promise.race([
      output.then(([exitCode, stdout, stderr]) => ({ exitCode, stdout, stderr })),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("upstream installation test subprocess timed out")),
          upstreamSubprocessTimeout,
        )
      }),
    ])
  } catch (error) {
    child.kill()
    await child.exited
    await output
    throw error
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

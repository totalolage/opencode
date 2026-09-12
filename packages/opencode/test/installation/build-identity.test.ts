import { describe, expect, test } from "bun:test"
import path from "path"

type ScriptSnapshot = {
  channel: string
  distribution: string
  forkTestOrigin: string
  forkRelease: boolean
  preview: boolean
  release: boolean
  upstream: boolean
  version: string
}

type ScriptResult = {
  exitCode: number
  script?: ScriptSnapshot
  stderr: string
  stdout: string
}

const SCRIPT_MARKER = "__OPENCODE_SCRIPT__"
const buildScriptPath = path.resolve(import.meta.dir, "../../script/build.ts")
const packageDirectory = path.resolve(import.meta.dir, "../..")
const identityEnvironment = new Set([
  "OPENCODE_CHANNEL",
  "OPENCODE_BUMP",
  "OPENCODE_VERSION",
  "OPENCODE_RELEASE",
  "OPENCODE_UPSTREAM_BUILD",
  "OPENCODE_FORK_RELEASE",
  "OPENCODE_FORK_TEST_ORIGIN",
])

async function runScript(overrides: Record<string, string>): Promise<ScriptResult> {
  const environment: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !identityEnvironment.has(key)) environment[key] = value
  }
  Object.assign(environment, overrides)

  const child = Bun.spawn(
    [
      process.execPath,
      "-e",
      `const { Script } = await import("@opencode-ai/script"); process.stdout.write(${JSON.stringify(SCRIPT_MARKER)} + JSON.stringify({ channel: Script.channel, distribution: Script.distribution, forkRelease: Script.forkRelease, forkTestOrigin: Script.forkTestOrigin, preview: Script.preview, release: Script.release, upstream: Script.upstream, version: Script.version }))`,
    ],
    {
      cwd: packageDirectory,
      env: environment,
      stderr: "pipe",
      stdout: "pipe",
    },
  )
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ])
  const line = stdout.split(/\r?\n/).findLast((value) => value.startsWith(SCRIPT_MARKER))
  return {
    exitCode,
    script: line ? (JSON.parse(line.slice(SCRIPT_MARKER.length)) as ScriptSnapshot) : undefined,
    stderr,
    stdout,
  }
}

function successful(result: ScriptResult) {
  if (result.exitCode !== 0 || !result.script) throw new Error(`${result.stdout}\n${result.stderr}`)
  return result.script
}

async function expectFailure(overrides: Record<string, string>, message: string) {
  const result = await runScript(overrides)
  expect(result.exitCode).not.toBe(0)
  expect(`${result.stdout}\n${result.stderr}`).toContain(message)
}

type BuildResult = {
  exitCode: number
  stderr: string
  stdout: string
}

const buildIdentityEnvironment = new Set([
  ...identityEnvironment,
  "OPENCODE_UPDATE_TEST_BUILD",
  "OPENCODE_UPDATE_TEST_ORIGIN",
])

async function runBuild(args: string[], overrides: Record<string, string>): Promise<BuildResult> {
  const environment: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !buildIdentityEnvironment.has(key)) environment[key] = value
  }
  Object.assign(environment, overrides)

  const child = Bun.spawn([process.execPath, "run", buildScriptPath, ...args], {
    cwd: packageDirectory,
    env: environment,
    stderr: "pipe",
    stdout: "pipe",
  })
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ])
  return { exitCode, stderr, stdout }
}

async function expectBuildGuard(args: string[], overrides: Record<string, string>, message: string) {
  const result = await runBuild(args, overrides)
  const output = `${result.stdout}\n${result.stderr}`
  expect(result.exitCode).not.toBe(0)
  expect(output).toContain(message)
  expect(output).not.toContain("Loaded models.dev snapshot")
  expect(output).not.toContain("Building Web UI to embed in the binary")
}

describe("build identity", () => {
  test("defaults to the fork and uses a local version without an upstream lookup", async () => {
    const script = successful(await runScript({ OPENCODE_CHANNEL: "latest" }))

    expect(script).toMatchObject({
      channel: "latest",
      distribution: "totalolage/opencode",
      forkTestOrigin: "",
      forkRelease: false,
      upstream: false,
    })
    expect(script.version).toMatch(/^0\.0\.0-latest-\d{12}$/)
  })

  test("opts into the upstream identity explicitly", async () => {
    const script = successful(
      await runScript({
        OPENCODE_CHANNEL: "latest",
        OPENCODE_UPSTREAM_BUILD: "1",
        OPENCODE_VERSION: "1.2.3",
      }),
    )

    expect(script).toMatchObject({
      channel: "latest",
      distribution: "upstream",
      forkRelease: false,
      upstream: true,
      version: "1.2.3",
    })
  })

  test("accepts a strict stable fork release identity", async () => {
    const script = successful(
      await runScript({
        OPENCODE_CHANNEL: "latest",
        OPENCODE_FORK_RELEASE: "1",
        OPENCODE_FORK_TEST_ORIGIN: "http://127.0.0.1:4173",
        OPENCODE_VERSION: "1.2.3",
      }),
    )

    expect(script).toMatchObject({
      channel: "latest",
      distribution: "totalolage/opencode",
      forkTestOrigin: "http://127.0.0.1:4173",
      forkRelease: true,
      upstream: false,
      version: "1.2.3",
    })
  })

  test("rejects a fork release without an explicit version", async () => {
    await expectFailure(
      {
        OPENCODE_CHANNEL: "latest",
        OPENCODE_FORK_RELEASE: "1",
      },
      "explicit stable X.Y.Z",
    )
  })

  test("rejects a fork release without the latest channel", async () => {
    await expectFailure(
      {
        OPENCODE_CHANNEL: "dev",
        OPENCODE_FORK_RELEASE: "1",
        OPENCODE_VERSION: "1.2.3",
      },
      "OPENCODE_CHANNEL=latest",
    )
  })

  test("rejects conflicting upstream and fork release modes", async () => {
    await expectFailure(
      {
        OPENCODE_CHANNEL: "latest",
        OPENCODE_FORK_RELEASE: "1",
        OPENCODE_UPSTREAM_BUILD: "1",
        OPENCODE_VERSION: "1.2.3",
      },
      "cannot be used with OPENCODE_UPSTREAM_BUILD=1",
    )
  })

  for (const version of ["v1.2.3", "01.2.3", "1.02.3", "1.2.03", "1.2.3-alpha.1", "1.2.3+build"]) {
    test(`rejects non-stable fork release version ${version}`, async () => {
      await expectFailure(
        {
          OPENCODE_CHANNEL: "latest",
          OPENCODE_FORK_RELEASE: "1",
          OPENCODE_VERSION: version,
        },
        "explicit stable X.Y.Z",
      )
    })
  }

  test("exposes the fork update test origin for build wiring", async () => {
    const script = successful(
      await runScript({
        OPENCODE_CHANNEL: "dev",
        OPENCODE_FORK_TEST_ORIGIN: "http://127.0.0.1:4173",
        OPENCODE_VERSION: "1.2.3",
      }),
    )

    expect(script.forkTestOrigin).toBe("http://127.0.0.1:4173")
  })

  test("wires build identity and fork release targets into the build", async () => {
    const source = await Bun.file(buildScriptPath).text()

    expect(source).toContain("const forkReleaseTargets = allTargets.filter")
    expect(source).toContain("const buildTargets = Script.forkRelease ? forkReleaseTargets : allTargets")
    expect(source).toContain('const forkUpdateTestFlag = process.argv.includes("--fork-update-test")')
    expect(source).toContain("OPENCODE_FORK_TEST_ORIGIN requires --fork-update-test")
    expect(source).toContain("if (forkUpdateTestFlag && Script.release)")
    expect(source).not.toContain("if (forkUpdateTestFlag && (Script.release || Script.forkRelease))")
    expect(source).toContain('item.avx2 === false ? "baseline" : undefined')
    expect(source).toContain('target: name.replace(pkg.name, "bun") as any')
    expect(source).not.toContain("!Script.forkRelease")
    expect(source).toContain("OPENCODE_DISTRIBUTION: JSON.stringify(Script.distribution)")
    expect(source).toContain('OPENCODE_UPDATE_TEST_ORIGIN: JSON.stringify(forkTestOrigin ?? "")')
    expect(source).toContain("if (Script.release && Script.upstream)")
    expect(source).not.toContain("OPENCODE_UPDATE_TEST_BUILD")
  })
})

describe("build.ts fixture guards", () => {
  test("rejects a supplied fixture origin without --fork-update-test", async () => {
    await expectBuildGuard(
      [],
      {
        OPENCODE_CHANNEL: "dev",
        OPENCODE_FORK_TEST_ORIGIN: "http://127.0.0.1:4173",
        OPENCODE_VERSION: "1.2.3",
      },
      "OPENCODE_FORK_TEST_ORIGIN requires --fork-update-test",
    )
  })

  for (const origin of [
    "http://example.com:4173",
    "http://localhost:4173",
    "https://127.0.0.1:4173",
    "http://127.0.0.1:4173/path",
    "http://user@127.0.0.1:4173",
    "http://127.0.0.1:4173?test=1",
    "http://127.0.0.1:4173#test",
    "http://127.0.0.1:0",
    "http://127.0.0.1:65536",
    "http://127.0.0.1:4173\n",
    "http://127.0.0.1:4173\r\n",
    "http://127.0.0.1:4173 ",
    " http://127.0.0.1:4173",
  ]) {
    test(`rejects invalid fixture origin ${JSON.stringify(origin)}`, async () => {
      await expectBuildGuard(
        ["--fork-update-test"],
        {
          OPENCODE_CHANNEL: "dev",
          OPENCODE_FORK_TEST_ORIGIN: origin,
          OPENCODE_VERSION: "1.2.3",
        },
        "OPENCODE_FORK_TEST_ORIGIN must be http://127.0.0.1:PORT without a path, credentials, query, or hash",
      )
    })
  }

  test("rejects fixture mode in release mode", async () => {
    await expectBuildGuard(
      ["--fork-update-test"],
      {
        OPENCODE_CHANNEL: "latest",
        OPENCODE_FORK_RELEASE: "1",
        OPENCODE_FORK_TEST_ORIGIN: "http://127.0.0.1:4173",
        OPENCODE_RELEASE: "1",
        OPENCODE_VERSION: "1.2.3",
      },
      "--fork-update-test cannot be used in release mode",
    )
  })
})

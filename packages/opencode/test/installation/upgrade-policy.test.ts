import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Installation } from "../../src/installation"
import { Fork } from "../../src/installation/fork"
import { tmpdir } from "../fixture/fixture"

const packageDirectory = path.resolve(import.meta.dir, "../..")
const upstreamSubprocessTimeout = 30_000
const configOverrides = new Set(["OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG", "OPENCODE_CONFIG_DIR"])

describe("upgrade policy", () => {
  if (Fork.IS_FORK) {
    test(
      "preserves upstream preview policy in an explicit upstream subprocess",
      async () => {
        const result = await runUpstreamUpgradePolicyTests()
        const output = `${result.stdout}\n${result.stderr}`
        expect(result.exitCode).toBe(0)
        expect(output).toContain("14 pass")
        expect(output).toContain("Ran 14 tests across 1 file")
      },
      { timeout: upstreamSubprocessTimeout + 5_000 },
    )
  }

  test("installs a newer patch release", () => {
    expect(decide({ latest: "1.2.4" })).toBe("install")
  })

  test("notifies for newer minor and major releases", () => {
    expect(decide({ latest: "1.3.0" })).toBe("notify")
    expect(decide({ latest: "2.0.0" })).toBe("notify")
  })

  test("does not notify when the latest release is equal or lower", () => {
    expect(decide({ latest: "1.2.3", alwaysNotify: true })).toBe("noop")
    expect(decide({ latest: "1.2.2", autoupdate: "notify" })).toBe("noop")
  })

  test("configured and forced notifications do not install patch updates", () => {
    expect(decide({ latest: "1.2.4", autoupdate: "notify" })).toBe("notify")
    expect(decide({ latest: "1.2.4", alwaysNotify: true })).toBe("notify")
  })

  test("does not install when the installed method is unknown", () => {
    expect(decide({ latest: "1.2.4", method: "unknown" })).toBe("noop")
    expect(decide({ latest: "1.3.0", method: "unknown" })).toBe("notify")
  })

  test(
    Fork.IS_FORK ? "treats local, prerelease, and malformed versions as no update" : "updates newer upstream previews",
    () => {
      expect(decide({ current: "local", latest: "1.2.4" })).toBe("noop")
      expect(decide({ current: "1.2", latest: "1.2.4" })).toBe("noop")
      expect(decide({ current: "1.2.3", latest: "1.2.3+build" })).toBe("noop")

      if (!Fork.IS_FORK) {
        expect(decide({ current: "1.2.3-alpha.1", latest: "1.2.4" })).toBe("install")
        expect(decide({ current: "1.2.3", latest: "1.2.4-beta.1" })).toBe("install")
        expect(decide({ current: "1.2.3", latest: "1.3.0-beta.1" })).toBe("notify")
        expect(decide({ current: "1.2.3", latest: "1.2.3" })).toBe("noop")
        expect(decide({ current: "1.2.3", latest: "1.2.2" })).toBe("noop")
        return
      }

      expect(decide({ current: "1.2.3-alpha.1", latest: "1.2.4" })).toBe("noop")
      expect(decide({ current: "1.2.3", latest: "1.2.4-beta.1" })).toBe("noop")
    },
  )

  test("preserves disabled configuration", () => {
    expect(decide({ latest: "1.2.4", autoupdate: false })).toBe("noop")
  })
})

describe("manual fork upgrade policy", () => {
  test("allows an explicit stable downgrade with an optional v prefix", () => {
    expect(
      Installation.decideManualUpgrade({
        current: "2.0.0",
        requested: "v1.2.3",
      }),
    ).toEqual({ type: "upgrade", target: "1.2.3" })
  })

  test("rejects an invalid explicit target before resolving latest", () => {
    expect(
      Installation.decideManualUpgrade({
        current: "1.2.3",
        requested: "1.2",
        latest: "2.0.0",
      }),
    ).toEqual({ type: "instructions" })
  })

  test("requires a latest lookup for an implicit target", () => {
    expect(Installation.decideManualUpgrade({ current: "1.2.3" })).toEqual({ type: "lookup" })
  })

  test("rejects malformed resolved latest metadata", () => {
    expect(Installation.decideManualUpgrade({ current: "1.2.3", latest: "1.2" })).toEqual({ type: "instructions" })
  })

  test("skips an implicit equal or lower latest release", () => {
    expect(Installation.decideManualUpgrade({ current: "1.2.3", latest: "1.2.3" })).toEqual({ type: "skip" })
    expect(Installation.decideManualUpgrade({ current: "1.2.3", latest: "1.2.2" })).toEqual({ type: "skip" })
  })

  test("requires an explicit target when the current version is invalid", () => {
    expect(Installation.decideManualUpgrade({ current: "local", latest: "2.0.0" })).toEqual({ type: "instructions" })
  })
})

describe("fork upgrade method policy", () => {
  test("does not let a requested curl method bypass an unknown actual method", () => {
    expect(Installation.canUseForkUpgrade({ actual: "unknown", requested: "curl" })).toBe(false)
    expect(Installation.canUseForkUpgrade({ actual: "curl", requested: "npm" })).toBe(false)
    expect(Installation.canUseForkUpgrade({ actual: "curl" })).toBe(true)
  })
})

describe("fork timestamped update policy", () => {
  if (!Fork.IS_FORK) return

  const oldTimestamp = "1.18.30-f8y-20260913140000"
  const newTimestamp = "1.18.30-f8y-20260913200000"

  test("accepts a concrete fork timestamped release", () => {
    expect(Fork.supportedVersion(oldTimestamp)).toBe(oldTimestamp)
    expect(Installation.decideManualUpgrade({ current: "1.18.29", requested: oldTimestamp })).toEqual({
      type: "upgrade",
      target: oldTimestamp,
    })
  })

  test("installs a newer timestamp and ignores an older one", () => {
    expect(decide({ current: oldTimestamp, latest: newTimestamp, method: "curl" })).toBe("install")
    expect(decide({ current: newTimestamp, latest: oldTimestamp, method: "curl" })).toBe("noop")
  })

  test("allows a suffix to upgrade to the same-base stable release", () => {
    expect(decide({ current: oldTimestamp, latest: "1.18.30", method: "curl" })).toBe("install")
  })

  test("does not auto-downgrade a stable release to a same-base suffix", () => {
    expect(decide({ current: "1.18.30", latest: oldTimestamp, method: "curl" })).toBe("noop")
    expect(Installation.decideManualUpgrade({ current: "1.18.30", latest: oldTimestamp })).toEqual({
      type: "skip",
    })
  })

  test("installs a higher-core suffix over a lower stable release", () => {
    expect(decide({ current: "1.18.29", latest: newTimestamp, method: "curl" })).toBe("install")
  })

  test("rejects an unsafe current version", () => {
    expect(decide({ current: "1.18.30-f8y-notatime", latest: "1.18.31", method: "curl" })).toBe("noop")
  })

  test("treats implicit suffix comparison in manual upgrades consistently", () => {
    expect(Installation.decideManualUpgrade({ current: oldTimestamp, latest: newTimestamp })).toEqual({
      type: "upgrade",
      target: newTimestamp,
    })
    expect(Installation.decideManualUpgrade({ current: oldTimestamp, latest: "1.18.30" })).toEqual({
      type: "upgrade",
      target: "1.18.30",
    })
    expect(Installation.decideManualUpgrade({ current: oldTimestamp, latest: "1.18.29" })).toEqual({
      type: "skip",
    })
  })
})

async function runUpstreamUpgradePolicyTests() {
  await using tmp = await tmpdir()
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined && !configOverrides.has(entry[0]),
    ),
  )
  Object.assign(environment, {
    HOME: tmp.path,
    XDG_CONFIG_HOME: path.join(tmp.path, "config"),
    XDG_DATA_HOME: path.join(tmp.path, "data"),
    XDG_CACHE_HOME: path.join(tmp.path, "cache"),
    XDG_STATE_HOME: path.join(tmp.path, "state"),
  })
  await fs.mkdir(environment.XDG_CONFIG_HOME, { recursive: true })
  await fs.mkdir(environment.XDG_DATA_HOME, { recursive: true })
  await fs.mkdir(environment.XDG_CACHE_HOME, { recursive: true })
  await fs.mkdir(environment.XDG_STATE_HOME, { recursive: true })

  const child = Bun.spawn(
    [process.execPath, "test", "--timeout=30000", "--define", 'OPENCODE_DISTRIBUTION="upstream"', import.meta.path],
    {
      cwd: packageDirectory,
      env: environment,
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
          () => reject(new Error("upstream upgrade policy test subprocess timed out")),
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

function decide(input: {
  current?: string
  latest: string
  method?: Installation.Method
  autoupdate?: boolean | "notify"
  alwaysNotify?: boolean
}) {
  return Installation.decideUpdate({
    current: input.current ?? "1.2.3",
    latest: input.latest,
    method: input.method ?? "npm",
    autoupdate: input.autoupdate,
    alwaysNotify: input.alwaysNotify,
  })
}

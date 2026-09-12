import { describe, expect, test } from "bun:test"
import { Installation } from "../../src/installation"

describe("upgrade policy", () => {
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

  test("treats local, prerelease, and malformed versions as no update", () => {
    expect(decide({ current: "local", latest: "1.2.4" })).toBe("noop")
    expect(decide({ current: "1.2.3-alpha.1", latest: "1.2.4" })).toBe("noop")
    expect(decide({ current: "1.2.3", latest: "1.2.4-beta.1" })).toBe("noop")
    expect(decide({ current: "1.2", latest: "1.2.4" })).toBe("noop")
    expect(decide({ current: "1.2.3", latest: "1.2.3+build" })).toBe("noop")
  })

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

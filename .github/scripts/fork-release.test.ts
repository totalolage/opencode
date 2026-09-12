import { describe, expect, test } from "bun:test"
import { chmod, mkdir, mkdtemp, rm } from "node:fs/promises"
import path from "node:path"
import {
  CHECKSUMS_FILENAME,
  RELEASE_TARGETS,
  getReleaseTarget,
  packageRelease,
  validateVersion,
  verifyReleaseArtifacts,
  writeSha256Sums,
} from "./fork-release"

describe("fork release workflow", () => {
  test("accepts only stable X.Y.Z versions", () => {
    expect(validateVersion("1.2.3")).toBe("1.2.3")
    expect(() => validateVersion("v1.2.3")).toThrow()
    expect(() => validateVersion("1.2.3-beta.1")).toThrow()
    expect(() => validateVersion("01.2.3")).toThrow()
  })

  test("packages and verifies all four release archives", async () => {
    const root = await mkdtemp(path.join("/tmp", "fork-release-test-"))
    const distDirectory = path.join(root, "dist")
    const outputDirectory = path.join(root, "release")
    const version = "1.2.3"

    await Promise.all(
      RELEASE_TARGETS.map(async (target) => {
        const binaryPath = path.join(distDirectory, target.binaryDirectory, "bin", "opencode")
        await mkdir(path.dirname(binaryPath), { recursive: true })
        await Bun.write(binaryPath, `#!/bin/sh\nprintf '%s\\n' '${version}'\n`)
        await chmod(binaryPath, 0o755)
      }),
    )

    await Promise.all(
      RELEASE_TARGETS.map((target) =>
        packageRelease({
          version,
          platform: target.platform,
          arch: target.arch,
          distDirectory,
          outputDirectory,
        }),
      ),
    )
    await rm(distDirectory, { recursive: true, force: true })
    const checksums = await writeSha256Sums(outputDirectory)
    await verifyReleaseArtifacts(outputDirectory)

    expect(checksums.split("\n").filter(Boolean)).toHaveLength(RELEASE_TARGETS.length)
    expect(await Bun.file(path.join(outputDirectory, CHECKSUMS_FILENAME)).text()).toBe(checksums)

    await rm(root, { recursive: true, force: true })
  })

  test("rejects unexpected and duplicate artifact paths", async () => {
    const root = await mkdtemp(path.join("/tmp", "fork-release-test-"))
    const distDirectory = path.join(root, "dist")
    const outputDirectory = path.join(root, "release")
    const version = "1.2.3"

    await Promise.all(
      RELEASE_TARGETS.map(async (target) => {
        const binaryPath = path.join(distDirectory, target.binaryDirectory, "bin", "opencode")
        await mkdir(path.dirname(binaryPath), { recursive: true })
        await Bun.write(binaryPath, `#!/bin/sh\nprintf '%s\\n' '${version}'\n`)
        await chmod(binaryPath, 0o755)
      }),
    )
    await Promise.all(
      RELEASE_TARGETS.map((target) =>
        packageRelease({
          version,
          platform: target.platform,
          arch: target.arch,
          distDirectory,
          outputDirectory,
        }),
      ),
    )
    await writeSha256Sums(outputDirectory)

    await Bun.write(path.join(outputDirectory, "README"), "unexpected")
    await expect(verifyReleaseArtifacts(outputDirectory)).rejects.toThrow("unexpected release artifacts")

    await rm(path.join(outputDirectory, "README"))
    await mkdir(path.join(outputDirectory, "duplicate"), { recursive: true })
    await Bun.write(
      path.join(outputDirectory, "duplicate", RELEASE_TARGETS[0].archiveName),
      await Bun.file(path.join(outputDirectory, RELEASE_TARGETS[0].archiveName)).arrayBuffer(),
    )
    await expect(verifyReleaseArtifacts(outputDirectory)).rejects.toThrow("duplicate release artifacts")

    await rm(root, { recursive: true, force: true })
  })

  test("maps x64 releases to baseline binaries", () => {
    expect(getReleaseTarget("linux", "x64").binaryDirectory).toBe("opencode-linux-x64-baseline")
    expect(getReleaseTarget("darwin", "arm64").binaryDirectory).toBe("opencode-darwin-arm64")
  })

  test("parses the workflow as dispatch-only YAML", async () => {
    const workflowPath = path.join(import.meta.dir, "../workflows/fork-release.yml")
    const workflow = Bun.YAML.parse(await Bun.file(workflowPath).text()) as Record<string, unknown>
    const trigger = workflow.on as Record<string, unknown>
    const inputs = (trigger.workflow_dispatch as Record<string, unknown>).inputs as Record<
      string,
      Record<string, unknown>
    >

    expect(Object.keys(trigger)).toEqual(["workflow_dispatch"])
    expect(inputs.version.required).toBe(true)
    expect(inputs.ref.required).toBe(true)
    expect(inputs.ref.default).toBe("dev")
    expect(inputs.publish.default).toBe(false)
    expect(inputs.publish.type).toBe("boolean")
  })
})

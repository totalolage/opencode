import { expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { tmpdir } from "../fixture/fixture"

const packageDirectory = path.resolve(import.meta.dir, "../..")
const fixturePath = path.join(import.meta.dir, "upgrade-cli-fixture.ts")
const packageManagers = ["npm", "yarn", "pnpm", "bun", "brew", "scoop", "choco"]
const processTimeout = 10_000
const testTimeout = processTimeout + 5_000

for (const method of [undefined, "curl", "npm", "brew", "yarn", "unknown"]) {
  test(
    `rejects an unsupported actual executable path${method ? ` with ${method}` : " without a method"}`,
    async () => {
      const result = await runUpgrade(method)
      const output = `${result.stdout}\n${result.stderr}`

      expect(result.code).not.toBe(0)
      expect(output).toContain("https://github.com/totalolage/opencode/releases")
      expect(output).not.toContain("Install anyways?")
      expect(result.probes).toBe("")
    },
    { timeout: testTimeout },
  )
}

async function runUpgrade(method?: string) {
  await using tmp = await tmpdir()
  const home = path.join(tmp.path, "home")
  const config = path.join(tmp.path, "config")
  const data = path.join(tmp.path, "data")
  const cache = path.join(tmp.path, "cache")
  const state = path.join(tmp.path, "state")
  const bin = path.join(tmp.path, "bin")
  const probeLog = path.join(tmp.path, "probes")

  await Promise.all(
    [home, config, data, cache, state, bin].map((directory) => fs.mkdir(directory, { recursive: true })),
  )
  await Promise.all(packageManagers.map((name) => writeProbe(bin, name)))

  const environment = {
    ...Object.fromEntries(
      Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    HOME: home,
    XDG_CONFIG_HOME: config,
    XDG_DATA_HOME: data,
    XDG_CACHE_HOME: cache,
    XDG_STATE_HOME: state,
    PATH: `${bin}:${process.env.PATH ?? ""}`,
    UPGRADE_CLI_PROBE_LOG: probeLog,
  }
  const args = [process.execPath, fixturePath, "upgrade", "0.0.1"]
  if (method !== undefined) args.push("--method", method)

  const child = Bun.spawn(args, {
    cwd: packageDirectory,
    env: environment,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
  const output = Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
  let timer: ReturnType<typeof setTimeout> | undefined

  try {
    const [code, stdout, stderr] = await Promise.race([
      output,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("upgrade CLI subprocess timed out")), processTimeout)
      }),
    ])
    return {
      code,
      stdout,
      stderr,
      probes: await fs.readFile(probeLog, "utf8").catch(() => ""),
    }
  } catch (error) {
    child.kill()
    await child.exited
    await output
    throw error
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function writeProbe(bin: string, name: string) {
  const file = path.join(bin, name)
  await Bun.write(
    file,
    `#!/bin/sh
printf '%s\\n' "$0" >> "$UPGRADE_CLI_PROBE_LOG"
exit 97
`,
  )
  await fs.chmod(file, 0o755)
}

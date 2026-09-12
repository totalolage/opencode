import { expect, test } from "bun:test"
import { readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { runCommand, installSignalHandlers, requireCommandCleanup } from "../../script/verify-fork-upgrade"
import { tmpdir } from "../fixture/fixture"

const packageDirectory = path.resolve(import.meta.dir, "../..")
const verifierScriptPath = path.resolve(packageDirectory, "script/verify-fork-upgrade.ts")

test(
  "does not launch a command after an interruption between filesystem steps",
  async () => {
    await using tmp = await tmpdir()
    const fixturePath = path.join(tmp.path, "interrupt-fixture.ts")
    const prepPath = path.join(tmp.path, "filesystem-prep")
    const handledPath = path.join(tmp.path, "interrupt-handled")
    const markerPath = path.join(tmp.path, "command-started")
    const childPidPath = path.join(tmp.path, "command-pid")
    const source = [
      `import { installSignalHandlers, runCommand } from ${JSON.stringify(verifierScriptPath)}`,
      `const root = ${JSON.stringify(tmp.path)}`,
      `const prepPath = ${JSON.stringify(prepPath)}`,
      `const handledPath = ${JSON.stringify(handledPath)}`,
      `const markerPath = ${JSON.stringify(markerPath)}`,
      `const childPidPath = ${JSON.stringify(childPidPath)}`,
      "const removeSignalHandlers = installSignalHandlers()",
      'const signalReceived = new Promise<void>((resolve) => process.once("SIGINT", () => resolve()))',
      'const filesystemPrep = Bun.write(prepPath, "prepared")',
      'process.kill(process.pid, "SIGINT")',
      "await filesystemPrep",
      "await signalReceived",
      "try {",
      "  await runCommand(",
      '    [process.execPath, "-e",',
      '      `await Bun.write(${JSON.stringify(markerPath)}, "started"); await Bun.write(${JSON.stringify(childPidPath)}, String(process.pid))`,',
      "    ],",
      "    { cwd: root, env: { ...Bun.env }, timeoutMs: 1_000 },",
      "  )",
      "  process.exitCode = 2",
      "} catch (error) {",
      '  if (!String(error).includes("process interrupted by SIGINT")) {',
      "    console.error(error)",
      "    process.exitCode = 2",
      "  } else {",
      '    await Bun.write(handledPath, "handled")',
      "    process.exitCode = 1",
      "  }",
      "} finally {",
      "  removeSignalHandlers()",
      "}",
      "",
    ].join("\n")
    await writeFile(fixturePath, source)

    const child = Bun.spawn([process.execPath, fixturePath], {
      cwd: packageDirectory,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    })
    const output = Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
    let completed = false
    let timer: ReturnType<typeof setTimeout> | undefined

    try {
      const [exitCode, stdout, stderr] = await Promise.race([
        output,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("interruption fixture timed out")), 10_000)
        }),
      ])
      completed = true

      expect(exitCode).not.toBe(0)
      expect(await Bun.file(prepPath).exists()).toBe(true)
      expect(await Bun.file(handledPath).exists()).toBe(true)
      expect(await Bun.file(markerPath).exists()).toBe(false)
      expect(await Bun.file(childPidPath).exists()).toBe(false)
      expect(`${stdout}\n${stderr}`).not.toContain("interruption fixture timed out")
    } finally {
      if (timer) clearTimeout(timer)
      if (!completed) child.kill()
      await Promise.allSettled([child.exited, output])
    }
  },
  { timeout: 30_000 },
)

test(
  "kills a timed out process group and its inherited descendant",
  async () => {
    await using tmp = await tmpdir()
    const childPidPath = path.join(tmp.path, "child-pid")
    const descendantPidPath = path.join(tmp.path, "descendant-pid")
    const readyPath = path.join(tmp.path, "ready")
    const descendantSource = ['process.on("SIGTERM", () => {})', "setInterval(() => {}, 1_000)", ""].join("\n")
    const childSource = [
      'process.on("SIGTERM", () => {})',
      "// Deliberately inherit the verifier child process group; do not detach this descendant.",
      `const descendant = Bun.spawn([process.execPath, "-e", ${JSON.stringify(descendantSource)}], { stdin: "ignore", stdout: "ignore", stderr: "ignore" })`,
      `await Bun.write(${JSON.stringify(descendantPidPath)}, String(descendant.pid))`,
      `await Bun.write(${JSON.stringify(childPidPath)}, String(process.pid))`,
      `await Bun.write(${JSON.stringify(readyPath)}, "ready")`,
      "setInterval(() => {}, 1_000)",
      "",
    ].join("\n")
    const environment = Object.fromEntries(
      Object.entries(Bun.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
    )
    const command = runCommand([process.execPath, "-e", childSource], {
      cwd: tmp.path,
      env: environment,
      timeoutMs: 1_000,
    })
    let childPid: number | undefined
    let descendantPid: number | undefined

    try {
      await waitForFile(readyPath, 5_000)
      const startedChildPid = Number.parseInt(await readFile(childPidPath, "utf8"), 10)
      const startedDescendantPid = Number.parseInt(await readFile(descendantPidPath, "utf8"), 10)
      if (!isSafePid(startedChildPid) || !isSafePid(startedDescendantPid)) {
        throw new Error("timeout fixture did not publish safe child PIDs")
      }
      childPid = startedChildPid
      descendantPid = startedDescendantPid

      const result = await command
      expect(result.timedOut).toBe(true)
      expect(result.exitCode === null || result.exitCode !== 0).toBe(true)
      expect(result.cleanupError).toBeUndefined()
      expect(await waitForProcessExit(childPid, 3_000)).toBe(true)
      expect(await waitForProcessExit(descendantPid, 3_000)).toBe(true)
    } finally {
      childPid ??= await readPid(childPidPath)
      descendantPid ??= await readPid(descendantPidPath)
      if (isSafePid(childPid)) killProcessGroup(childPid)
      for (const pid of [childPid, descendantPid]) {
        if (isSafePid(pid)) killProcess(pid)
      }
      const completed = await Promise.race([
        command.then(
          () => true,
          () => true,
        ),
        delay(5_000).then(() => false),
      ])
      if (!completed) throw new Error("timeout fixture command did not finish after cleanup")
      if (isSafePid(childPid)) expect(await waitForProcessExit(childPid, 3_000)).toBe(true)
      if (isSafePid(descendantPid)) expect(await waitForProcessExit(descendantPid, 3_000)).toBe(true)
    }
  },
  { timeout: 30_000 },
)

test("requires command cleanup errors regardless of scenario context", () => {
  expect(() => requireCommandCleanup({ cleanupError: "failed" })).toThrow("command cleanup failed: failed")
  expect(() => requireCommandCleanup({})).not.toThrow()
})

async function waitForFile(filePath: string, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs
  while (!(await Bun.file(filePath).exists())) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error(`file was not published before timeout: ${filePath}`)
    await delay(Math.min(25, remaining))
  }
}

async function waitForProcessExit(pid: number, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs
  while (processIsAlive(pid)) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) return false
    await delay(Math.min(25, remaining))
  }
  return true
}

function processIsAlive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    if (hasErrorCode(error, "ESRCH")) return false
    if (hasErrorCode(error, "EPERM")) return true
    throw error
  }
}

function isSafePid(pid: number | undefined): pid is number {
  return pid !== undefined && Number.isSafeInteger(pid) && pid > 1 && pid !== process.pid
}

function killProcessGroup(pid: number) {
  try {
    process.kill(-pid, "SIGKILL")
  } catch (error) {
    if (!hasErrorCode(error, "ESRCH")) throw error
  }
}

function killProcess(pid: number) {
  try {
    process.kill(pid, "SIGKILL")
  } catch (error) {
    if (!hasErrorCode(error, "ESRCH")) throw error
  }
}

async function readPid(filePath: string) {
  const value = await Bun.file(filePath)
    .text()
    .catch(() => "")
  const pid = Number.parseInt(value, 10)
  return isSafePid(pid) ? pid : undefined
}

function hasErrorCode(error: unknown, code: string) {
  return typeof error === "object" && error !== null && "code" in error && error.code === code
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
}

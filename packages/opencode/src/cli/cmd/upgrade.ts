import type { Argv } from "yargs"
import { UI } from "../ui"
import * as prompts from "@clack/prompts"
import { Installation } from "../../installation"
import { Fork } from "../../installation/fork"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { errorMessage } from "@/util/error"
import semver from "semver"

export const UpgradeCommand = {
  command: "upgrade [target]",
  describe: "upgrade opencode to the latest or a specific version",
  builder: (yargs: Argv) => {
    return yargs
      .positional("target", {
        describe: "version to upgrade to, for ex '0.1.48' or 'v0.1.48'",
        type: "string",
      })
      .option("method", {
        alias: "m",
        describe: "installation method to use",
        type: "string",
        choices: ["curl", "npm", "pnpm", "bun", "brew", "choco", "scoop"],
      })
  },
  handler: async (args: { target?: string; method?: string }) => {
    UI.empty()
    UI.println(UI.logo("  "))
    UI.empty()
    prompts.intro("Upgrade")
    const detectedMethod = await Installation.method().catch((error) => {
      process.exitCode = 1
      prompts.log.error(errorMessage(error))
      prompts.outro("Done")
      return undefined
    })
    if (!detectedMethod) return
    if (Fork.IS_FORK) {
      await upgradeFork({ target: args.target, method: args.method, detectedMethod })
      return
    }

    const method = (args.method as Installation.Method) ?? detectedMethod
    if (method === "unknown") {
      prompts.log.error(`opencode is installed to ${process.execPath} and may be managed by a package manager`)
      const install = await prompts.select({
        message: "Install anyways?",
        options: [
          { label: "Yes", value: true },
          { label: "No", value: false },
        ],
        initialValue: false,
      })
      if (!install) {
        prompts.outro("Done")
        return
      }
    }
    prompts.log.info("Using method: " + method)
    const target = args.target
      ? args.target.replace(/^v/, "")
      : await Installation.latest().catch((error) => {
          process.exitCode = 1
          prompts.log.error(errorMessage(error))
          prompts.outro("Done")
          return undefined
        })
    if (!target) return

    if (InstallationVersion === target) {
      prompts.log.warn(`opencode upgrade skipped: ${target} is already installed`)
      prompts.outro("Done")
      return
    }

    prompts.log.info(`From ${InstallationVersion} → ${target}`)
    const spinner = prompts.spinner()
    spinner.start("Upgrading...")
    const err = await Installation.upgrade(method, target).catch((err) => err)
    if (err) {
      spinner.stop("Upgrade failed", 1)
      if (err instanceof Installation.UpgradeFailedError) {
        // necessary because choco only allows install/upgrade in elevated terminals
        if (method === "choco" && err.stderr.includes("not running from an elevated command shell")) {
          prompts.log.error("Please run the terminal as Administrator and try again")
        } else {
          prompts.log.error(err.stderr)
        }
      } else prompts.log.error(errorMessage(err))
      process.exitCode = 1
      prompts.outro("Done")
      return
    }
    spinner.stop("Upgrade complete")
    prompts.outro("Done")
  },
}

async function upgradeFork(input: { target?: string; method?: string; detectedMethod: Installation.Method }) {
  if (!Installation.canUseForkUpgrade({ actual: input.detectedMethod, requested: input.method })) {
    printForkInstructions()
    return
  }

  const decision = Installation.decideManualUpgrade({
    current: InstallationVersion,
    requested: input.target,
  })
  if (decision.type === "instructions") {
    printForkInstructions()
    return
  }

  if (decision.type === "skip") {
    prompts.log.warn("opencode upgrade skipped: the latest release is not newer")
    prompts.outro("Done")
    return
  }

  const target = decision.type === "lookup" ? await latestForkTarget() : decision.target
  if (!target) return

  if (decision.type === "upgrade") {
    const current = Fork.stableVersion(InstallationVersion)
    if (current && semver.eq(current, target)) {
      prompts.log.warn(`opencode upgrade skipped: ${target} is already installed`)
      prompts.outro("Done")
      return
    }
  }

  await runForkUpgrade(target)
}

async function latestForkTarget() {
  const latest = await Installation.latest().catch((error) => {
    process.exitCode = 1
    prompts.log.error(errorMessage(error))
    return undefined
  })
  if (!latest) {
    process.exitCode = 1
    prompts.log.error("Unable to determine the latest fork release")
    prompts.outro("Done")
    return undefined
  }

  const decision = Installation.decideManualUpgrade({ current: InstallationVersion, latest })
  if (decision.type === "instructions") {
    printForkInstructions()
    return undefined
  }
  if (decision.type === "skip") {
    prompts.log.warn("opencode upgrade skipped: the latest release is not newer")
    prompts.outro("Done")
    return undefined
  }
  if (decision.type !== "upgrade") return undefined
  return decision.target
}

async function runForkUpgrade(target: string) {
  prompts.log.info("Using method: curl")
  prompts.log.info(`From ${InstallationVersion} → ${target}`)
  const spinner = prompts.spinner()
  spinner.start("Upgrading...")
  const err = await Installation.upgrade("curl", target).catch((err) => err)
  if (err) {
    spinner.stop("Upgrade failed", 1)
    prompts.log.error(errorMessage(err))
    process.exitCode = 1
    prompts.outro("Done")
    return
  }
  spinner.stop("Upgrade complete")
  prompts.outro("Done")
}

function printForkInstructions() {
  process.exitCode = 1
  prompts.log.error(Fork.INSTRUCTIONS)
  prompts.outro("Done")
}

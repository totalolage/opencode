import yargs from "yargs"
import { UpgradeCommand } from "../../src/cli/cmd/upgrade"

await yargs(process.argv.slice(2)).exitProcess(false).help().command(UpgradeCommand).strict().parseAsync()
process.exit(process.exitCode ?? 0)

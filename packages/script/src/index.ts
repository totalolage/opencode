import { $ } from "bun"
import semver from "semver"
import path from "path"

const rootPkgPath = path.resolve(import.meta.dir, "../../../package.json")
const rootPkg = await Bun.file(rootPkgPath).json()
const expectedBunVersion = rootPkg.packageManager?.split("@")[1]

if (!expectedBunVersion) {
  throw new Error("packageManager field not found in root package.json")
}

// relax version requirement
const expectedBunVersionRange = `^${expectedBunVersion}`

if (!semver.satisfies(process.versions.bun, expectedBunVersionRange)) {
  throw new Error(`This script requires bun@${expectedBunVersionRange}, but you are using bun@${process.versions.bun}`)
}

const env = {
  OPENCODE_CHANNEL: process.env["OPENCODE_CHANNEL"],
  OPENCODE_BUMP: process.env["OPENCODE_BUMP"],
  OPENCODE_VERSION: process.env["OPENCODE_VERSION"],
  OPENCODE_RELEASE: process.env["OPENCODE_RELEASE"],
  OPENCODE_UPSTREAM_BUILD: process.env["OPENCODE_UPSTREAM_BUILD"],
  OPENCODE_FORK_RELEASE: process.env["OPENCODE_FORK_RELEASE"],
  OPENCODE_FORK_TEST_ORIGIN: process.env["OPENCODE_FORK_TEST_ORIGIN"],
}
const IS_UPSTREAM_BUILD = env.OPENCODE_UPSTREAM_BUILD === "1"
const IS_FORK_RELEASE = env.OPENCODE_FORK_RELEASE === "1"

if (IS_UPSTREAM_BUILD && IS_FORK_RELEASE) {
  throw new Error("OPENCODE_FORK_RELEASE=1 cannot be used with OPENCODE_UPSTREAM_BUILD=1")
}

if (IS_FORK_RELEASE) {
  if (!env.OPENCODE_VERSION || !isStrictStableVersion(env.OPENCODE_VERSION)) {
    throw new Error("OPENCODE_VERSION must be an explicit stable X.Y.Z version for OPENCODE_FORK_RELEASE=1")
  }
  if (env.OPENCODE_CHANNEL !== "latest") {
    throw new Error("OPENCODE_CHANNEL=latest is required for OPENCODE_FORK_RELEASE=1")
  }
}

const CHANNEL = await (async () => {
  if (env.OPENCODE_CHANNEL) return env.OPENCODE_CHANNEL
  if (env.OPENCODE_BUMP) return "latest"
  if (env.OPENCODE_VERSION && !env.OPENCODE_VERSION.startsWith("0.0.0-")) return "latest"
  return await $`git branch --show-current`.text().then((x) => x.trim())
})()
const IS_PREVIEW = CHANNEL !== "latest"

const VERSION = await (async () => {
  if (env.OPENCODE_VERSION) return env.OPENCODE_VERSION
  if (!IS_UPSTREAM_BUILD || IS_PREVIEW)
    return `0.0.0-${CHANNEL}-${new Date().toISOString().slice(0, 16).replace(/[-:T]/g, "")}`
  const version = await fetch("https://registry.npmjs.org/opencode-ai/latest")
    .then((res) => {
      if (!res.ok) throw new Error(res.statusText)
      return res.json()
    })
    .then((data: any) => data.version)
  const [major, minor, patch] = version.split(".").map((x: string) => Number(x) || 0)
  const t = env.OPENCODE_BUMP?.toLowerCase()
  if (t === "major") return `${major + 1}.0.0`
  if (t === "minor") return `${major}.${minor + 1}.0`
  return `${major}.${minor}.${patch + 1}`
})()
const DISTRIBUTION = IS_UPSTREAM_BUILD ? "upstream" : "totalolage/opencode"
const FORK_TEST_ORIGIN = env.OPENCODE_FORK_TEST_ORIGIN ?? ""

const bot = ["actions-user", "opencode", "opencode-agent[bot]"]
const teamPath = path.resolve(import.meta.dir, "../../../.github/TEAM_MEMBERS")
const team = [
  ...(await Bun.file(teamPath)
    .text()
    .then((x) => x.split(/\r?\n/).map((x) => x.trim()))
    .then((x) => x.filter((x) => x && !x.startsWith("#")))),
  ...bot,
]

export const Script = {
  get upstream() {
    return IS_UPSTREAM_BUILD
  },
  get forkRelease() {
    return IS_FORK_RELEASE
  },
  get distribution() {
    return DISTRIBUTION
  },
  get forkTestOrigin() {
    return FORK_TEST_ORIGIN
  },
  get channel() {
    return CHANNEL
  },
  get version() {
    return VERSION
  },
  get preview() {
    return IS_PREVIEW
  },
  get release(): boolean {
    return !!env.OPENCODE_RELEASE
  },
  get team() {
    return team
  },
}
console.log(`opencode script`, JSON.stringify(Script, null, 2))

function isStrictStableVersion(value: string) {
  return /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(value) && semver.valid(value) === value
}

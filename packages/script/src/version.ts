import semver from "semver"

// Accepts a stable X.Y.Z or a fork release X.Y.Z-f8y-<14 digit UTC timestamp>.
// An optional single leading "v" is accepted and stripped in the normalized result.
const pattern = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-f8y-(\d{14}))?$/

export function parse(input: string): string | undefined {
  const match = pattern.exec(input)
  if (!match) return undefined
  const normalized = input.startsWith("v") ? input.slice(1) : input
  if (semver.valid(normalized) !== normalized) return undefined
  const timestamp = match[4]
  if (timestamp && !validTimestamp(timestamp)) return undefined
  return normalized
}

// YYYYMMDDHHmmss must be a real UTC Gregorian date.
function validTimestamp(timestamp: string) {
  const year = Number(timestamp.slice(0, 4))
  const month = Number(timestamp.slice(4, 6))
  const day = Number(timestamp.slice(6, 8))
  const hour = Number(timestamp.slice(8, 10))
  const minute = Number(timestamp.slice(10, 12))
  const second = Number(timestamp.slice(12, 14))
  if (year < 1 || year > 9999) return false
  if (month < 1 || month > 12) return false
  if (hour > 23 || minute > 59 || second > 59) return false

  const date = new Date(0)
  // setUTCFullYear avoids the Date.UTC quirk that maps years 0..99 to 1900..1999.
  date.setUTCFullYear(year, month - 1, day)
  date.setUTCHours(hour, minute, second, 0)
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day &&
    date.getUTCHours() === hour &&
    date.getUTCMinutes() === minute &&
    date.getUTCSeconds() === second
  )
}

export * as ForkVersion from "./version"

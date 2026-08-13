import { describe, expect, it } from "bun:test"

import { packageName } from "../src/index.ts"

describe("@arena/db scaffold", () => {
  it("exposes its package name from the barrel", () => {
    expect(packageName).toBe("@arena/db")
  })
})

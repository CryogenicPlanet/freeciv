export function requiredValue<T>(value: T | null | undefined, label: string): T {
  if (value == null) throw new Error(`Missing test fixture: ${label}`)
  return value
}

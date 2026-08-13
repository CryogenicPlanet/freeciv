export function sortedCopy<T>(
  values: readonly T[],
  compare: (left: T, right: T) => number,
): T[] {
  if (values.length < 2) return [...values]
  const middle = Math.floor(values.length / 2)
  const left = sortedCopy(values.slice(0, middle), compare)
  const right = sortedCopy(values.slice(middle), compare)
  const result: T[] = []
  const leftIterator = left.values()
  const rightIterator = right.values()
  let leftEntry = leftIterator.next()
  let rightEntry = rightIterator.next()

  while (!leftEntry.done && !rightEntry.done) {
    if (compare(leftEntry.value, rightEntry.value) <= 0) {
      result.push(leftEntry.value)
      leftEntry = leftIterator.next()
    } else {
      result.push(rightEntry.value)
      rightEntry = rightIterator.next()
    }
  }
  while (!leftEntry.done) {
    result.push(leftEntry.value)
    leftEntry = leftIterator.next()
  }
  while (!rightEntry.done) {
    result.push(rightEntry.value)
    rightEntry = rightIterator.next()
  }
  return result
}

export function reversedCopy<T>(values: readonly T[]): T[] {
  return values.reduceRight<T[]>((result, value) => {
    result.push(value)
    return result
  }, [])
}

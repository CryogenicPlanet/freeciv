import { describe, expect, it } from 'vitest'
import { reversedCopy, sortedCopy } from './ordered'

describe('immutable ordering helpers', () => {
  it('sorts without mutating and preserves equal-value order', () => {
    const source = [
      { rank: 2, label: 'second-a' },
      { rank: 1, label: 'first' },
      { rank: 2, label: 'second-b' },
    ]

    expect(sortedCopy(source, (left, right) => left.rank - right.rank)).toEqual([
      { rank: 1, label: 'first' },
      { rank: 2, label: 'second-a' },
      { rank: 2, label: 'second-b' },
    ])
    expect(source.map((entry) => entry.label)).toEqual([
      'second-a', 'first', 'second-b',
    ])
  })

  it('reverses without mutating', () => {
    const source = [1, 2, 3]
    expect(reversedCopy(source)).toEqual([3, 2, 1])
    expect(source).toEqual([1, 2, 3])
  })
})

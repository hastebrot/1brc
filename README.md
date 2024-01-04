# 1brc

solutions for 1brc [1] in typescript with bun.

> The One Billion Row Challenge
> A fun exploration of how quickly 1B rows from a text file can be aggregated with Java.

[1] https://github.com/gunnarmorling/1brc

for a target time of 1 minute:

- `1_000_000` records in 60 ms (0.06 seconds)
- `10_000_000` records in 600 ms (0.6 seconds)
- `100_000_000` records in 6000 ms (6 seconds)

**overview:**

- reads data textfile as byte stream
- uses Bun.ArrayBufferSink() but reusing a new Uint8Array(10) might be faster
- uses non-cryptographic hash with Bun.hash.cityHash32(key) but reusing a single number with FNV-1a might be faster
- uses hashmap to store and retrieve count, sum, min, and max values
- processes without concurrent separate chunks
- multiplies the floating points rounded to 1 decimal by 10

### bun

- `❯ cd bun/`
- `❯ bun install`

```
❯ bun run 1brc.bun.buffers.ts 10_000_000
countMap size: 413
sumMap size: 413
numOfRecords 10,000,000
duration: 6.856 s
est. duration: 685.574 s
```

```
❯ bun run 1brc.bun.lesscopy.ts 10_000_000
countMap size: 413
sumMap size: 413
numOfRecords 10,000,000
duration: 2.263 s
est. duration: 226.270 s
```

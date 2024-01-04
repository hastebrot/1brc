# 1brc

solutions for 1brc [1] in typescript with bun.

> The One Billion Row Challenge
> A fun exploration of how quickly 1B rows from a text file can be aggregated with Java.

[1] https://github.com/gunnarmorling/1brc

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
❯ /usr/bin/time -p bun run 1brc.bun.buffers.ts
numOfRecords 10000000
countMap size: 413
sumMap size: 413
real 7.08
user 7.04
sys 0.46
```

```
❯ /usr/bin/time -p bun run 1brc.bun.lesscopy.ts
numOfRecords 10000000
countMap size: 413
sumMap size: 413
real 3.18
user 3.16
sys 0.06
```

const file = Bun.file("../measurements.txt");
const stream = file.stream();
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const CHAR_NEWLINE = encoder.encode("\n")[0];
const CHAR_SEMICOLON = encoder.encode(";")[0];
const CHAR_ZERO = encoder.encode("0")[0];
const CHAR_DOT = encoder.encode(".")[0];
const CHAR_MINUS = encoder.encode("-")[0];

const MIN_VALUE = Number.MIN_VALUE;
const MAX_VALUE = Number.MAX_VALUE;

type key = string | bigint | number;
const countMap = new Map<key, number>();
const sumMap = new Map<key, number>();
const minMap = new Map<key, number>();
const maxMap = new Map<key, number>();

// const maxNumOfRecords = 10_000_000;
const maxNumOfRecords = 10;
let numOfRecords = 0;
let isKey = true;
let isStop = false;

const fnvPrime = 16777619;
const fnvOffset = 2166136261;
let hash = fnvOffset;

const valBuffer = new Uint8Array(10);
let valIndex = 0;
let valSign = 1;

const keySink = new Bun.ArrayBufferSink();
keySink.start({ stream: true, asUint8Array: true });

const valueSink = new Bun.ArrayBufferSink();
valueSink.start({ stream: true, asUint8Array: true });

for await (const chunk of stream) {
  if (isStop) {
    break;
  }

  let index = 0;
  while (index < chunk.length) {
    let char = chunk[index];
    index++;

    if (char === CHAR_NEWLINE) {
      isKey = true;
      // const value = valueSink.flush() as Uint8Array;

      const key = hash;
      hash = fnvOffset;

      const value = valBuffer.slice(0, valIndex);
      const valueString = decoder.decode(value);
      // const valueNumber = parseFloat(valueString) * 10;
      const valueNumber = parseInt(valueString) * valSign;
      valIndex = 0;
      valSign = 1;

      sumMap.set(key, (sumMap.get(key) ?? 0) + valueNumber);
      minMap.set(key, Math.min(minMap.get(key) ?? MAX_VALUE, valueNumber));
      maxMap.set(key, Math.max(maxMap.get(key) ?? MIN_VALUE, valueNumber));

      numOfRecords++;
      if (numOfRecords >= maxNumOfRecords) {
        isStop = true;
        break;
      }
      continue;
    }

    if (char === CHAR_SEMICOLON) {
      isKey = false;
      // const key = keySink.flush() as Uint8Array;

      const keyString = hash;
      //   const keyString = Bun.hash.cityHash32(key);
      countMap.set(keyString, (countMap.get(keyString) ?? 0) + 1);

      continue;
    }

    if (isKey) {
      // keySink.write(new Uint8Array([char]));

      const firstOctet = char & 0xff;
      hash = hash ^ firstOctet;
      hash = (hash * fnvPrime) | 0;
      const secondOctet = char >> 8;
      hash = hash ^ secondOctet;
      hash = (hash * fnvPrime) | 0;

      continue;
    }

    if (!isKey) {
      // valueSink.write(new Uint8Array([char]));

      // valBuffer[valIndex] = char;
      // valIndex += 1;

      if (char === CHAR_MINUS) {
        valSign = -1;
      } else if (char !== CHAR_DOT) {
        valBuffer[valIndex] = char;
        valIndex += 1;
      }
    }
  }
}

for (const [key, value] of countMap) {
  const count = value;
  const sum = sumMap.get(key)! / 10;
  const min = minMap.get(key)! / 10;
  const max = maxMap.get(key)! / 10;
  const avg = sum / count;
  console.log(
    key,
    count,
    sum,
    JSON.stringify({
      avg: avg.toFixed(1),
      min: min.toFixed(1),
      max: max.toFixed(1),
    })
  );
}

console.log("numOfRecords", numOfRecords);
console.log("countMap size:", countMap.size);
console.log("sumMap size:", sumMap.size);

import { readCSV } from "https://deno.land/x/csv@v0.9.2/mod.ts";

const file = await Deno.open("./measurements.txt");
const reader = file.readable.getReader();
const decoder = new TextDecoder();

let done = false;
let index = 0;
while (!done) {
  const result = await reader.read();
  done = result.done;
  if (result.value) {
    decoder.decode(result.value);
  }
  index += 1;
  if (index > 1_000_000) {
    break;
  }
}

// let index = 0;
// for await (const row of readCSV(file, { columnSeparator: ";" })) {
//   await Array.fromAsync(row);
//   index += 1;
//   if (index > 1_000_000) {
//     break;
//   }
// }

file.close();

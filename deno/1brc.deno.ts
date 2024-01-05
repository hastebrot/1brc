const file = await Deno.open("./measurements.txt");
const stream = file.readable;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

const CHAR_NEWLINE = encoder.encode("\n")[0];

async function main() {
  let count = 0;

  for await (const chunk of stream) {
    let index = 0;
    count += chunk.slice(0, chunk.indexOf(CHAR_NEWLINE)).length;

    // while (index < chunk.length) {
    //   const char = chunk[index];
    //   index += 1;

    //   if (char === CHAR_NEWLINE) {
    //     count += 1;
    //   }
    // }
  }

  console.log({ count });
}

await main();
try {
  file.close();
} catch (_) {
  null;
}

import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function createLargeTestData(
  SIZE = 1100,
  path = fileURLToPath(new URL('./input.txt', import.meta.url))
) {
  const writeStream = fs.createWriteStream(path);

  for (let i = 0; i < SIZE; i++) {
    const data = getRandomInt(0, SIZE);
    writeStream.write(data.toString());
    if (i !== SIZE - 1) {
      writeStream.write('\n');
    }
  }
}

// createLargeTestData();

import { sortFile } from '../lib/index.mjs';
import test from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
test('small data', async (t) => {
  const testInputFilePath = fileURLToPath(
    new URL('./fixtures/1/input.txt', import.meta.url)
  );
  const testOutputFilePath = fileURLToPath(
    new URL('./fixtures/1/output.txt', import.meta.url)
  );
  await sortFile(testInputFilePath, testOutputFilePath);
  const readStream = createInterface({
    input: fs.createReadStream(testOutputFilePath),
  });
  let preLine = Number.MIN_VALUE;
  for await (const line of readStream) {
    assert(Number(line) >= preLine);
    preLine = Number(line);
  }
});

test('large data', async (t) => {
  const testInputFilePath = fileURLToPath(
    new URL('./fixtures/2/input.txt', import.meta.url)
  );
  const testOutputFilePath = fileURLToPath(
    new URL('./fixtures/2/output.txt', import.meta.url)
  );
  await sortFile(testInputFilePath, testOutputFilePath);
  const readStream = createInterface({
    input: fs.createReadStream(testOutputFilePath),
  });
  let preLine = Number.MIN_VALUE;
  for await (const line of readStream) {
    assert(Number(line) >= preLine);
    preLine = Number(line);
  }
});

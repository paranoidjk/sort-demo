// 有一个 10G 文件，每一行是一个时间戳，
// 现在要在一台 2C4G 的机器上对它进行排序，输出排序以后的文件

// 案例输入
// 1570593273487
// 1570593273486
// 1570593273488
// …

// 输出
// 1570593273486
// 1570593273487
// 1570593273488
// …

import { createInterface } from 'node:readline';
import { createReadStream, createWriteStream } from 'node:fs';
import { rename, rm } from 'node:fs/promises';
import { join, dirname } from 'node:path';

const MAX_LINE_IN_MEMORY = 200;

/**
 * 拆分成内部排序好的小文件
 * @param {*} inputFile
 * @param {*} outputFile
 * @returns
 */
async function splitFile(inputFile, outputFile) {
  const inputStream = createReadStream(inputFile);
  const inputLineStream = createInterface({
    input: inputStream,
  });

  const tmpFilePaths = [];
  const tmpFileStreams = [];
  let chunk = [];
  const writeTmpFile = () => {
    // 把截止当前片段排序
    chunk.sort((a, b) => a - b);
    // 把截止当前片段排序结果写入
    const fileIndex = tmpFilePaths.length + 1;
    const tmpFile = join(dirname(outputFile), `tmp${fileIndex}.txt`);
    const writeStream = createWriteStream(tmpFile);
    chunk.forEach((l) => writeStream.write(l.toString() + '\n'));
    writeStream.end();
    tmpFilePaths.push(tmpFile);
    tmpFileStreams.push(writeStream);
    // 清空 chunk 缓存区
    chunk = [];
  };
  for await (const line of inputLineStream) {
    chunk.push(Number(line));
    if (chunk.length === MAX_LINE_IN_MEMORY) {
      // 从下一行开始就要超过最大内存容量，要开启新文件了
      writeTmpFile();
    }
  }

  if (chunk.length > 0) {
    writeTmpFile();
  }
  await Promise.all(
    tmpFileStreams.map(
      (s) =>
        new Promise((resolve, reject) => {
          s.on('finish', resolve);
          s.on('error', reject);
        })
    )
  );
  return tmpFilePaths;
}

async function mergeFiles(tmpFiles, outputFile) {
  if (tmpFiles.length === 1) {
    await rename(tmpFiles[0], outputFile);
    return;
  }

  // 下面执行多路归并排序
  const readers = tmpFiles.map((tmpFile) => {
    const readStream = createReadStream(tmpFile);
    const readLineStream = createInterface({
      input: readStream,
    });
    return readLineStream[Symbol.asyncIterator]();
  });

  const currentData = await Promise.all(
    readers.map(async (r) => {
      const { done, value } = await r.next();
      return done ? Number.MAX_VALUE : Number(value);
    })
  );
  const writeStream = createWriteStream(outputFile);

  while (true) {
    const minValue = Math.min(...currentData);
    if (minValue === Number.MAX_VALUE) {
      // 已经没有下一个值了
      break;
    }
    // 最小的优先写入
    const minIndex = currentData.findIndex((d) => d === minValue);
    writeStream.write(minValue.toString() + '\n');
    // 重新读取当前 chunk 下一个最小的
    const { done, value } = await readers[minIndex].next();
    currentData[minIndex] = done ? Number.MAX_VALUE : Number(value);
  }
  writeStream.end();
  await new Promise((resolve, reject) => {
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });

  await cleanTmpFiles(tmpFiles);
}

async function cleanTmpFiles(files) {
  await Promise.all(
    files.map(async (f) => {
      await rm(f);
    })
  );
}

export async function sortFile(inputFile, outputFile) {
  const tmpFiles = await splitFile(inputFile, outputFile);
  await mergeFiles(tmpFiles, outputFile);
}

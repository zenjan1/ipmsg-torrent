const fs = require('fs');
const path = require('path');

const size = 256;
const canvas = Buffer.alloc(size * size * 4);

for (let y = 0; y < size; y++) {
  for (let x = 0; x < size; x++) {
    const idx = (y * size + x) * 4;
    const cx = x - size/2;
    const cy = y - size/2;
    const dist = Math.sqrt(cx*cx + cy*cy);
    
    if (dist < size/2 - 10) {
      canvas[idx] = 0x07;
      canvas[idx + 1] = 0xC1;
      canvas[idx + 2] = 0x60;
      canvas[idx + 3] = 0xFF;
    } else if (dist < size/2) {
      canvas[idx] = 0x06;
      canvas[idx + 1] = 0xAD;
      canvas[idx + 2] = 0x56;
      canvas[idx + 3] = 0xFF;
    } else {
      canvas[idx] = 0;
      canvas[idx + 1] = 0;
      canvas[idx + 2] = 0;
      canvas[idx + 3] = 0;
    }
  }
}

const width = size;
const height = size;
const channels = 4;

function createPNG(width, height, rgba) {
  const zlib = require('zlib');
  
  function crc32(buf) {
    let crc = -1;
    for (let i = 0; i < buf.length; i++) {
      crc = (crc >>> 8) ^ crc32Table[(crc ^ buf[i]) & 0xFF];
    }
    return (crc ^ -1) >>> 0;
  }
  
  const crc32Table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    crc32Table[i] = c;
  }
  
  function chunk(type, data) {
    const typeData = Buffer.from(type);
    const len = Buffer.alloc(4);
    len.writeUInt32BE(data.length);
    const crcInput = Buffer.concat([typeData, data]);
    const crcVal = Buffer.alloc(4);
    crcVal.writeUInt32BE(crc32(crcInput));
    return Buffer.concat([len, typeData, data, crcVal]);
  }
  
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  
  const raw = Buffer.alloc(height * (1 + width * 4));
  for (let y = 0; y < height; y++) {
    raw[y * (1 + width * 4)] = 0;
    for (let x = 0; x < width; x++) {
      const srcIdx = (y * width + x) * 4;
      const dstIdx = y * (1 + width * 4) + 1 + x * 4;
      raw[dstIdx] = rgba[srcIdx];
      raw[dstIdx + 1] = rgba[srcIdx + 1];
      raw[dstIdx + 2] = rgba[srcIdx + 2];
      raw[dstIdx + 3] = rgba[srcIdx + 3];
    }
  }
  
  const compressed = zlib.deflateSync(raw);
  
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
    chunk('IHDR', ihdr),
    chunk('IDAT', compressed),
    chunk('IEND', Buffer.alloc(0))
  ]);
}

const png = createPNG(size, size, canvas);
fs.writeFileSync('/workspace/build/icon.png', png);
console.log('Icon created: /workspace/build/icon.png');

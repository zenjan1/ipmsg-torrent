const fs = require('fs');
const zlib = require('zlib');

function createPNG(width, height, rgba) {
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

function generateIcon(size) {
  const canvas = Buffer.alloc(size * size * 4);
  const center = size / 2;
  const radius = size / 2 - 2;
  
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const idx = (y * size + x) * 4;
      const dx = x - center;
      const dy = y - center;
      const dist = Math.sqrt(dx * dx + dy * dy);
      
      if (dist < radius - 2) {
        canvas[idx] = 0x07;
        canvas[idx + 1] = 0xC1;
        canvas[idx + 2] = 0x60;
        canvas[idx + 3] = 0xFF;
      } else if (dist < radius) {
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
  
  return createPNG(size, size, canvas);
}

const buildDir = '/workspace/build';
const iconsDir = buildDir + '/icons';

fs.mkdirSync(iconsDir, { recursive: true });

const sizes = [16, 24, 32, 48, 64, 128, 256, 512, 1024];
sizes.forEach(size => {
  const png = generateIcon(size);
  const filename = size + 'x' + size + '.png';
  fs.writeFileSync(iconsDir + '/' + filename, png);
  console.log('Created: ' + filename);
});

fs.writeFileSync(buildDir + '/icon.png', generateIcon(256));
console.log('Created: icon.png');

console.log('All icons generated!');

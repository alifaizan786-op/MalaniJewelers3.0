const fs = require('fs');
const path = require('path');

const logDir = path.join(__dirname, '..', '..', 'logs');

if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
  console.log('Created log directory:', logDir);
}
function writeLog(storeCode, message) {
  try {
    const filename = `${sanitizeFileName(storeCode)}-log.txt`;
    const fullPath = path.join(logDir, filename);
    const timestamp = new Date().toLocaleString();
    const line = `${timestamp} | ${message}\n`;

    fs.appendFileSync(fullPath, line);
  } catch (err) {
    console.error('❌ Failed to write log:', err);
  }
}

// Very basic file name sanitizer
function sanitizeFileName(name) {
  return name.replace(/[^a-zA-Z0-9-_]/g, '');
}

module.exports = {
  writeLog,
};

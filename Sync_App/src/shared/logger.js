// src/shared/logger.js
const fs = require('fs');
const path = require('path');
const { app } = require('electron');

// Determine the correct log directory based on whether app is packaged
function getLogDirectory() {
  if (app.isPackaged) {
    // For packaged apps, use the directory where the executable is located
    const exeDir = path.dirname(app.getPath('exe'));
    return path.join(exeDir, 'logs');
  } else {
    // For development, use the traditional relative path
    return path.join(__dirname, '..', '..', 'logs');
  }
}

const logDir = getLogDirectory();

// Create log directory with better error handling
function ensureLogDirectory() {
  try {
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
      console.log('✅ Created log directory:', logDir);
    }
  } catch (err) {
    console.error('❌ Failed to create log directory:', err);
    console.error('Attempted path:', logDir);

    // Try alternative location (user data directory)
    try {
      const alternativeLogDir = path.join(
        app.getPath('userData'),
        'logs'
      );
      if (!fs.existsSync(alternativeLogDir)) {
        fs.mkdirSync(alternativeLogDir, { recursive: true });
        console.log(
          '✅ Created alternative log directory:',
          alternativeLogDir
        );
        return alternativeLogDir;
      }
      return alternativeLogDir;
    } catch (altErr) {
      console.error(
        '❌ Failed to create alternative log directory:',
        altErr
      );
      return null;
    }
  }
  return logDir;
}

// Initialize and get the actual log directory to use
const actualLogDir = ensureLogDirectory();

function writeLog(storeCode, message) {
  try {
    if (!actualLogDir) {
      console.log(`[${storeCode}] ${message}`); // Fallback to console only
      return;
    }

    const filename = `${sanitizeFileName(storeCode)}-log.txt`;
    const fullPath = path.join(actualLogDir, filename);
    const timestamp = new Date().toLocaleString();
    const line = `${timestamp} | ${message}\n`;

    fs.appendFileSync(fullPath, line);
  } catch (err) {
    console.error('❌ Failed to write log:', err);
    console.log(`[${storeCode}] ${message}`); // Fallback to console
  }
}

// Very basic file name sanitizer
function sanitizeFileName(name) {
  return name.replace(/[^a-zA-Z0-9-_]/g, '');
}

module.exports = {
  writeLog,
};

const { app, BrowserWindow, ipcMain } = require('electron');

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED REJECTION:', reason);
});

const path = require('path');
const { loadStoreCodes, saveStoreCode } = require('../shared/config');
const { writeLog } = require('../shared/logger');
const { readSARECORD } = require('../shared/DBFReader');

// Enable electron-reload
require('electron-reload')(path.join(__dirname, '..', '..'), {
  // Only watch the src folder
  // Don't watch logs or generated files
  hardResetMethod: 'exit',
  ignored: [/logs\//, /store-config\.json$/],
});

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 1000,
    height: 700,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
    },
  });

  mainWindow.webContents.openDevTools(); // optional: helpful for debugging

  mainWindow.loadFile(
    path.join(__dirname, '..', 'renderer', 'index.html')
  );
}

ipcMain.handle('get-current-time', async () => {
  return new Date().toLocaleString();
});

ipcMain.handle('get-store-codes', () => {
  return loadStoreCodes();
});

ipcMain.handle('save-store-code', (event, storeId, storeCode) => {
  saveStoreCode(storeId, storeCode);
});

ipcMain.handle('write-log', (event, storeCode, message) => {
  writeLog(storeCode, message); // ← comment this out temporarily
});

ipcMain.handle(
  'read-sarecord',
  async (event, storeCode, backDate) => {
    return await readSARECORD(storeCode, backDate);
  }
);

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

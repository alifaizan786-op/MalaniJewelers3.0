const { app, BrowserWindow, ipcMain } = require('electron');
const {
  getInventoryItemIdsBySKUs,
  updateInventoryLevel,
  evaluateProductForDrafting,
} = require('../shared/shopify');

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

ipcMain.handle(
  'save-store-code',
  (event, storeId, storeCode, shopifyLocationId) => {
    saveStoreCode(storeId, storeCode, shopifyLocationId);
  }
);

ipcMain.handle('write-log', (event, storeCode, message) => {
  writeLog(storeCode, message); // ← comment this out temporarily
});

ipcMain.handle(
  'read-sarecord',
  async (event, storeCode, backDate) => {
    return await readSARECORD(storeCode, backDate);
  }
);

ipcMain.handle(
  'sync-shopify-batch',
  async (event, storeCode, locationId, records) => {
    try {
      const skuList = records
        .map((r) => r.SKU_NO?.trim())
        .filter((sku) => sku);

      const skuMap = await getInventoryItemIdsBySKUs(skuList);

      // ✅ Log into file instead of console
      skuList.forEach((sku) => {
        if (skuMap[sku]) {
          writeLog(
            storeCode,
            `Found SKU ${sku} with inventoryItemId: ${skuMap[sku]}`
          );
        } else {
          writeLog(storeCode, `SKU ${sku} not found in Shopify`);
        }
      });

      let successCount = 0;

      for (const rec of records) {
        const sku = rec.SKU_NO?.trim();
        const qty = parseInt(rec.QTYONHAND || 0);
        const inventoryItemId = skuMap[sku];

        if (!inventoryItemId) {
          continue; // Already logged above
        }

        writeLog(
          storeCode,
          `Attempting to hide SKU ${sku} with inventoryItemId: ${skuMap[sku]}`
        );

        const updated = await updateInventoryLevel(
          inventoryItemId,
          qty,
          locationId
        );

        if (updated) {
          successCount++;
          writeLog(
            storeCode,
            `Successfully hide SKU ${sku} with inventoryItemId: ${skuMap[sku]}`
          );
        }

        // ✅ NEW STEP: Evaluate product status after inventory update
        await evaluateProductForDrafting(
          inventoryItemId,
          locationId,
          storeCode,
          writeLog
        );
      }

      return successCount;
    } catch (err) {
      console.error('Batch sync failed:', err);
      writeLog(storeCode, `❌ Batch sync failed: ${err}`);
      return 0;
    }
  }
);

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

// src/main/main.js
const { app, BrowserWindow, ipcMain } = require('electron');
const {
  batchUpdateInventory,
  getRateLimiterStatus,
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

// Enable electron-reload only in development
const isDev = !app.isPackaged;
if (isDev) {
  try {
    require('electron-reload')(path.join(__dirname, '..', '..'), {
      hardResetMethod: 'exit',
      ignored: [/logs\//, /store-config\.json$/],
    });
  } catch (err) {
    console.log(
      'electron-reload not available (this is normal in production)'
    );
  }
}

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    show: false, // Don't show until ready
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
    },
    // Force window to be visible and on screen
    x: 100,
    y: 100,
    minWidth: 800,
    minHeight: 600,
    // Ensure window appears on top
    alwaysOnTop: false,
    skipTaskbar: false,
    titleBarStyle: 'default',
  });

  // Handle window ready
  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    mainWindow.focus();
    console.log('Window is now visible and focused');
  });

  // Add error handling for window loading
  mainWindow.webContents.on(
    'did-fail-load',
    (event, errorCode, errorDescription) => {
      console.error(
        'Failed to load window:',
        errorCode,
        errorDescription
      );
    }
  );

  // Show dev tools for debugging

  // Force focus after a short delay
  setTimeout(() => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.show();
      mainWindow.focus();
      mainWindow.setAlwaysOnTop(true);
      setTimeout(() => {
        mainWindow.setAlwaysOnTop(false);
      }, 1000);
    }
  }, 2000);

  mainWindow.loadFile(
    path.join(__dirname, '..', 'renderer', 'index.html')
  );

  return mainWindow;
}

// Store active sync operations and sync coordinator
const activeSyncs = new Map();
const syncQueue = [];
let processingQueue = false;

// Sync coordinator to stagger store syncs
async function addToSyncQueue(syncOperation) {
  return new Promise((resolve, reject) => {
    syncQueue.push({ operation: syncOperation, resolve, reject });
    processSyncQueue();
  });
}

async function processSyncQueue() {
  if (processingQueue || syncQueue.length === 0) return;

  processingQueue = true;

  while (syncQueue.length > 0) {
    const { operation, resolve, reject } = syncQueue.shift();

    try {
      const result = await operation();
      resolve(result);
    } catch (error) {
      reject(error);
    }

    // Small delay between store syncs to prevent overwhelming the API
    if (syncQueue.length > 0) {
      await new Promise((resolve) => setTimeout(resolve, 5000)); // Increased to 5 seconds
    }
  }

  processingQueue = false;
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
  writeLog(storeCode, message);
});

ipcMain.handle(
  'read-sarecord',
  async (event, storeCode, backDate) => {
    return await readSARECORD(storeCode, backDate);
  }
);

// Get rate limiter status for monitoring
ipcMain.handle('get-rate-limiter-status', () => {
  return getRateLimiterStatus();
});

// Cancel active sync
ipcMain.handle('cancel-sync', (event, storeCode) => {
  if (activeSyncs.has(storeCode)) {
    activeSyncs.get(storeCode).cancelled = true;
    writeLog(storeCode, '🛑 Sync cancellation requested');
    return true;
  }
  return false;
});

// Enhanced sync with progress tracking and rate limiting
ipcMain.handle(
  'sync-shopify-batch',
  async (event, storeCode, locationId, records) => {
    // Check if sync is already running for this store
    if (activeSyncs.has(storeCode)) {
      writeLog(
        storeCode,
        '⚠️ Sync already in progress for this store'
      );
      return { success: false, error: 'Sync already in progress' };
    }

    // Use sync coordinator to stagger store syncs
    return await addToSyncQueue(async () => {
      // Initialize sync tracking
      const syncState = {
        cancelled: false,
        startTime: Date.now(),
        totalRecords: records.length,
      };
      activeSyncs.set(storeCode, syncState);

      try {
        writeLog(
          storeCode,
          `🚀 Starting enhanced batch sync for ${records.length} records`
        );
        writeLog(
          storeCode,
          `📊 Store-specific rate limiting enabled - processing will be throttled`
        );

        // Progress callback to send updates to renderer
        const progressCallback = (progress) => {
          // Check for cancellation
          if (activeSyncs.get(storeCode)?.cancelled) {
            throw new Error('Sync cancelled by user');
          }

          // Send progress update to renderer
          event.sender.send('sync-progress', {
            storeCode,
            progress: {
              current: progress.current,
              total: progress.total,
              percentage: Math.round(
                (progress.current / progress.total) * 100
              ),
              currentSku: progress.sku,
              phase: progress.phase,
              eta: calculateETA(
                syncState.startTime,
                progress.current,
                progress.total
              ),
            },
          });

          writeLog(
            storeCode,
            `📈 Progress: ${progress.current}/${
              progress.total
            } (${Math.round(
              (progress.current / progress.total) * 100
            )}%) - ${progress.sku}`
          );
        };

        // Enhanced logger that also sends updates to renderer
        const enhancedLogger = (storeCode, message) => {
          writeLog(storeCode, message);

          // Send log update to renderer
          event.sender.send('sync-log-update', {
            storeCode,
            message,
            timestamp: new Date().toLocaleString(),
          });
        };

        // Use the new batch processing function
        const successCount = await batchUpdateInventory(
          records,
          locationId,
          storeCode,
          enhancedLogger,
          progressCallback
        );

        const duration = (Date.now() - syncState.startTime) / 1000;
        const rate = successCount / duration;

        enhancedLogger(storeCode, `🎉 Sync completed successfully!`);
        enhancedLogger(
          storeCode,
          `📊 Final stats: ${successCount}/${
            records.length
          } records processed in ${duration.toFixed(
            1
          )}s (${rate.toFixed(2)} records/sec)`
        );

        // Send completion notification
        event.sender.send('sync-complete', {
          storeCode,
          success: true,
          successCount,
          totalRecords: records.length,
          duration: duration.toFixed(1),
          rate: rate.toFixed(2),
        });

        return {
          success: true,
          successCount,
          totalRecords: records.length,
          duration: duration.toFixed(1),
          rate: rate.toFixed(2),
        };
      } catch (err) {
        const duration = (Date.now() - syncState.startTime) / 1000;
        const errorMessage = err.message || 'Unknown error';

        writeLog(
          storeCode,
          `❌ Batch sync failed after ${duration.toFixed(
            1
          )}s: ${errorMessage}`
        );

        // Send error notification
        event.sender.send('sync-complete', {
          storeCode,
          success: false,
          error: errorMessage,
          duration: duration.toFixed(1),
        });

        return {
          success: false,
          error: errorMessage,
          duration: duration.toFixed(1),
        };
      } finally {
        // Clean up sync tracking
        activeSyncs.delete(storeCode);
      }
    });
  }
);

// Utility function to calculate ETA
function calculateETA(startTime, current, total) {
  if (current === 0) return 'Calculating...';

  const elapsed = (Date.now() - startTime) / 1000;
  const rate = current / elapsed;
  const remaining = total - current;
  const eta = remaining / rate;

  if (eta < 60) {
    return `${Math.round(eta)}s`;
  } else if (eta < 3600) {
    return `${Math.round(eta / 60)}m ${Math.round(eta % 60)}s`;
  } else {
    const hours = Math.floor(eta / 3600);
    const minutes = Math.round((eta % 3600) / 60);
    return `${hours}h ${minutes}m`;
  }
}

// Periodic rate limiter status updates
setInterval(() => {
  const status = getRateLimiterStatus();

  // Only send status if there are active syncs
  if (activeSyncs.size > 0) {
    // Broadcast to all windows (if multiple)
    BrowserWindow.getAllWindows().forEach((window) => {
      window.webContents.send('rate-limiter-status', status);
    });
  }
}, 5000); // Update every 5 seconds

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

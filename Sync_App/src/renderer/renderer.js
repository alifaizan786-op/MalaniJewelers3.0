// src/renderer/renderer.js
const { ipcRenderer } = require('electron');

document.addEventListener('DOMContentLoaded', async () => {
  console.log('Renderer loaded', new Date().toLocaleTimeString());

  const storeIds = ['store-1', 'store-2', 'store-3'];
  const config = await ipcRenderer.invoke('get-store-codes');
  const storeState = {};

  // Initialize store configurations
  storeIds.forEach((storeId) => {
    const storeNum = storeId.split('-')[1];
    const storeCodeInput = document.querySelector(
      `#storeCode-${storeNum}`
    );
    const storeCodeLabel = document.querySelector(
      `#label-storeCode-${storeNum}`
    );
    const locationIdInput = document.querySelector(
      `#shopifyLocationId-${storeNum}`
    );
    const locationIdLabel = document.querySelector(
      `#label-shopifyLocationId-${storeNum}`
    );

    const configEntry = config[storeId];
    if (configEntry) {
      const heading = document.createElement('h2');
      heading.textContent = `${configEntry.storeCode} (Location: ${configEntry.shopifyLocationId})`;
      storeCodeInput.replaceWith(heading);
      storeCodeLabel.remove();
      locationIdInput.remove();
      locationIdLabel.remove();
    } else {
      const saveBtn = document.createElement('button');
      saveBtn.textContent = 'Save Store Config';
      locationIdInput.after(saveBtn);

      saveBtn.addEventListener('click', async () => {
        const storeCode = storeCodeInput.value.trim();
        const locationId = locationIdInput.value.trim();
        if (!storeCode || !locationId) {
          alert('Both Store Code & Shopify Location ID required');
          return;
        }

        await ipcRenderer.invoke(
          'save-store-code',
          storeId,
          storeCode,
          locationId
        );

        const heading = document.createElement('h2');
        heading.textContent = `${storeCode} (Location: ${locationId})`;
        storeCodeInput.replaceWith(heading);
        storeCodeLabel.remove();
        locationIdInput.remove();
        locationIdLabel.remove();
        saveBtn.remove();
      });
    }
  });

  // Initialize sync controls for each store
  storeIds.forEach((storeId) => {
    const storeNum = storeId.split('-')[1];
    const runBtn = document.getElementById(`run-${storeNum}`);
    const backDateInput = document.getElementById(
      `backDate-${storeNum}`
    );
    const intervalInput = document.getElementById(
      `interval-${storeNum}`
    );
    const logBox = document.getElementById(`logBox-${storeNum}`);
    const timestampBox = document.getElementById(
      `timestamp-${storeNum}`
    );
    const statusBox = document.getElementById(`status-${storeNum}`);

    // Add progress bar and cancel button
    const progressContainer = document.createElement('div');
    progressContainer.className = 'progress-container';
    progressContainer.style.display = 'none';
    progressContainer.innerHTML = `
      <div class="progress-bar">
        <div class="progress-fill" id="progress-fill-${storeNum}"></div>
        <div class="progress-text" id="progress-text-${storeNum}">0%</div>
      </div>
      <div class="progress-details" id="progress-details-${storeNum}"></div>
      <button id="cancel-${storeNum}" class="cancel-btn">Cancel Sync</button>
    `;

    runBtn.after(progressContainer);

    const cancelBtn = document.getElementById(`cancel-${storeNum}`);
    const progressFill = document.getElementById(
      `progress-fill-${storeNum}`
    );
    const progressText = document.getElementById(
      `progress-text-${storeNum}`
    );
    const progressDetails = document.getElementById(
      `progress-details-${storeNum}`
    );

    // Cancel button handler
    cancelBtn.addEventListener('click', async () => {
      const configEntry = config[storeId];
      if (configEntry) {
        await ipcRenderer.invoke(
          'cancel-sync',
          configEntry.storeCode
        );
        log(
          `🛑 Cancellation requested`,
          logBox,
          configEntry.storeCode
        );
      }
    });

    // Run button handler
    runBtn?.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();

      const backDays = parseInt(backDateInput?.value || '0');
      const interval = parseInt(intervalInput?.value || '0') * 1000;

      const configEntry = config[storeId];
      if (!configEntry) {
        alert(
          'Store config missing — please save store code and location ID first.'
        );
        return;
      }

      const storeCode = configEntry.storeCode;
      const shopifyLocationId = configEntry.shopifyLocationId;

      storeState[storeId] = {
        firstRun: true,
        storeCode,
        shopifyLocationId,
        backDays,
        interval,
        isRunning: false,
      };

      // Add visual indicator that this store is starting
      const startTime = new Date().toLocaleTimeString();
      log(
        `🚀 Starting enhanced sync with interval ${
          interval / 1000
        }s (Started at ${startTime})`,
        logBox,
        storeCode
      );
      log(
        `⚡ Store-specific rate limiting enabled - each store has its own rate limiter`,
        logBox,
        storeCode
      );

      await syncNow(storeId);

      if (interval && !storeState[storeId].cancelled) {
        storeState[storeId].intervalId = setInterval(
          () => syncNow(storeId),
          interval
        );
        log(
          `🔄 Scheduled sync every ${interval / 1000} seconds`,
          logBox,
          storeCode
        );
      }
    });

    // Main sync function
    async function syncNow(storeId) {
      const state = storeState[storeId];
      if (state.isRunning) {
        log(
          `⚠️ Sync already running, skipping...`,
          logBox,
          state.storeCode
        );
        return;
      }

      state.isRunning = true;
      const { storeCode, shopifyLocationId, backDays, firstRun } =
        state;

      // Update UI
      runBtn.disabled = true;
      progressContainer.style.display = 'block';
      statusBox.textContent = 'Status: Reading DBF';

      try {
        log(`📖 Reading DBF file...`, logBox, storeCode);

        const actualBackDate = firstRun ? backDays : 0;
        const result = await ipcRenderer.invoke(
          'read-sarecord',
          storeCode,
          actualBackDate
        );

        if (!result || result.length === 0) {
          log(`📭 No records found`, logBox, storeCode);
          return;
        }

        log(
          `📊 Found ${result.length} records to process`,
          logBox,
          storeCode
        );

        if (result.length > 100) {
          log(
            `⚡ Large dataset detected - rate limiting will prevent API issues`,
            logBox,
            storeCode
          );
        }

        // Start the enhanced batch sync
        statusBox.textContent = `Status: Syncing ${result.length} records`;

        const syncResult = await ipcRenderer.invoke(
          'sync-shopify-batch',
          storeCode,
          shopifyLocationId,
          result
        );

        if (syncResult.success) {
          log(
            `🎉 Sync completed: ${syncResult.successCount}/${syncResult.totalRecords} in ${syncResult.duration}s`,
            logBox,
            storeCode
          );
          timestampBox.textContent = `Last Sync: ${now()} (${
            syncResult.rate
          } records/sec)`;
        } else {
          log(
            `❌ Sync failed: ${syncResult.error}`,
            logBox,
            storeCode
          );
        }

        state.firstRun = false;
      } catch (err) {
        log(`❌ Sync error: ${err.message}`, logBox, storeCode);
      } finally {
        state.isRunning = false;
        runBtn.disabled = false;
        progressContainer.style.display = 'none';
        statusBox.textContent = 'Status: Idle';

        // Reset progress bar
        progressFill.style.width = '0%';
        progressText.textContent = '0%';
        progressDetails.textContent = '';
      }
    }

    // Listen for sync progress updates
    ipcRenderer.on('sync-progress', (event, data) => {
      if (data.storeCode === storeState[storeId]?.storeCode) {
        const { progress } = data;

        // Update progress bar
        progressFill.style.width = `${progress.percentage}%`;
        progressText.textContent = `${progress.percentage}%`;
        progressDetails.textContent = `${progress.current}/${progress.total} - ${progress.currentSku} (ETA: ${progress.eta})`;

        // Update status
        statusBox.textContent = `Status: ${progress.phase.replace(
          '_',
          ' '
        )} (${progress.percentage}%)`;
      }
    });

    // Listen for sync completion
    ipcRenderer.on('sync-complete', (event, data) => {
      if (data.storeCode === storeState[storeId]?.storeCode) {
        if (data.success) {
          statusBox.textContent = `Status: Complete - ${data.successCount}/${data.totalRecords} in ${data.duration}s`;
          timestampBox.textContent = `Last Sync: ${now()} (${
            data.rate
          } records/sec)`;
        } else {
          statusBox.textContent = `Status: Failed - ${data.error}`;
        }
      }
    });

    // Listen for log updates
    ipcRenderer.on('sync-log-update', (event, data) => {
      if (data.storeCode === storeState[storeId]?.storeCode) {
        const fullLine = `${data.timestamp} | ${data.message}`;
        logBox.innerHTML += fullLine + '<br />';
        logBox.scrollTop = logBox.scrollHeight;
      }
    });
  });

  // Add rate limiter status display
  const rateLimiterStatus = document.createElement('div');
  rateLimiterStatus.id = 'rate-limiter-status';
  rateLimiterStatus.style.cssText = `
    position: fixed;
    top: 10px;
    right: 10px;
    background: rgba(0,0,0,0.8);
    color: #0f0;
    padding: 10px;
    border-radius: 5px;
    font-family: monospace;
    font-size: 12px;
    display: none;
  `;
  document.body.appendChild(rateLimiterStatus);

  // Listen for rate limiter status updates
  ipcRenderer.on('rate-limiter-status', (event, status) => {
    if (status.activeStores > 0) {
      rateLimiterStatus.style.display = 'block';

      let statusHTML = `<div><strong>Rate Limiter Status:</strong></div>`;
      statusHTML += `<div>Active Stores: ${status.activeStores}</div>`;
      statusHTML += `<div>Total Queue: ${status.totalQueueLength}</div>`;

      if (status.stores) {
        statusHTML += `<div style="margin-top: 5px;"><strong>Per Store:</strong></div>`;
        for (const [storeCode, storeStatus] of Object.entries(
          status.stores
        )) {
          statusHTML += `<div style="font-size: 10px; margin-left: 10px;">`;
          statusHTML += `${storeCode}: REST ${storeStatus.restCallsInLastSecond}/${storeStatus.restLimit}/s, `;
          statusHTML += `GQL ${storeStatus.graphqlPointsUsed}/${storeStatus.graphqlLimit}/min, `;
          statusHTML += `Q ${storeStatus.queueLength}`;
          statusHTML += `</div>`;
        }
      }

      rateLimiterStatus.innerHTML = statusHTML;
    } else {
      rateLimiterStatus.style.display = 'none';
    }
  });

  // Utility functions
  function log(text, logBox, storeCode = 'unknown') {
    const fullLine = `${now()} | ${text}`;
    logBox.innerHTML += fullLine + '<br />';
    logBox.scrollTop = logBox.scrollHeight;

    if (storeCode && storeCode !== 'unknown') {
      ipcRenderer.invoke('write-log', storeCode, text);
    }
  }

  function now() {
    return new Date().toLocaleString();
  }
});

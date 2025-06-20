const { ipcRenderer } = require('electron');

document.addEventListener('DOMContentLoaded', async () => {
  console.log('Renderer loaded', new Date().toLocaleTimeString());

  const storeIds = ['store-1', 'store-2', 'store-3'];
  const config = await ipcRenderer.invoke('get-store-codes');

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

  const storeState = {};

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

    runBtn?.addEventListener('click', async (e) => {
      e.preventDefault();
      e.stopPropagation();
      const backDays = parseInt(backDateInput?.value || '0');
      const interval = parseInt(intervalInput?.value || '0') * 1000;

      // Now read from config
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
      };

      log(
        `Starting sync with interval ${interval / 1000}s`,
        logBox,
        storeCode
      );

      syncNow(storeId);
      if (interval) {
        setInterval(() => syncNow(storeId), interval);
      }
    });

    function syncNow(storeId) {
      const state = storeState[storeId];
      const { storeCode, shopifyLocationId, backDays, firstRun } =
        state;

      statusBox.textContent = 'Status: Reading DBF';
      log(`Reading DBF`, logBox, storeCode);

      // Determine what date range to use
      const actualBackDate = firstRun ? backDays : 0;

      ipcRenderer
        .invoke('read-sarecord', storeCode, actualBackDate)
        .then(async (result) => {
          if (!result || result.length === 0) {
            log(`No records found`, logBox, storeCode);
            return;
          }

          result.forEach((record) => {
            const resultSku = record?.SKU_NO?.trim();
            if (resultSku) {
              log(`${resultSku} was sold`, logBox, storeCode);
            }
          });

          statusBox.textContent = `Checking on shopify to find SKU's`;

          log(`Checking on shopify to find SKU's`, logBox, storeCode);

          ipcRenderer
            .invoke(
              'sync-shopify-batch',
              storeCode,
              shopifyLocationId,
              result
            )
            .then((successCount) => {
              log(
                `✅ Synced ${successCount} SKUs`,
                logBox,
                storeCode
              );
              storeState[storeId].firstRun = false;
            });

          // After first run completed, disable backDate for next interval runs
          storeState[storeId].firstRun = false;
        });
    }

    function log(text, logBox, storeCode = 'unknown') {
      const fullLine = `${now()} | ${text}`;
      logBox.innerHTML += fullLine + '<br />';
      logBox.scrollTop = logBox.scrollHeight;
      if (storeCode && storeCode !== 'unknown') {
        ipcRenderer.invoke('write-log', storeCode, text);
      } else {
        console.warn('⚠️ Missing storeCode in log:', fullLine);
      }
    }

    function now() {
      return new Date().toLocaleString();
    }
  });
});

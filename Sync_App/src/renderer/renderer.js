const { ipcRenderer } = require('electron');

document.addEventListener('DOMContentLoaded', async () => {
  console.log('Renderer loaded', new Date().toLocaleTimeString());

  const storeIds = ['store-1', 'store-2', 'store-3'];
  const config = await ipcRenderer.invoke('get-store-codes');

  storeIds.forEach((storeId) => {
    const input = document.querySelector(`#${storeId} input`);
    const label = document.querySelector(`#${storeId} label`);

    const code = config[storeId];

    if (code) {
      const heading = document.createElement('h2');
      heading.textContent = code;
      input.replaceWith(heading);
      label.remove();
    } else {
      const saveBtn = document.createElement('button');
      saveBtn.textContent = 'Save Store Code';
      input.after(saveBtn);

      saveBtn.addEventListener('click', async () => {
        const codeValue = input.value.trim();
        if (!codeValue) return;

        await ipcRenderer.invoke(
          'save-store-code',
          storeId,
          codeValue
        );

        const heading = document.createElement('h2');
        heading.textContent = codeValue;
        input.replaceWith(heading);
        label.remove();
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

    runBtn?.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const backDays = parseInt(backDateInput?.value || '0');
      const interval = parseInt(intervalInput?.value || '0') * 1000;

      const storeCode = document.querySelector(
        `#${storeId} h2`
      )?.textContent;

      if (!storeCode) return alert('Store code is missing.');

      // Save state
      storeState[storeId] = {
        firstRun: true,
        storeCode,
        backDays,
        interval,
      };

      log(
        `Starting sync with interval ${interval / 1000}s`,
        logBox,
        storeCode
      ); // ✅ storeCode passed

      syncNow(storeId);
      if (interval) {
        setInterval(() => syncNow(storeId), interval);
      }
    });

    function syncNow(storeId) {
      const { storeCode, firstRun, backDays } = storeState[storeId];

      // Simulate reading DBF
      statusBox.textContent = 'Status: Reading DBF';
      log(`Reading DBF`, logBox, storeCode);
      
      ipcRenderer.invoke('read-sarecord', storeCode, backDays).then(
        (result) => {
         
        }
      );



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

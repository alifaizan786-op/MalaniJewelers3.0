const fs = require('fs');
const path = require('path');
const { app } = require('electron');

// Get config paths
function getConfigPaths() {
  const primaryPath = app.isPackaged
    ? path.join(path.dirname(app.getPath('exe')), 'store-config.json')
    : path.join(__dirname, '..', '..', 'store-config.json');

  const alternativePath = path.join(
    app.getPath('userData'),
    'store-config.json'
  );

  return { primaryPath, alternativePath };
}

function loadStoreCodes() {
  const { primaryPath, alternativePath } = getConfigPaths();

  // Try primary location first
  try {
    if (fs.existsSync(primaryPath)) {
      const data = fs.readFileSync(primaryPath, 'utf-8');
      if (data.trim()) {
        // Check if file has content
        return JSON.parse(data);
      }
    }
  } catch (err) {
    console.log(
      'Primary config location failed, trying alternative...'
    );
  }

  // Try alternative location
  try {
    if (fs.existsSync(alternativePath)) {
      const data = fs.readFileSync(alternativePath, 'utf-8');
      if (data.trim()) {
        // Check if file has content
        console.log(
          '✅ Loaded config from alternative location:',
          alternativePath
        );
        return JSON.parse(data);
      }
    }
  } catch (err) {
    console.error(
      '❌ Failed to load from alternative location:',
      err
    );
  }

  return {};
}

function saveStoreCode(storeId, storeCode, shopifyLocationId) {
  const { primaryPath, alternativePath } = getConfigPaths();
  const config = loadStoreCodes();
  config[storeId] = { storeCode, shopifyLocationId };

  // Try to save to primary location first
  try {
    fs.writeFileSync(primaryPath, JSON.stringify(config, null, 2));
    console.log('✅ Saved store config to:', primaryPath);
    return;
  } catch (err) {
    console.log(
      '❌ Primary save failed, using alternative location...'
    );
  }

  // Save to alternative location
  try {
    const altDir = path.dirname(alternativePath);
    if (!fs.existsSync(altDir)) {
      fs.mkdirSync(altDir, { recursive: true });
    }

    fs.writeFileSync(
      alternativePath,
      JSON.stringify(config, null, 2)
    );
    console.log(
      '✅ Saved store config to alternative location:',
      alternativePath
    );
  } catch (altErr) {
    console.error(
      '❌ Failed to save to alternative location:',
      altErr
    );
  }
}

module.exports = {
  loadStoreCodes,
  saveStoreCode,
};

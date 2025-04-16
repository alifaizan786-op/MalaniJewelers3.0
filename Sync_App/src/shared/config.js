const fs = require('fs');
const path = require('path');

const configPath = path.join(__dirname, '..', '..', 'store-config.json');

function loadStoreCodes() {
  if (fs.existsSync(configPath)) {
    return JSON.parse(fs.readFileSync(configPath, 'utf-8'));
  }
  return {};
}

function saveStoreCode(storeId, storeCode) {
  const config = loadStoreCodes();
  config[storeId] = storeCode;
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}

module.exports = {
  loadStoreCodes,
  saveStoreCode,
};

const parseDBF = require('parsedbf');
const fs = require('fs');
const path = require('path');

// Read & parse DBF
async function readSARECORD(storeCode, daysBack = 0) {
  const filePath = path.resolve(
    `Z:/VISUALJS/data/${storeCode}/CURRENT/SARECORD.DBF`
  );

  try {
    const buffer = fs.readFileSync(filePath);
    const records = parseDBF(buffer);
    console.log(
      `✅ Read ${storeCode} DBF:`,
      records.length,
      'records'
    );

    // Common SKU filter function
    const excludeDummySkus = (rec) => {
      const sku = rec?.SKU_NO?.toString().trim();
      return !(sku && sku.endsWith('00001'));
    };

    let filteredRecords = [];

    if (daysBack > 0) {
      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - daysBack);

      filteredRecords = records
        .filter((rec) => {
          const date = new Date(rec.DATE);
          return date >= fromDate;
        })
        .filter(excludeDummySkus);

      console.log(
        `Filtered ${storeCode} DBF:`,
        filteredRecords.length,
        'records'
      );
    } else {
      const today = new Date().toISOString().split('T')[0];
      filteredRecords = records
        .filter((rec) => rec.DATE === today)
        .filter(excludeDummySkus);

      console.log(
        `Filtered ${storeCode} DBF for today:`,
        filteredRecords.length,
        'records'
      );
    }

    return filteredRecords;
  } catch (err) {
    console.error(`❌ Failed to read ${storeCode} DBF:`, err);
    throw err;
  }
}

module.exports = {
  readSARECORD,
};

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


    if (daysBack > 0) {
      const fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - daysBack);

      const filtered = records.filter((rec) => {
        const date = new Date(rec.DATE);
        return date >= fromDate;
      });

      console.log(
        `Filtered ${storeCode} DBF:`,
        filtered.length,
        'records'
      );

      // return filtered;
    } else {
      const today = new Date();

      const todayString = today.toISOString().split('T')[0];
      const todayRecords = records.filter((rec) => {
        return rec.DATE === todayString;
      });
      console.log(
        `Filtered ${storeCode} DBF for today:`,
        todayRecords.length,
        'records'
      );
    }

    // if (!daysBack) return records;

    // // Filter by backDate
    // const fromDate = new Date();
    // fromDate.setDate(fromDate.getDate() - daysBack);

    // const filtered = records.filter((rec) => {
    //   const date = new Date(rec.DATE);
    //   return date >= fromDate;
    // });

    // return filtered;
  } catch (err) {
    console.error(`❌ Failed to read ${storeCode} DBF:`, err);
    throw err;
  }
}

module.exports = {
  readSARECORD,
};

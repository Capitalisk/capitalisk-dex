const util = require('util');
const fs = require('fs');
const path = require('path');
const { mapListFields, mapFields } = require('../utils');
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

let snapshotPath = process.argv[2];

function floorDecimalNumberAsString(decimalValue) {
  return String(Math.floor(decimalValue));
}

(async () => {
  let snapshotFilePath = path.resolve(snapshotPath);
  let snapshotJSON = await readFile(snapshotFilePath, {encoding: 'utf8'});
  let snapshot = JSON.parse(snapshotJSON);
  snapshot.orderBook.askLimitOrders = mapListFields(snapshot.orderBook.askLimitOrders, {
    size: floorDecimalNumberAsString,
    sizeRemaining: floorDecimalNumberAsString,
    lastSizeTaken: floorDecimalNumberAsString,
    lastValueTaken: floorDecimalNumberAsString,
    sourceChainAmount: floorDecimalNumberAsString
  });
  snapshot.orderBook.bidLimitOrders = mapListFields(snapshot.orderBook.bidLimitOrders, {
    value: floorDecimalNumberAsString,
    valueRemaining: floorDecimalNumberAsString,
    lastSizeTaken: floorDecimalNumberAsString,
    lastValueTaken: floorDecimalNumberAsString,
    sourceChainAmount: floorDecimalNumberAsString
  });
  let sanitizedSnapshotJSON = JSON.stringify(snapshot);
  await writeFile(snapshotFilePath, sanitizedSnapshotJSON, {encoding: 'utf8'});
})();

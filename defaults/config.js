module.exports = {
  passiveMode: false,
  priceDecimalPrecision: null,
  initRetryDelay: 20000,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  multisigFlushInterval: 15000,
  multisigReadyDelay: 15000,
  multisigMaxBatchSize: 25,
  multisigRetryInterval: 60000,
  recentTransfersExpiry: 1800000,
  signatureFlushInterval: 5000,
  signatureReadyDelay: 10000,
  signatureRetryInterval: 60000,
  signatureMaxBatchSize: 400,
  orderBookSnapshotFinality: 303,
  orderBookUpdateSnapshotDirPath: 'dex-update-snapshots',
  orderBookSnapshotFilePath: 'dex-snapshot.json',
  orderBookSnapshotBackupDirPath: 'dex-snapshot-backups',
  orderBookSnapshotBackupMaxCount: 50,
  baseChain: 'lsk',
  apiIsPublic: true,
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 200,
  apiMaxFilterFields: 10,
  apiEnableAdvancedSorting: false,
  apiEnableAdvancedFiltering: true,
  tradeHistorySize: 10000,
  tradeHistoryUpdateInterval: 10000,
  tradeHistoryUnprocessedTransactionExpiry: 600000,
  readBlocksInterval: 10000,
  // The base chain height at which to enable the DEX.
  dexEnabledFromHeight: 0,
  // Can be used to disable the DEX starting at a specific base chain height.
  dexDisabledFromHeight: null,
  dexDisabledRefundHeightOffset: 303,
  components: {
    logger: {
      fileLogLevel: 'debug',
      consoleLogLevel: 'debug'
    }
  },
  chains: {}
};

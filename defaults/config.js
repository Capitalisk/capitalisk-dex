module.exports = {
  passiveMode: false,
  priceDecimalPrecision: null,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  multisigFlushInterval: 15000,
  multisigReadyDelay: 5000,
  multisigMaxBatchSize: 25,
  signatureFlushInterval: 5000,
  signatureMaxBatchSize: 400,
  orderBookSnapshotFinality: 303,
  orderBookUpdateSnapshotDirPath: './dex-update-snapshots',
  orderBookSnapshotFilePath: './dex-snapshot.json',
  orderBookSnapshotBackupDirPath: './dex-snapshot-backups',
  orderBookSnapshotBackupMaxCount: 50,
  baseChain: 'lsk',
  chainsWhitelistPath: null,
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 100,
  apiMaxFilterFields: 10,
  readBlocksInterval: 3000,
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

module.exports = {
  passiveMode: false,
  signatureBroadcastDelay: 8000,
  transactionSubmitDelay: 3000,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  orderBookSnapshotFinality: 303,
  orderBookSnapshotFilePath: './dex-snapshot.json',
  orderBookSnapshotBackupDirPath: './dex-snapshot-backups',
  orderBookSnapshotBackupMaxCount: 50,
  baseChain: 'lsk',
  chainsWhitelistPath: null,
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 100,
  apiMaxFilterFields: 10,
  readBlocksInterval: 3000,
  // Can be used to disable the DEX starting at a specific baseChain height.
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

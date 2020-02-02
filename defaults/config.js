module.exports = {
  passiveMode: false,
  signatureBroadcastDelay: 8000,
  transactionSubmitDelay: 3000,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  orderBookSnapshotFinality: 303,
  orderBookSnapshotFilePath: './lisk-dex-order-book-snapshot.json',
  orderBookSnapshotBackupDirPath: './order-book-snapshots',
  orderBookSnapshotBackupMaxCount: 50,
  baseChain: 'lsk',
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 100,
  apiMaxFilterFields: 10,
  readBlocksInterval: 3000,
  // Can be used to disable the DEX starting at a specific baseChain height.
  dexDisabledFromHeight: null,
  dexDisabledRefundHeightOffset: 303,
  logger: {
    fileLogLevel: 'debug',
    consoleLogLevel: 'debug'
  },
  chains: {
    lsk: {
      database: 'lisk_test',
      moduleAlias: 'chain',
      walletAddress: '',
      sharedPassphrase: '',
      // encryptedPassphrase: '',
      passphrase: '',
      requiredConfirmations: 2,
      orderHeightExpiry: 259200,
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
      minOrderAmount: 1000000000,
      rebroadcastAfterHeight: 5,
      rebroadcastUntilHeight: 720,
      readMaxBlocks: 1000,
      dividendStartHeight: 0,
      dividendHeightInterval: 60480,
      dividendHeightOffset: 303,
      dividendRate: .9,
      // Can be null if blockchain address system is not compatible with Lisk's.
      walletAddressSystem: 'lisk/v1',
      // Can be used to specify the new address if the DEX has moved to a different wallet.
      dexMovedToAddress: null
    },
    clsk: {
      database: 'capitalisk_test',
      moduleAlias: 'capitalisk',
      walletAddress: '',
      sharedPassphrase: '',
      // encryptedPassphrase: '',
      passphrase: '',
      requiredConfirmations: 2,
      orderHeightExpiry: 259200,
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
      minOrderAmount: 1000000000,
      rebroadcastAfterHeight: 5,
      rebroadcastUntilHeight: 720,
      readMaxBlocks: 1000,
      dividendStartHeight: 0,
      dividendHeightInterval: 60480,
      dividendHeightOffset: 303,
      dividendRate: .9,
      // Can be null if blockchain address system is not compatible with Lisk's.
      walletAddressSystem: 'lisk/v1',
      // Can be used to specify the new address if the DEX has moved to a different wallet.
      dexMovedToAddress: null
    }
  }
};

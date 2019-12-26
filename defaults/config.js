module.exports = {
  passiveMode: false,
  signatureBroadcastDelay: 15000,
  transactionSubmitDelay: 5000,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  orderBookSnapshotFinality: 303,
  orderBookSnapshotFilePath: './lisk-dex-orderbook-snapshot.json',
  orderBookSnapshotBackupDirPath: './orderbook-snapshots',
  orderBookSnapshotBackupMaxCount: 50,
  baseChain: 'lsk',
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 100,
  apiMaxFilterFields: 10,
  readBlocksInterval: 3000,
  logger: {
    fileLogLevel: 'debug',
    consoleLogLevel: 'debug'
  },
  chains: {
    lsk: {
      database: 'lisk_test',
      moduleAlias: 'chain',
      walletAddress: '',
      // sharedEncryptedPassphrase: '', // TODO 2: Use encrypted passphrase
      sharedPassphrase: '',
      // encryptedPassphrase: '' // TODO 2: Use encrypted passphrase
      passphrase: '',
      requiredConfirmations: 2,
      orderHeightExpiry: 259200,
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
      minOrderAmount: 1000000000,
      rebroadcastAfterHeight: 5,
      rebroadcastUntilHeight: 100,
      readMaxBlocks: 1000,
      dividendStartHeight: 0,
      dividendHeightInterval: 60480,
      dividendHeightOffset: 303,
      dividendRate: .9,
      // Can be used to disable the DEX starting at a specific height.
      dexDisabledFromHeight: null,
      // Can be used to specify the new address if the DEX has moved to a different wallet.
      dexMovedToAddress: null
    },
    clsk: {
      database: 'capitalisk_test',
      moduleAlias: 'capitalisk',
      walletAddress: '',
      // sharedEncryptedPassphrase: '', // TODO 2: Use encrypted passphrase
      sharedPassphrase: '',
      // encryptedPassphrase: '' // TODO 2: Use encrypted passphrase
      passphrase: '',
      requiredConfirmations: 2,
      orderHeightExpiry: 259200,
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
      minOrderAmount: 1000000000,
      rebroadcastAfterHeight: 5,
      rebroadcastUntilHeight: 100,
      readMaxBlocks: 1000,
      dividendStartHeight: 0,
      dividendHeightInterval: 60480,
      dividendHeightOffset: 303,
      dividendRate: .9,
      // Can be used to disable the DEX starting at a specific height.
      dexDisabledFromHeight: null,
      // Can be used to specify the new address if the DEX has moved to a different wallet.
      dexMovedToAddress: null
    }
  }
};

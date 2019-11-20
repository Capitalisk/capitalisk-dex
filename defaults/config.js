module.exports = {
  passiveMode: false,
  requiredConfirmations: 2,
  signatureBroadcastDelay: 15000,
  multisigExpiry: 86400000,
  multisigExpiryCheckInterval: 60000,
  orderBookSnapshotFinality: 101,
  orderBookSnapshotFilePath: './lisk-dex-orderbook-snapshot.json',
  orderBookSnapshotBackupDirPath: './orderbook-snapshots',
  orderBookSnapshotBackupMaxCount: 200,
  orderHeightExpiry: 259200,
  baseChain: 'lsk',
  apiDefaultPageLimit: 100,
  apiMaxPageLimit: 100,
  rebroadcastAfterHeight: 5,
  rebroadcastUntilHeight: 100,
  readBlocksInterval: 3000,
  readMaxBlocks: 1000,
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
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
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
      exchangeFeeBase: 10000000,
      exchangeFeeRate: .001,
      // Can be used to disable the DEX starting at a specific height.
      dexDisabledFromHeight: null,
      // Can be used to specify the new address if the DEX has moved to a different wallet.
      dexMovedToAddress: null
    }
  }
};

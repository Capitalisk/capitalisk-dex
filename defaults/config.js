module.exports = {
  requiredConfirmations: 2,
  signatureBroadcastDelay: 15000,
  orderBookSnapshotFinality: 101,
  orderBookSnapshotFilePath: './lisk-dex-orderbook-snapshot.json',
  orderHeightExpiry: 259200,
  baseChain: 'lsk',
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
      exchangeFeeRate: .001
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
      exchangeFeeRate: .001
    }
  },
  // Set to true to disable the DEX.
  dexDisabled: false,
  // This can be used to specify the new address if the DEX has moved to a different wallet.
  dexMovedToAddress: null
};

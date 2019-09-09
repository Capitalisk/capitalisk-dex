module.exports = {
  requiredConfirmations: 101,
  signatureBroadcastDelay: 22000,
  orderBookSnapshotFinality: 101,
  orderBookSnapshotFilePath: './lisk-dex-orderbook-snapshot.json',
  orderHeightExpiry: 259200,
  baseChain: 'lsk',
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
  }
};

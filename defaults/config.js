module.exports = {
  database: 'lisk_dex_main',
  requiredConfirmations: 101,
  baseChain: 'chain',
  chains: {
    chain: {
      database: 'lisk_test',
      walletAddress: '',
      sharedEncryptedPassphrase: '',
      participants: []
    },
    capitalisk: {
      database: 'capitalisk_test',
      walletAddress: '',
      sharedEncryptedPassphrase: '',
      participants: []
    }
  }
};

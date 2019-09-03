module.exports = {
  requiredConfirmations: 101,
  baseChain: 'chain',
  chains: {
    chain: {
      database: 'lisk_test',
      walletAddress: '',
      // sharedEncryptedPassphrase: '', // TODO 2: Use encrypted passphrase
      sharedPassphrase: '',
      // encryptedPassphrase: '' // TODO 2: Use encrypted passphrase
      passphrase: ''
    },
    capitalisk: {
      database: 'capitalisk_test',
      walletAddress: '',
      // sharedEncryptedPassphrase: '', // TODO 2: Use encrypted passphrase
      sharedPassphrase: '',
      // encryptedPassphrase: '' // TODO 2: Use encrypted passphrase
      passphrase: ''
    }
  }
};

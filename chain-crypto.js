const liskCryptography = require('@liskhq/lisk-cryptography');
const liskTransactions = require('@liskhq/lisk-transactions');

class ChainCrypto {
  constructor({chainOptions}) {
    this.sharedPassphrase = chainOptions.sharedPassphrase;
    this.passphrase = chainOptions.passphrase;
  }

  async load() {}

  async unload() {}

  // This method checks that:
  // 1. The signerAddress corresponds to the publicKey.
  // 2. The publicKey corresponds to the signature.
  async verifyTransactionSignature(transaction, signaturePacket) {
    let { signature: signatureToVerify, publicKey, signerAddress } = signaturePacket;
    let expectedAddress = liskCryptography.getAddressFromPublicKey(publicKey);
    if (signerAddress !== expectedAddress) {
      return false;
    }
    let { signature, signSignature, signatures, ...transactionToHash } = transaction;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    return liskCryptography.verifyData(txnHash, signatureToVerify, publicKey);
  }

  prepareTransaction(transactionData) {
    let sharedPassphrase = this.sharedPassphrase;
    let passphrase = this.passphrase;
    let txn = {
      type: 0,
      amount: transactionData.amount.toString(),
      recipientId: transactionData.recipientAddress,
      fee: liskTransactions.constants.TRANSFER_FEE.toString(),
      asset: {},
      timestamp: transactionData.timestamp,
      senderPublicKey: liskCryptography.getAddressAndPublicKeyFromPassphrase(sharedPassphrase).publicKey
    };
    if (transactionData.message != null) {
      txn.asset.data = transactionData.message;
    }
    let preparedTxn = liskTransactions.utils.prepareTransaction(txn, sharedPassphrase);

    let { signature, signSignature, signatures, ...transactionToHash } = preparedTxn;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    let { address: signerAddress, publicKey } = liskCryptography.getAddressAndPublicKeyFromPassphrase(passphrase);

    // The signature needs to be an object with a signerAddress property, the other
    // properties are flexible and depend on the requirements of the underlying blockchain.
    let multisigTxnSignature = {
      signerAddress,
      publicKey,
      signature: liskCryptography.signData(txnHash, passphrase)
    };
    preparedTxn.signatures = [multisigTxnSignature];

    return {transaction: preparedTxn, signature: multisigTxnSignature};
  }
}

module.exports = ChainCrypto;

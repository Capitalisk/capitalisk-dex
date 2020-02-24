const liskCryptography = require('@liskhq/lisk-cryptography');
const liskTransactions = require('@liskhq/lisk-transactions');

class ChainCrypto {
  getPublicKeyFromPassphrase(passphrase) {
    return liskCryptography.getAddressAndPublicKeyFromPassphrase(passphrase).publicKey;
  }

  getAddressFromPublicKey(publicKey) {
    return liskCryptography.getAddressFromPublicKey(publicKey);
  }

  verifyTransactionSignature(transaction, signatureToVerify, publicKey) {
    let {signature, signSignature, ...transactionToHash} = transaction;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    return liskCryptography.verifyData(txnHash, signatureToVerify, publicKey);
  }

  prepareTransaction(transactionData, chainOptions) {
    let {sharedPassphrase, passphrase} = chainOptions;
    let txn = {
      type: 0,
      amount: transactionData.amount.toString(),
      recipientId: transactionData.recipientId,
      fee: liskTransactions.constants.TRANSFER_FEE.toString(),
      asset: {},
      timestamp: transactionData.timestamp,
      senderPublicKey: this.getPublicKeyFromPassphrase(sharedPassphrase)
    };
    if (transactionData.message != null) {
      txn.asset.data = transactionData.message;
    }
    let preparedTxn = liskTransactions.utils.prepareTransaction(txn, sharedPassphrase);

    let {signature, signSignature, ...transactionToHash} = preparedTxn;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    let multisigTxnSignature = liskCryptography.signData(txnHash, passphrase);

    preparedTxn.signatures = [multisigTxnSignature];

    return {transaction: preparedTxn, signature: multisigTxnSignature};
  }
}

module.exports = ChainCrypto;

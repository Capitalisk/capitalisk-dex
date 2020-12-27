const liskCryptography = require('@liskhq/lisk-cryptography');
const liskTransactions = require('@liskhq/lisk-transactions');

class ChainCrypto {
  async verifyTransactionSignature(transaction, signaturePacket) {
    // TODO 222: Ensure that the signaturePacket.signerAddress corresponds to the signaturePacket.publicKey or signature; for stateful chain, will need to fetch the account
    let {singature: signatureToVerify, publicKey} = signaturePacket;
    let {signature, signSignature, ...transactionToHash} = transaction;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    return liskCryptography.verifyData(txnHash, signatureToVerify, publicKey);
  }

  prepareTransaction(transactionData, chainOptions) {
    let {sharedPassphrase, passphrase} = chainOptions;
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

    let {signature, signSignature, ...transactionToHash} = preparedTxn;
    let txnHash = liskCryptography.hash(liskTransactions.utils.getTransactionBytes(transactionToHash));
    let { address: signerAddress, publicKey } = liskCryptography.getAddressAndPublicKeyFromPassphrase(passphrase);
    let multisigTxnSignature = {
      signerAddress,
      publicKey,
      signature: liskCryptography.signData(txnHash, passphrase)
    };
    // Signature needs to be full signaturePacket object and not just a string

    preparedTxn.signatures = [multisigTxnSignature];

    return {transaction: preparedTxn, signature: multisigTxnSignature};
  }
}

module.exports = ChainCrypto;

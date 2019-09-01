const LiskCryptography = require('@liskhq/lisk-cryptography');
const LiskTransactions = require('@liskhq/lisk-transactions');

class LiskAdapter {
  // TODO 222: Need multisignature transaction.
  async signTransaction(transaction, passphrase) {
    let liskTransaction = {
      type: 0,
      amount: transaction.amount,
      recipientId: transaction.recipient,
      fee: await this.fetchFees(),
      asset: {},
      senderPublicKey: LiskCryptography.getAddressAndPublicKeyFromPassphrase(passphrase).publicKey
    };
    return LiskTransactions.utils.prepareTransaction(liskTransaction, passphrase);
  }

  async fetchFees() {
    return LiskTransactions.constants.TRANSFER_FEE.toString();
  }
}

module.exports = LiskAdapter;

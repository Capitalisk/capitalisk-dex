// Minimal ChainCrypto plugin used by the DEX module unit tests. It only has to be
// constructible; the tests exercise code paths which do not reach the chain.
class TestChainCrypto {
  constructor({chainSymbol, chainOptions, logger}) {
    this.chainSymbol = chainSymbol;
    this.chainOptions = chainOptions;
    this.logger = logger;
  }

  async load() {}

  async unload() {}

  async prepareTransaction(transactionData) {
    throw new Error('The test ChainCrypto plugin cannot prepare transactions');
  }

  async verifyTransactionSignature() {
    return false;
  }
}

module.exports = TestChainCrypto;

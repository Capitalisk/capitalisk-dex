const path = require('path');
const CapitaliskDEXModule = require('../../index');

const CHAIN_CRYPTO_LIB_PATH = path.join(__dirname, 'chain-crypto.js');
const LOG_LEVELS = ['debug', 'info', 'warn', 'error', 'fatal'];

function createLogger() {
  let logger = {entries: {}};
  for (let level of LOG_LEVELS) {
    logger.entries[level] = [];
    logger[level] = (...args) => {
      logger.entries[level].push(
        args.map(arg => arg instanceof Error ? arg.message : String(arg)).join(' ')
      );
    };
  }
  return logger;
}

function createChainOptions(chainSymbol, overrides) {
  return {
    moduleAlias: `${chainSymbol}_chain`,
    multisigAddress: `${chainSymbol}DEXWalletAddress`,
    exchangeFeeBase: '1000',
    exchangeFeeRate: 0.001,
    minOrderAmount: '10000',
    orderHeightExpiry: 100,
    requiredConfirmations: 1,
    chainCryptoLibPath: CHAIN_CRYPTO_LIB_PATH,
    ...overrides
  };
}

// Builds a DEX module instance without loading it, so that the pure order
// validation, refund scheduling and API query logic can be unit tested.
function createDEXModule(options) {
  let { baseChainOptions, quoteChainOptions, ...configOverrides } = options || {};
  let config = {
    baseChain: 'lsk',
    priceDecimalPrecision: 4,
    chains: {
      lsk: createChainOptions('lsk', baseChainOptions),
      clsk: createChainOptions('clsk', quoteChainOptions)
    },
    ...configOverrides
  };
  return new CapitaliskDEXModule({
    alias: 'capitalisk_dex',
    config,
    updates: [],
    appConfig: {},
    logger: createLogger(),
    updater: {activeUpdate: null}
  });
}

function addBid(tradeEngine, {id, value, price, height, type, walletAddress}) {
  return tradeEngine.addOrder({
    id,
    type: type || 'limit',
    side: 'bid',
    price,
    value,
    sourceChain: 'lsk',
    targetChain: 'clsk',
    sourceWalletAddress: walletAddress || `${id}SourceWallet`,
    targetWalletAddress: `${id}TargetWallet`,
    senderAddress: walletAddress || `${id}SourceWallet`,
    sourceChainAmount: value,
    height: height == null ? 1 : height,
    timestamp: height == null ? 1 : height
  });
}

function addAsk(tradeEngine, {id, size, price, height, type, walletAddress}) {
  return tradeEngine.addOrder({
    id,
    type: type || 'limit',
    side: 'ask',
    price,
    size,
    sourceChain: 'clsk',
    targetChain: 'lsk',
    sourceWalletAddress: walletAddress || `${id}SourceWallet`,
    targetWalletAddress: `${id}TargetWallet`,
    senderAddress: walletAddress || `${id}SourceWallet`,
    sourceChainAmount: size,
    height: height == null ? 1 : height,
    timestamp: height == null ? 1 : height
  });
}

// Leaves a bid resting in the order book with a valueRemaining which is too small to
// buy a single unit of size at its own price, then rests an ask at that same price.
// The order book skips such a dust maker, so a bid and an ask end up at the same price.
function createLockedOrderBook(tradeEngine, price) {
  addBid(tradeEngine, {id: 'dustBid', value: BigInt(price) * 1000n + BigInt(price) - 1n, price, height: 1});
  addAsk(tradeEngine, {id: 'dustTrimmer', size: 1000n, price, height: 2});
  addAsk(tradeEngine, {id: 'lockedAsk', size: 500000n, price, height: 3});
}

module.exports = {
  createDEXModule,
  createLogger,
  addBid,
  addAsk,
  createLockedOrderBook
};

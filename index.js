'use strict';

const crypto = require('crypto');
const fs = require('fs');
const util = require('util');
const path = require('path');
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const unlink = util.promisify(fs.unlink);
const mkdir = util.promisify(fs.mkdir);
const ProperSkipList = require('proper-skip-list');
const defaultConfig = require('./defaults/config');
const TradeEngine = require('./trade-engine');
const BigIntCalculator = require('./big-int-calculator');
const { mapListFields } = require('./utils');
const packageJSON = require('./package.json');

const DEFAULT_MODULE_ALIAS = 'lisk_dex';
const { LISK_DEX_PASSWORD } = process.env;
const CIPHER_ALGORITHM = 'aes-192-cbc';
const CIPHER_KEY = LISK_DEX_PASSWORD ? crypto.scryptSync(LISK_DEX_PASSWORD, 'salt', 24) : undefined;
const CIPHER_IV = Buffer.alloc(16, 0);
const DEFAULT_MULTISIG_READY_DELAY = 5000;
const DEFAULT_PROTOCOL_EXCLUDE_REASON = false;
const DEFAULT_PROTOCOL_MAX_ARGUMENT_LENGTH = 64;
const DEFAULT_PRICE_DECIMAL_PRECISION = 4;

/**
 * Lisk DEX module specification
 *
 * @namespace Framework.Modules
 * @type {module.LiskDEXModule}
 */
module.exports = class LiskDEXModule {
  constructor({alias, config, updates, appConfig, logger, updater}) {
    this.options = {...defaultConfig, ...config};
    this.appConfig = appConfig;
    this.alias = alias || DEFAULT_MODULE_ALIAS;
    this.updater = updater;
    if (!updates) {
      updates = [];
    }
    for (let update of updates) {
      if (!update.criteria) {
        update.criteria = {};
      }
      if (!update.criteria.baseChainHeight) {
        update.criteria.baseChainHeight = 0;
      }
    }
    updates.sort((a, b) => {
      let critA = a.criteria;
      let critB = b.criteria;
      if (critA.baseChainHeight < critB.baseChainHeight) {
        return -1;
      }
      if (critA.baseChainHeight > critB.baseChainHeight) {
        return 1;
      }
      return 0;
    });
    let updateCount = updates.length;
    for (let i = 1; i < updateCount; i++) {
      let currentUpdate = updates[i];
      let previousUpdate = updates[i - 1];
      if (currentUpdate.criteria.baseChainHeight - previousUpdate.criteria.baseChainHeight <= this.options.orderBookSnapshotFinality) {
        throw new Error(
          `DEX updates ${
            previousUpdate.id
          } and ${
            currentUpdate.id
          } were scheduled too close to each other. There must be at least ${
            this.options.orderBookSnapshotFinality
          } height difference between them`
        );
      }
    }
    this.updates = updates;
    if (this.updater.activeUpdate) {
      this.pendingUpdates = this.updates.filter(update => update.id !== this.updater.activeUpdate.id);
    } else {
      this.pendingUpdates = [...this.updates];
    }
    this.chainSymbols = Object.keys(this.options.chains);
    if (this.chainSymbols.length !== 2) {
      throw new Error('DEX module can only handle on 2 chains');
    }
    this.logger = logger;
    this.multisigWalletInfo = {};
    this.isForked = false;
    this.lastSnapshot = null;
    this.pendingTransfers = new Map();
    this.chainSymbols.forEach((chainSymbol) => {
      this.multisigWalletInfo[chainSymbol] = {
        members: new Set(),
        memberCount: 0,
        requiredSignatureCount: null
      };
    });

    this.passiveMode = this.options.passiveMode;
    this.baseChainSymbol = this.options.baseChain;
    this.quoteChainSymbol = this.chainSymbols.find(chain => chain !== this.baseChainSymbol);
    let baseChainOptions = this.options.chains[this.baseChainSymbol];
    let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
    this.baseAddress = baseChainOptions.walletAddress;
    this.quoteAddress = quoteChainOptions.walletAddress;
    this.multisigReadyDelay = this.options.multisigReadyDelay || DEFAULT_MULTISIG_READY_DELAY;
    this.protocolExcludeReason = this.options.protocolExcludeReason || DEFAULT_PROTOCOL_EXCLUDE_REASON;
    this.protocolMaxArgumentLength = this.options.protocolMaxArgumentLength || DEFAULT_PROTOCOL_MAX_ARGUMENT_LENGTH;

    this.priceDecimalPrecision = this.options.priceDecimalPrecision == null ?
      DEFAULT_PRICE_DECIMAL_PRECISION : this.options.priceDecimalPrecision;

    if (this.priceDecimalPrecision <= 0) {
      throw new Error('DEX module priceDecimalPrecision config must be greater than 0');
    }

    this.validPriceRegex = new RegExp(`^([0-9]+[.]?|[0-9]*[.][0-9]{1,${this.priceDecimalPrecision}})$`);

    this.defaultMaxOrderAmount = BigInt(Number.MAX_SAFE_INTEGER);

    this.tradeEngine = new TradeEngine({
      baseCurrency: this.baseChainSymbol,
      quoteCurrency: this.quoteChainSymbol,
      baseOrderHeightExpiry: baseChainOptions.orderHeightExpiry,
      quoteOrderHeightExpiry: quoteChainOptions.orderHeightExpiry,
      baseMinPartialTake: BigInt(baseChainOptions.minPartialTake || 0),
      quoteMinPartialTake: BigInt(quoteChainOptions.minPartialTake || 0),
      priceDecimalPrecision: this.priceDecimalPrecision
    });
    this.processedHeights = {
      [this.baseChainSymbol]: 0,
      [this.quoteChainSymbol]: 0
    };

    this.chainCrypto = {};
    this.recentPricesSkipList = new ProperSkipList();
    this.recentTransfersSkipList = new ProperSkipList();

    this.timestampTransforms = {};
    this.lastSkippedBlocks = {};

    this.bigIntPriceCalculator = new BigIntCalculator({
      decimalPrecision: this.priceDecimalPrecision
    });

    this.bigIntFeeCalculators = {};
    this.chainExchangeFeeBases = {};

    this.chainSymbols.forEach((chainSymbol) => {
      let chainOptions = this.options.chains[chainSymbol];

      this.timestampTransforms[chainSymbol] = {
        multiplier: chainOptions.timestampMultiplier || 1,
        offset: chainOptions.timestampOffset || 0
      };

      this.bigIntFeeCalculators[chainSymbol] = new BigIntCalculator({
        decimalPrecision: this._getDecimalCount(chainOptions.exchangeFeeRate)
      });

      this.chainExchangeFeeBases[chainSymbol] = BigInt(chainOptions.exchangeFeeBase);

      if (chainOptions.encryptedPassphrase) {
        if (!LISK_DEX_PASSWORD) {
          throw new Error(
            `Cannot decrypt the encryptedPassphrase from the ${
              this.alias
            } module config for the ${
              chainSymbol
            } chain without a valid LISK_DEX_PASSWORD environment variable`
          );
        }
        if (chainOptions.passphrase) {
          throw new Error(
            `The ${
              this.alias
            } module config for the ${
              chainSymbol
            } chain should have either a passphrase or encryptedPassphrase but not both`
          );
        }
        try {
          let decipher = crypto.createDecipheriv(CIPHER_ALGORITHM, CIPHER_KEY, CIPHER_IV);
          let decrypted = decipher.update(chainOptions.encryptedPassphrase, 'hex', 'utf8');
          decrypted += decipher.final('utf8');
          chainOptions.passphrase = decrypted;
        } catch (error) {
          throw new Error(
            `Failed to decrypt encryptedPassphrase in ${
              this.alias
            } config for chain ${
              chainSymbol
            } - Check that the LISK_DEX_PASSWORD environment variable is correct`
          );
        }
      }
      if (chainOptions.encryptedSharedPassphrase) {
        if (!LISK_DEX_PASSWORD) {
          throw new Error(
            `Cannot decrypt the encryptedSharedPassphrase from the ${
              this.alias
            } config for the ${
              chainSymbol
            } chain without a valid LISK_DEX_PASSWORD environment variable`
          );
        }
        if (chainOptions.sharedPassphrase) {
          throw new Error(
            `The ${
              this.alias
            } config for the ${
              chainSymbol
            } chain should have either a sharedPassphrase or encryptedSharedPassphrase but not both`
          );
        }
        try {
          let decipher = crypto.createDecipheriv(CIPHER_ALGORITHM, CIPHER_KEY, CIPHER_IV);
          let decrypted = decipher.update(chainOptions.encryptedSharedPassphrase, 'hex', 'utf8');
          decrypted += decipher.final('utf8');
          chainOptions.sharedPassphrase = decrypted;
        } catch (error) {
          throw new Error(
            `Failed to decrypt encryptedSharedPassphrase in ${
              this.alias
            } config for chain ${
              chainSymbol
            } - Check that the LISK_DEX_PASSWORD environment variable is correct`
          );
        }
      }

      if (chainOptions.chainCryptoLibPath == null) {
        throw new Error(
          `The ${
            this.alias
          } config for chain ${
            chainSymbol
          } should specify a chainCryptoLibPath`
        );
      }

      let ChainCryptoClass = require(chainOptions.chainCryptoLibPath);

      this.chainCrypto[chainSymbol] = new ChainCryptoClass({
        chainSymbol,
        chainOptions,
        logger: this.logger
      });
    });

    if (this.options.dividendLibPath) {
      this.computeDividends = require(this.options.dividendLibPath);
    } else {
      this.computeDividends = async ({chainSymbol, contributionData, chainOptions, memberCount}) => {
        return Object.keys(contributionData).map((walletAddress) => {
          let payableContribution = contributionData[walletAddress] * BigInt(Math.floor(chainOptions.dividendRate * 10000)) / 10000n;
          let totalPayableAmount = payableContribution * BigInt(Math.floor(chainOptions.exchangeFeeRate * 10000)) / 10000n;
          return {
            walletAddress,
            amount: totalPayableAmount / BigInt(memberCount)
          };
        });
      };
    }
  }

  get dependencies() {
    let chainConfigList = Object.values(this.options.chains);
    return ['app', 'network'].concat(chainConfigList.map(chainConfig => chainConfig.moduleAlias));
  }

  static get alias() {
    return DEFAULT_MODULE_ALIAS;
  }

  static get info() {
    return {
      author: 'Jonathan Gros-Dubois',
      version: packageJSON.version,
      name: DEFAULT_MODULE_ALIAS,
    };
  }

  static get migrations() {
    return [];
  }

  static get defaults() {
    return defaultConfig;
  }

  _getDecimalCount(decimalNumber) {
    return (String(decimalNumber).split('.')[1] || '').length;
  }

  _execQueryAgainstIterator(query, sourceIterator, idExtractorFn, allowFiltering, allowSorting) {
    query = query || {};
    let { before, after, limit, sort, ...filterMap } = query;
    if (sort && !this.options.apiEnableAdvancedSorting && !allowSorting) {
      let error = new Error('Advanced sorting is disabled');
      error.name = 'InvalidQueryError';
      throw error;
    }
    let filterFields = Object.keys(filterMap);
    let useFiltering = before || after || filterFields.length;
    if (useFiltering && !this.options.apiEnableAdvancedFiltering && !allowFiltering) {
      let error = new Error(
        'Advanced filtering is disabled'
      );
      error.name = 'InvalidQueryError';
      throw error;
    }
    if (filterFields.length > this.options.apiMaxFilterFields) {
      let error = new Error(
        `Too many custom filter fields were specified in the query - The maximum allowed is ${
          this.options.apiMaxFilterFields
        }`
      );
      error.name = 'InvalidQueryError';
      throw error;
    }
    if (limit == null) {
      limit = this.options.apiDefaultPageLimit;
    }
    if (typeof limit !== 'number') {
      let error = new Error(
        'If specified, the limit parameter of the query must be a number'
      );
      error.name = 'InvalidQueryError';
      throw error;
    }
    if (limit > this.options.apiMaxPageLimit) {
      let error = new Error(
        `The limit parameter of the query cannot be greater than ${
          this.options.apiMaxPageLimit
        }`
      );
      error.name = 'InvalidQueryError';
      throw error;
    }
    let [sortField, sortOrderString] = (sort || '').split(':');
    if (sortOrderString != null && sortOrderString !== 'asc' && sortOrderString !== 'desc') {
      let error = new Error(
        'If specified, the sort order must be either asc or desc'
      );
      error.name = 'InvalidQueryError';
      throw error;
    }
    let sortOrder = sortOrderString === 'desc' ? -1 : 1;
    let iterator;
    if (sortField) {
      let list = [...sourceIterator];
      list.sort((a, b) => {
        let valueA = a[sortField];
        let valueB = b[sortField];
        if (valueA > valueB) {
          return sortOrder;
        }
        if (valueA < valueB) {
          return -sortOrder;
        }
        return 0;
      });
      iterator = list;
    } else {
      iterator = sourceIterator;
    }

    let result = [];
    if (after) {
      let beforeString = before == null ? null : String(before);
      let afterString = String(after);
      let isCapturing = false;
      for (let item of iterator) {
        let itemIdString = String(idExtractorFn(item));
        if (before && itemIdString === beforeString) {
          break;
        }
        if (isCapturing) {
          let itemMatchesFilter = filterFields.every(
            field => String(item[field]) === String(filterMap[field])
          );
          if (itemMatchesFilter) {
            result.push(item);
          }
        } else if (itemIdString === afterString) {
          isCapturing = true;
        }
        if (result.length >= limit) {
          break;
        }
      }
      return result;
    }
    if (before) {
      let previousItems = [];
      let beforeString = String(before);
      for (let item of iterator) {
        if (String(idExtractorFn(item)) === beforeString) {
          let length = previousItems.length;
          let firstIndex = length - limit;
          if (firstIndex < 0) {
            firstIndex = 0;
          }
          result = previousItems.slice(firstIndex, length);
          break;
        }
        let itemMatchesFilter = filterFields.every(
          field => String(item[field]) === String(filterMap[field])
        );
        if (itemMatchesFilter) {
          previousItems.push(item);
        }
      }
      return result;
    }
    for (let item of iterator) {
      let itemMatchesFilter = filterFields.every(
        field => String(item[field]) === String(filterMap[field])
      );
      if (itemMatchesFilter) {
        result.push(item);
      }
      if (result.length >= limit) {
        break;
      }
    }
    return result;
  }

  get events() {
    return [
      'bootstrap',
    ];
  }

  get actions() {
    return {
      getStatus: {
        handler: () => {
          return {
            version: LiskDEXModule.info.version,
            orderBookHash: this.tradeEngine.orderBookHash,
            processedHeights: this.processedHeights,
            baseChain: this.options.baseChain,
            priceDecimalPrecision: this.priceDecimalPrecision,
            chains: {
              [this.baseChainSymbol]: this._getChainInfo(this.baseChainSymbol),
              [this.quoteChainSymbol]: this._getChainInfo(this.quoteChainSymbol)
            },
            pendingUpdates: this.pendingUpdates
          };
        }
      },
      getMarket: {
        handler: () => {
          return {
            baseSymbol: this.baseChainSymbol,
            quoteSymbol: this.quoteChainSymbol
          };
        }
      },
      getBids: {
        handler: (action) => {
          let query = {...action.params};
          // Optimization.
          if (query.sort === 'price:desc') {
            delete query.sort;
          }
          let orderIterator;
          if (query.sourceWalletAddress) {
            // Optimization.
            orderIterator = this.tradeEngine.getSourceWalletBidIterator(query.sourceWalletAddress);
            delete query.sourceWalletAddress;
          } else {
            orderIterator = this.tradeEngine.getBidIteratorFromMax();
          }
          let orderList = this._execQueryAgainstIterator(query, orderIterator, item => item.id);
          return mapListFields(orderList, {
            value: String,
            sourceChainAmount: String,
            valueRemaining: String,
            lastSizeTaken: String,
            lastValueTaken: String
          });
        }
      },
      getAsks: {
        handler: (action) => {
          let query = {...action.params};
          // Optimization.
          if (query.sort === 'price:asc') {
            delete query.sort;
          }
          let orderIterator;
          if (query.sourceWalletAddress) {
            // Optimization.
            orderIterator = this.tradeEngine.getSourceWalletAskIterator(query.sourceWalletAddress);
            delete query.sourceWalletAddress;
          } else {
            orderIterator = this.tradeEngine.getAskIteratorFromMin();
          }
          let orderList = this._execQueryAgainstIterator(query, orderIterator, item => item.id);
          return mapListFields(orderList, {
            size: String,
            sourceChainAmount: String,
            sizeRemaining: String,
            lastSizeTaken: String,
            lastValueTaken: String
          });
        }
      },
      getOrders: {
        handler: (action) => {
          let query = {...action.params};
          let orderIterator;
          if (query.sourceWalletAddress) {
            // Optimization.
            orderIterator = this.tradeEngine.getSourceWalletOrderIterator(query.sourceWalletAddress);
            delete query.sourceWalletAddress;
          } else {
            orderIterator = this.tradeEngine.getOrderIterator();
          }
          let orderList = this._execQueryAgainstIterator(query, orderIterator, item => item.id);
          return mapListFields(orderList, {
            value: String,
            size: String,
            sourceChainAmount: String,
            valueRemaining: String,
            sizeRemaining: String,
            lastSizeTaken: String,
            lastValueTaken: String
          });
        }
      },
      getOrderBook: {
        handler: (action) => {
          let query = {...action.params};
          let { depth } = query;
          if (depth == null) {
            depth = Math.floor(this.options.apiDefaultPageLimit / 2);
          } else {
            delete query.depth;
          }
          if (typeof depth != 'number') {
            let error = new Error(
              'If specified, the depth parameter of the query must be a number'
            );
            error.name = 'InvalidQueryError';
            throw error;
          }
          let doubleDepth = depth * 2;
          let askLevelIterator = this.tradeEngine.getAskLevelIteratorFromMin();
          let bidLevelIterator = this.tradeEngine.getBidLevelIteratorFromMax();

          let orderBook = [];
          let lastEntry = {};

          for (let askLevel of askLevelIterator) {
            if (askLevel.price === lastEntry.price) {
              lastEntry.sizeRemaining += askLevel.sizeRemaining;
            } else {
              if (orderBook.length >= depth) break;
              lastEntry = {
                side: 'ask',
                price: askLevel.price,
                sizeRemaining: askLevel.sizeRemaining
              };
              orderBook.push(lastEntry);
            }
          }

          orderBook.reverse();

          for (let bidLevel of bidLevelIterator) {
            if (bidLevel.price === lastEntry.price) {
              lastEntry.valueRemaining += bidLevel.valueRemaining;
            } else {
              if (orderBook.length >= doubleDepth) break;
              lastEntry = {
                side: 'bid',
                price: bidLevel.price,
                valueRemaining: bidLevel.valueRemaining
              };
              orderBook.push(lastEntry);
            }
          }

          let orderLevelList = this._execQueryAgainstIterator(query, orderBook, item => item.price);
          return mapListFields(orderLevelList, {
            valueRemaining: String,
            sizeRemaining: String
          });
        }
      },
      getRecentPrices: {
        handler: (action) => {
          let priceEntryIterator = this.recentPricesSkipList.findEntriesFromMax();
          let priceGenerator = this._getValuesGenerator(priceEntryIterator);
          let recentPricesList = this._execQueryAgainstIterator(action.params, priceGenerator, item => item.baseTimestamp);
          return mapListFields(recentPricesList, {
            volume: String
          });
        }
      },
      getPendingTransfers: {
        handler: (action) => {
          let transferList = this._execQueryAgainstIterator(
            action.params,
            this.pendingTransfers.values(),
            item => item.id,
            true,
            true
          );
          return transferList.map((transfer) => {
            let { signatures, ...transactionWithoutSignatures } = transfer.transaction;
            return {
              id: transfer.id,
              transaction: transactionWithoutSignatures,
              recipientAddress: transfer.recipientAddress,
              targetChain: transfer.targetChain,
              collectedSignatureCount: transfer.processedSignerAddressSet.size,
              contributors: [...transfer.processedSignerAddressSet],
              timestamp: transfer.timestamp,
              type: transfer.type,
              originOrderId: transfer.originOrderId,
              closerOrderId: transfer.closerOrderId,
              takerOrderId: transfer.takerOrderId,
              makerOrderId: transfer.makerOrderId,
              makerCount: transfer.makerCount
            };
          });
        }
      },
      getRecentTransfers: {
        handler: (action) => {
          let recentTransfersIterator = this.recentTransfersSkipList.findEntriesFromMax();
          let transferGenerator = this._getNestedObjectValuesGenerator(recentTransfersIterator);
          let transferList = this._execQueryAgainstIterator(action.params, transferGenerator, item => item.id, true);
          return transferList.map((transfer) => {
            let { signatures, ...transactionWithoutSignatures } = transfer.transaction;
            return {
              id: transfer.id,
              transaction: transactionWithoutSignatures,
              recipientAddress: transfer.recipientAddress,
              targetChain: transfer.targetChain,
              collectedSignatureCount: transfer.processedSignerAddressSet.size,
              contributors: [...transfer.processedSignerAddressSet],
              timestamp: transfer.timestamp,
              type: transfer.type,
              originOrderId: transfer.originOrderId,
              closerOrderId: transfer.closerOrderId,
              takerOrderId: transfer.takerOrderId,
              makerOrderId: transfer.makerOrderId,
              makerCount: transfer.makerCount
            };
          });
        }
      }
    };
  }

  *_getValuesGenerator(entriesIterator) {
    for (let [key, value] of entriesIterator) {
      yield value;
    }
  }

  *_getNestedObjectValuesGenerator(iterator) {
    for (let [key, nestedObject] of iterator) {
      let values = Object.values(nestedObject);
      for (let value of values) {
        yield value;
      }
    }
  }

  _getChainInfo(chainSymbol) {
    let chainOptions = this.options.chains[chainSymbol];
    let multisigWalletInfo = this.multisigWalletInfo[chainSymbol];
    return {
      walletAddressSystem: chainOptions.walletAddressSystem,
      walletAddress: chainOptions.walletAddress,
      multisigMembers: [...multisigWalletInfo.members],
      multisigRequiredSignatureCount: multisigWalletInfo.requiredSignatureCount,
      minOrderAmount: String(chainOptions.minOrderAmount || 0n),
      maxOrderAmount: String(
        chainOptions.maxOrderAmount == null ? this.defaultMaxOrderAmount : chainOptions.maxOrderAmount
      ),
      minPartialTake: String(chainOptions.minPartialTake || 0n),
      exchangeFeeBase: String(chainOptions.exchangeFeeBase),
      exchangeFeeRate: chainOptions.exchangeFeeRate,
      requiredConfirmations: chainOptions.requiredConfirmations,
      orderHeightExpiry: chainOptions.orderHeightExpiry
    };
  }

  _getSignatureQuota(targetChain, transaction) {
    return transaction.signatures.length - (this.multisigWalletInfo[targetChain] || {}).requiredSignatureCount;
  }

  async _verifySignature(targetChain, transaction, signaturePacket) {
    let hasMemberAddress = this.multisigWalletInfo[targetChain].members.has(signaturePacket.signerAddress);
    if (!hasMemberAddress) {
      return false;
    }
    return this.chainCrypto[targetChain].verifyTransactionSignature(transaction, signaturePacket);
  }

  async _processSignature(signatureData) {
    let { signaturePacket, transactionId } = signatureData;
    if (!signaturePacket) {
      signaturePacket = {};
    }
    let transfer = this.pendingTransfers.get(transactionId);
    let { signerAddress } = signaturePacket;
    if (!transfer) {
      throw new Error(
        `Could not find a pending transfer ${
          transactionId
        } to match the signature from the signer ${
          signerAddress
        }`
      );
    }
    let { transaction, processedSignerAddressSet, targetChain } = transfer;
    if (processedSignerAddressSet.has(signerAddress)) {
      throw new Error(
        `A signature from the signer ${
          signerAddress
        } has already been received for the transaction ${
          transactionId
        }`
      );
    }

    let isValidSignature = await this._verifySignature(targetChain, transaction, signaturePacket);
    if (!isValidSignature) {
      throw new Error(
        `The signature from the signer ${
          signerAddress
        } for the transaction ${
          transactionId
        } was invalid or did not correspond to the account in its current state`
      );
    }

    processedSignerAddressSet.add(signerAddress);
    transaction.signatures.push(signaturePacket);

    let signatureQuota = this._getSignatureQuota(targetChain, transaction);
    if (signatureQuota >= 0 && transfer.readyTimestamp == null) {
      transfer.readyTimestamp = Date.now();
    }
  }

  expireMultisigTransactions() {
    let now = Date.now();
    for (let [txnId, transfer] of this.pendingTransfers) {
      if (now - transfer.timestamp < this.options.multisigExpiry) {
        break;
      }
      this.pendingTransfers.delete(txnId);
    }
  }

  flushPendingMultisigTransactions() {
    let transactionsToBroadcastPerChain = {};
    let now = Date.now();

    for (let transfer of this.pendingTransfers.values()) {
      if (transfer.readyTimestamp != null && transfer.readyTimestamp + this.multisigReadyDelay <= now) {
        if (!transactionsToBroadcastPerChain[transfer.targetChain]) {
          transactionsToBroadcastPerChain[transfer.targetChain] = [];
        }
        transactionsToBroadcastPerChain[transfer.targetChain].push(transfer.transaction);
      }
    }

    let chainSymbolList = Object.keys(transactionsToBroadcastPerChain);
    for (let chainSymbol of chainSymbolList) {
      let maxBatchSize = this.options.multisigMaxBatchSize;
      let transactionsToBroadcast = transactionsToBroadcastPerChain[chainSymbol].slice(0, maxBatchSize);
      this._broadcastTransactionsToChain(chainSymbol, transactionsToBroadcast);
    }
  }

  flushPendingSignatures() {
    let signaturesToBroadcast = [];

    for (let transfer of this.pendingTransfers.values()) {
      for (let signaturePacket of transfer.transaction.signatures) {
        signaturesToBroadcast.push({
          signaturePacket,
          transactionId: transfer.transaction.id
        });
      }
    }

    if (signaturesToBroadcast.length) {
      let maxBatchSize = this.options.signatureMaxBatchSize;
      this._broadcastSignaturesToSubnet(
        signaturesToBroadcast.slice(0, maxBatchSize)
      );
    }
  }

  async _broadcastTransactionsToChain(targetChain, transactions) {
    let chainOptions = this.options.chains[targetChain];
    if (chainOptions && chainOptions.moduleAlias) {
      for (let transaction of transactions) {
        try {
          await this.channel.invoke(`${chainOptions.moduleAlias}:postTransaction`, {
            transaction
          });
        } catch (error) {
          this.logger.error(
            `Error encountered while attempting to post transaction ${
              transaction.id
            } to the ${targetChain} network - ${error.message}`
          );
        }
      }
    }
  }

  _getExpectedCounterpartyTransactionCount(transaction) {
    let transactionData = transaction.message || '';
    let header = transactionData.split(':')[0];
    let parts = header.split(',');
    let txnType = parts[0];
    if (txnType === 't1') {
      return parts[3] || 1;
    }
    if (txnType === 't2') {
      return 1;
    }
    return 0;
  }

  _getTakerOrderIdFromTransaction(transaction) {
    let transactionData = transaction.message || '';
    let header = transactionData.split(':')[0];
    let parts = header.split(',');
    let txnType = parts[0];
    if (txnType === 't1') {
      return parts[2];
    }
    if (txnType === 't2') {
      return parts[3];
    }
    return null;
  }

  _isTakerTransaction(transaction) {
    let transactionData = transaction.message || '';
    let header = transactionData.split(':')[0];
    return header.split(',')[0] === 't1';
  }

  _isMakerTransaction(transaction) {
    let transactionData = transaction.message || '';
    let header = transactionData.split(':')[0];
    return header.split(',')[0] === 't2';
  }

  async _getRecentPrices(fromBaseTimestamp, fromQuoteTimestamp) {
    let tradeHistorySize = this.options.tradeHistorySize;
    if (!tradeHistorySize) {
      return [];
    }
    let [baseChainTxns, quoteChainTxns] = await Promise.all([
      this._getOutboundTransactions(this.baseChainSymbol, this.baseAddress, fromBaseTimestamp, tradeHistorySize),
      this._getOutboundTransactions(this.quoteChainSymbol, this.quoteAddress, fromQuoteTimestamp, tradeHistorySize)
    ]);

    let quoteChainMakers = {};
    let quoteChainTakers = {};

    for (let txn of quoteChainTxns) {
      let isMaker = this._isMakerTransaction(txn);
      let isTaker = this._isTakerTransaction(txn);
      let takerOrderId = this._getTakerOrderIdFromTransaction(txn);
      if (isMaker) {
        if (!quoteChainMakers[takerOrderId]) {
          quoteChainMakers[takerOrderId] = [];
        }
        quoteChainMakers[takerOrderId].push(txn);
      } else if (isTaker) {
        quoteChainTakers[takerOrderId] = [txn];
      }
    }

    let txnPairsMap = {};

    for (let txn of baseChainTxns) {
      let isMaker = this._isMakerTransaction(txn);
      let isTaker = this._isTakerTransaction(txn);

      if (!isMaker && !isTaker) {
        continue;
      }

      let counterpartyTakerId = this._getTakerOrderIdFromTransaction(txn);
      let counterpartyTxns = quoteChainMakers[counterpartyTakerId] || quoteChainTakers[counterpartyTakerId] || [];

      if (!counterpartyTxns.length) {
        continue;
      }

      // Group base chain orders which were matched with the same counterparty order together.
      if (!txnPairsMap[counterpartyTakerId]) {
        txnPairsMap[counterpartyTakerId] = {
          base: [],
          quote: counterpartyTxns
        };
      }
      let txnPair = txnPairsMap[counterpartyTakerId];
      txnPair.base.push(txn);
    }

    let priceHistory = [];

    // Filter out all entries which are incompete.
    let txnPairsList = Object.values(txnPairsMap).filter((txnPair) => {
      let firstBaseTxn = txnPair.base[0];
      let firstQuoteTxn = txnPair.quote[0];
      if (!firstBaseTxn || !firstQuoteTxn) {
        return false;
      }
      let expectedBaseCount = this._getExpectedCounterpartyTransactionCount(firstQuoteTxn);
      let expectedQuoteCount = this._getExpectedCounterpartyTransactionCount(firstBaseTxn);

      return txnPair.base.length >= expectedBaseCount && txnPair.quote.length >= expectedQuoteCount;
    });

    for (let txnPair of txnPairsList) {
      let baseChainOptions = this.options.chains[this.baseChainSymbol];
      let baseChainFeeBase = this.chainExchangeFeeBases[this.baseChainSymbol];
      let baseChainFeeRate = baseChainOptions.exchangeFeeRate;
      let baseTotalFee = baseChainFeeBase * BigInt(txnPair.base.length);
      let baseCalc = this.bigIntFeeCalculators[this.baseChainSymbol];
      let fullBaseAmount = txnPair.base.reduce(
        (accumulator, txn) => {
          return accumulator + baseCalc.divideBigIntByDecimal(BigInt(txn.amount), 1 - baseChainFeeRate);
        },
        0n
      ) + baseTotalFee;

      let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
      let quoteChainFeeBase = this.chainExchangeFeeBases[this.quoteChainSymbol];
      let quoteChainFeeRate = quoteChainOptions.exchangeFeeRate;
      let quoteTotalFee = quoteChainFeeBase * BigInt(txnPair.quote.length);
      let quoteCalc = this.bigIntFeeCalculators[this.quoteChainSymbol];
      let fullQuoteAmount = txnPair.quote.reduce(
        (accumulator, txn) => {
          return accumulator + quoteCalc.divideBigIntByDecimal(BigInt(txn.amount), 1 - quoteChainFeeRate);
        },
        0n
      ) + quoteTotalFee;

      let price = this.bigIntPriceCalculator.divideBigIntByBigInt(fullBaseAmount, fullQuoteAmount);

      priceHistory.push({
        baseTimestamp: txnPair.base[txnPair.base.length - 1].timestamp,
        quoteTimestamp: txnPair.quote[txnPair.quote.length - 1].timestamp,
        price,
        volume: fullBaseAmount
      });
    }

    return priceHistory;
  }

  async updateTradeHistory() {
    let latestPriceItem = this.recentPricesSkipList.maxValue() || {};
    let recentPriceList;
    try {
      recentPriceList = await this._getRecentPrices(latestPriceItem.baseTimestamp, latestPriceItem.quoteTimestamp);
    } catch (error) {
      this.logger.error(
        `Failed to fetch recent trade history because of error: ${
          error.message
        }`
      );
      return;
    }

    let mergedPriceMap = new Map();

    for (let priceEntry of recentPriceList) {
      let existingPriceEntry = mergedPriceMap.get(priceEntry.baseTimestamp);
      if (existingPriceEntry) {
        let existingEntryWeightedPrice = this.bigIntPriceCalculator.multiplyBigIntByDecimal(
          existingPriceEntry.volume,
          existingPriceEntry.price
        );
        let newEntryWeightedPrice = this.bigIntPriceCalculator.multiplyBigIntByDecimal(
          priceEntry.volume,
          priceEntry.price
        );
        let totalVolume = existingPriceEntry.volume + priceEntry.volume;
        existingPriceEntry.volume = totalVolume;
        existingPriceEntry.price = this.bigIntPriceCalculator.divideBigIntByBigInt(
          existingEntryWeightedPrice + newEntryWeightedPrice,
          totalVolume
        );
      } else {
        mergedPriceMap.set(priceEntry.baseTimestamp, priceEntry);
      }
    }
    for (let priceItem of mergedPriceMap.values()) {
      this.recentPricesSkipList.upsert(priceItem.baseTimestamp, priceItem);
    }
    while (this.recentPricesSkipList.length > this.options.tradeHistorySize) {
      this.recentPricesSkipList.delete(this.recentPricesSkipList.minKey());
    }
  }

  async load(channel) {
    this.channel = channel;

    await Promise.all(
      this.chainSymbols.map(async (chainSymbol) => {
        return this.chainCrypto[chainSymbol].load(channel);
      })
    );

    this._multisigExpiryInterval = setInterval(() => {
      this.expireMultisigTransactions();
    }, this.options.multisigExpiryCheckInterval);

    this._multisigFlushInterval = setInterval(() => {
      this.flushPendingMultisigTransactions();
    }, this.options.multisigFlushInterval);

    this._signatureFlushInterval = setInterval(() => {
      this.flushPendingSignatures();
    }, this.options.signatureFlushInterval);

    await this.updateTradeHistory();

    this._tradeHistoryUpdateInterval = setInterval(() => {
      this.updateTradeHistory();
    }, this.options.tradeHistoryUpdateInterval);

    await this.channel.invoke('app:updateModuleState', {
      [this.alias]: {
        baseAddress: this.baseAddress,
        quoteAddress: this.quoteAddress
      }
    });

    let hasMultisigWalletsInfo = false;

    this.channel.subscribe(`network:event:${this.alias}:signatures`, async ({data}) => {
      if (!hasMultisigWalletsInfo) {
        return;
      }
      let signatureDataList = Array.isArray(data) ? data.slice(0, this.options.signatureMaxBatchSize) : [];
      await Promise.all(
        signatureDataList.map(async (signatureData) => {
          try {
            await this._processSignature(signatureData || {});
          } catch (error) {
            this.logger.debug(
              `Failed to process signature because of error: ${error.message}`
            );
          }
        })
      );
    });

    try {
      await mkdir(this.options.orderBookSnapshotBackupDirPath, {recursive: true});
    } catch (error) {
      this.logger.error(
        `Failed to create snapshot directory ${
          this.options.orderBookSnapshotBackupDirPath
        } because of error: ${
          error.message
        }`
      );
    }

    let loadMultisigWalletInfo = async () => {
      return Promise.all(
        this.chainSymbols.map(async (chainSymbol) => {
          let chainOptions = this.options.chains[chainSymbol];
          let multisigMembers = await this._getMultisigWalletMembers(chainSymbol, chainOptions.walletAddress);
          let multisigMemberSet = new Set(multisigMembers);
          this.multisigWalletInfo[chainSymbol].members = multisigMemberSet;
          this.multisigWalletInfo[chainSymbol].memberCount = multisigMemberSet.size;
          this.multisigWalletInfo[chainSymbol].requiredSignatureCount = await this._getMinMultisigRequiredSignatures(chainSymbol, chainOptions.walletAddress);
        })
      );
    };

    let lastProcessedTimestamp = null;
    try {
      lastProcessedTimestamp = await this.loadSnapshot();
    } catch (error) {
      this.logger.error(
        `Failed to load initial snapshot because of error: ${error.message} - DEX node will start with an empty order book`
      );
    }

    let processBlock = async ({chainSymbol, chainHeight, latestChainHeights, blockData}) => {
      this.logger.info(
        `Chain ${chainSymbol}: Processing block at height ${chainHeight}`
      );

      let baseChainHeight = latestChainHeights[this.baseChainSymbol];
      if (baseChainHeight < this.options.dexEnabledFromHeight) {
        this.logger.info(
          `Base chain height ${baseChainHeight} is below the DEX enabled height of ${this.options.dexEnabledFromHeight}`
        );
        return;
      }
      if (!hasMultisigWalletsInfo) {
        await loadMultisigWalletInfo();
        hasMultisigWalletsInfo = true;
        this.logger.info(
          `Loaded DEX wallets info`
        );
      }

      let chainOptions = this.options.chains[chainSymbol];
      let minOrderAmount = BigInt(chainOptions.minOrderAmount || 0);
      let maxOrderAmount = chainOptions.maxOrderAmount == null ? this.defaultMaxOrderAmount : BigInt(chainOptions.maxOrderAmount);

      let latestBlockTimestamp = blockData.timestamp;

      if (chainSymbol === this.baseChainSymbol) {
        // Process pending updates.
        let updatesToActivate = [];

        for (let update of this.pendingUpdates) {
          let targetHeight = update.criteria.baseChainHeight;
          if (targetHeight > baseChainHeight) {
            break;
          }
          if (targetHeight === baseChainHeight) {
            updatesToActivate.push(update);
          }
        }

        if (
          this.updater.activeUpdate &&
          this.updater.activeUpdate.criteria.baseChainHeight <= baseChainHeight - this.options.orderBookSnapshotFinality
        ) {
          let updateSnapshotFilePath = this._getUpdateSnapshotFilePath(this.updater.activeUpdate.id);
          this.updater.mergeActiveUpdate();
          try {
            await unlink(updateSnapshotFilePath);
          } catch (error) {
            this.logger.error(
              `Failed to delete update snapshot file at path ${
                updateSnapshotFilePath
              } because of error: ${error.message}`
            );
          }
        }

        if (updatesToActivate.length) {
          let update = updatesToActivate[0];
          if (updatesToActivate.length > 1) {
            this.logger.error('Cannot activate multiple updates on the same base chain height');
          } else if (this.updater.activeUpdate) {
            this.logger.error(
              `Cannot activate update with id ${
                update.id
              } because an existing update with id ${
                this.updater.activeUpdate.id
              } is already active and has not been merged`
            );
          } else {
            let currentOrderBook = this.tradeEngine.getSnapshot();
            let processedChainHeights = {...latestChainHeights};
            processedChainHeights[this.baseChainSymbol]--;
            let snapshot = {
              orderBook: currentOrderBook,
              chainHeights: processedChainHeights
            };
            let updateSnapshotFilePath = this._getUpdateSnapshotFilePath(update.id);
            let error;
            try {
              await mkdir(this.options.orderBookUpdateSnapshotDirPath, {recursive: true});
              await this.saveSnapshot(snapshot, updateSnapshotFilePath);
            } catch (err) {
              error = err;
              this.logger.fatal(`Failed to save snapshot before update because of error: ${error.message}`);
            }
            if (!error) {
              this.updater.activateUpdate(update);
            }
            process.exit();
          }
        }

        // Make a new snapshot every orderBookSnapshotFinality blocks.
        if (chainHeight % this.options.orderBookSnapshotFinality === 0) {
          let currentOrderBook = this.tradeEngine.getSnapshot();
          if (this.lastSnapshot) {
            try {
              await this.saveSnapshot(this.lastSnapshot);
            } catch (error) {
              this.logger.error(`Failed to save snapshot because of error: ${error.message}`);
            }
          }
          let processedChainHeights = {...latestChainHeights};
          processedChainHeights[this.baseChainSymbol]--;
          this.lastSnapshot = {
            orderBook: currentOrderBook,
            chainHeights: processedChainHeights
          };
        }
        if (
          !this.passiveMode &&
          this.options.dexDisabledFromHeight != null &&
          chainHeight % this.options.dexDisabledFromHeight === 0
        ) {
          let currentOrderBook = this.tradeEngine.getSnapshot();
          this.tradeEngine.clear();
          try {
            await this.refundOrderBook(
              {
                orderBook: currentOrderBook,
                chainHeights: {...latestChainHeights}
              },
              latestBlockTimestamp,
              {
                [this.baseChainSymbol]: this.options.chains[this.baseChainSymbol].dexMovedToAddress,
                [this.quoteChainSymbol]: this.options.chains[this.quoteChainSymbol].dexMovedToAddress
              }
            );
          } catch (error) {
            this.logger.error(
              `Failed to refund the order book according to config because of error: ${error.message}`
            );
          }
        }
      }

      if (!blockData.numberOfTransactions) {
        this.logger.info(
          `Chain ${chainSymbol}: No transactions in block ${blockData.id} at height ${chainHeight}`
        );
      }

      // The height pointer for dividends needs to be delayed so that DEX member dividends are only distributed
      // over a range where there is no risk of fork in the underlying blockchain.
      let dividendTargetHeight = chainHeight - chainOptions.dividendHeightOffset;
      if (
        dividendTargetHeight > chainOptions.dividendStartHeight &&
        dividendTargetHeight % chainOptions.dividendHeightInterval === 0
      ) {
        try {
          await processDividends({
            chainSymbol,
            chainHeight,
            toHeight: dividendTargetHeight,
            latestBlockTimestamp
          });
        } catch (error) {
          this.logger.error(
            `Failed to process dividends at target height ${dividendTargetHeight} because of error: ${error.message}`
          );
        }
      }

      let blockTransactions = await Promise.all([
        this._getInboundTransactionsFromBlock(chainSymbol, chainOptions.walletAddress, blockData.id),
        this._getOutboundTransactionsFromBlock(chainSymbol, chainOptions.walletAddress, blockData.id)
      ]);

      let [inboundTxns, outboundTxns] = blockTransactions;

      outboundTxns.forEach((txn) => {
        let pendingTransfer = this.pendingTransfers.get(txn.id);
        if (pendingTransfer) {
          let recentTransfer = {...pendingTransfer, transaction: txn};
          let existingRecentTransfers = this.recentTransfersSkipList.find(recentTransfer.timestamp);
          if (existingRecentTransfers) {
            existingRecentTransfers[txn.id] = recentTransfer;
          } else {
            this.recentTransfersSkipList.upsert(recentTransfer.timestamp, {[txn.id]: recentTransfer});
          }
          this.pendingTransfers.delete(txn.id);
        }
      });

      let expiryTimestamp = Date.now() - this.options.recentTransfersExpiry;
      this.recentTransfersSkipList.deleteRange(0, expiryTimestamp, true);

      let orders = inboundTxns.map((txn) => {
        let orderTxn = {...txn};
        orderTxn.sourceChain = chainSymbol;
        orderTxn.sourceWalletAddress = orderTxn.senderAddress;
        let amount = BigInt(orderTxn.amount);

        let transferMessageString = orderTxn.message == null ? '' : orderTxn.message;

        if (transferMessageString === 'credit') {
          // The credit operation does nothing - The DEX wallet will simply accept the tokens.
          orderTxn.type = 'credit';
          return orderTxn;
        }

        if (amount > maxOrderAmount) {
          orderTxn.type = 'oversized';
          orderTxn.sourceChainAmount = amount;
          this.logger.debug(
            `Chain ${chainSymbol}: Incoming order ${orderTxn.id} amount ${orderTxn.sourceChainAmount.toString()} was too large - Maximum order amount is ${maxOrderAmount}`
          );
          return orderTxn;
        }

        orderTxn.sourceChainAmount = amount;

        if (
          this.options.dexDisabledFromHeight != null &&
          chainHeight >= this.options.dexDisabledFromHeight
        ) {
          if (chainOptions.dexMovedToAddress) {
            orderTxn.type = 'moved';
            orderTxn.movedToAddress = chainOptions.dexMovedToAddress;
            this.logger.debug(
              `Chain ${chainSymbol}: Cannot process order ${orderTxn.id} because the DEX has moved to the address ${chainOptions.dexMovedToAddress}`
            );
            return orderTxn;
          }
          orderTxn.type = 'disabled';
          this.logger.debug(
            `Chain ${chainSymbol}: Cannot process order ${orderTxn.id} because the DEX has been disabled`
          );
          return orderTxn;
        }

        let dataParts = transferMessageString.split(',');
        let targetChain = dataParts[0];

        orderTxn.targetChain = targetChain;
        let isSupportedChain = this.options.chains[targetChain] && targetChain !== chainSymbol;
        if (!isSupportedChain) {
          orderTxn.type = 'invalid';
          orderTxn.reason = 'Invalid target chain';
          this.logger.debug(
            `Chain ${chainSymbol}: Incoming order ${orderTxn.id} has an invalid target chain ${targetChain}`
          );
          return orderTxn;
        }

        if (
          (dataParts[1] === 'limit' || dataParts[1] === 'market') &&
          amount < minOrderAmount
        ) {
          orderTxn.type = 'undersized';
          this.logger.debug(
            `Chain ${chainSymbol}: Incoming order ${orderTxn.id} amount ${orderTxn.sourceChainAmount.toString()} was too small - Minimum order amount is ${minOrderAmount}`
          );
          return orderTxn;
        }

        if (this.tradeEngine.wasOrderProcessed(orderTxn.id, chainSymbol, chainHeight)) {
          orderTxn.type = 'redundant';
          orderTxn.reason = 'Already processed';
          this.logger.debug(
            `Chain ${chainSymbol}: Failed to process order ${orderTxn.id} because it was already processed`
          );
          return orderTxn;
        }

        if (dataParts[1] === 'limit') {
          // E.g. clsk,limit,.5,9205805648791671841L
          let priceString = dataParts[2];
          let price = Number(priceString);
          let targetWalletAddress = dataParts[3];
          if (!this.validPriceRegex.test(priceString) || isNaN(price) || price === 0) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Invalid price';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming limit order ${orderTxn.id} has an invalid price`
            );
            return orderTxn;
          }
          if (!targetWalletAddress) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Invalid wallet address';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming limit order ${orderTxn.id} has an invalid wallet address`
            );
            return orderTxn;
          }
          if (this._isLimitOrderTooSmallToConvert(chainSymbol, amount, price)) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Too small to convert';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming limit order ${orderTxn.id} was too small to cover base blockchain fees`
            );
            return orderTxn;
          }

          orderTxn.type = 'limit';
          orderTxn.height = chainHeight;
          orderTxn.price = price;
          orderTxn.targetWalletAddress = targetWalletAddress;
          if (chainSymbol === this.baseChainSymbol) {
            orderTxn.side = 'bid';
            orderTxn.value = amount;
          } else {
            orderTxn.side = 'ask';
            orderTxn.size = amount;
          }
        } else if (dataParts[1] === 'market') {
          // E.g. clsk,market,9205805648791671841L
          let targetWalletAddress = dataParts[2];
          if (!targetWalletAddress) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Invalid wallet address';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming market order ${orderTxn.id} has an invalid wallet address`
            );
            return orderTxn;
          }
          if (this._isMarketOrderTooSmallToConvert(chainSymbol, amount)) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Too small to convert';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming market order ${orderTxn.id} was too small to cover base blockchain fees`
            );
            return orderTxn;
          }
          orderTxn.type = 'market';
          orderTxn.height = chainHeight;
          orderTxn.targetWalletAddress = targetWalletAddress;
          if (chainSymbol === this.baseChainSymbol) {
            orderTxn.side = 'bid';
            orderTxn.value = amount;
          } else {
            orderTxn.side = 'ask';
            orderTxn.size = amount;
          }
        } else if (dataParts[1] === 'close') {
          // E.g. clsk,close,1787318409505302601
          let targetOrderId = dataParts[2];
          if (!targetOrderId) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Missing order ID';
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming close order ${orderTxn.id} is missing an order ID`
            );
            return orderTxn;
          }

          let targetOrder = this.tradeEngine.getOrder(targetOrderId);
          if (!targetOrder) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Invalid order ID';
            this.logger.debug(
              `Chain ${chainSymbol}: Failed to close order with ID ${targetOrderId} because it could not be found`
            );
            return orderTxn;
          }
          if (targetOrder.sourceChain !== orderTxn.sourceChain) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Wrong chain';
            this.logger.debug(
              `Chain ${chainSymbol}: Could not close order ID ${targetOrderId} because it is on a different chain`
            );
            return orderTxn;
          }
          if (targetOrder.sourceWalletAddress !== orderTxn.sourceWalletAddress) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Not authorized';
            this.logger.debug(
              `Chain ${chainSymbol}: Could not close order ID ${targetOrderId} because it belongs to a different account`
            );
            return orderTxn;
          }
          orderTxn.type = 'close';
          orderTxn.height = chainHeight;
          orderTxn.orderIdToClose = targetOrderId;
        } else {
          orderTxn.type = 'invalid';
          orderTxn.reason = 'Invalid operation';
          this.logger.debug(
            `Chain ${chainSymbol}: Incoming transaction ${orderTxn.id} is not a supported DEX order`
          );
        }
        return orderTxn;
      });

      let closeOrders = orders.filter(orderTxn => orderTxn.type === 'close');

      let limitAndMarketOrders = orders.filter(orderTxn => orderTxn.type === 'limit' || orderTxn.type === 'market');

      let invalidOrders = orders.filter(orderTxn => orderTxn.type === 'invalid');

      let oversizedOrders = orders.filter(orderTxn => orderTxn.type === 'oversized');

      let undersizedOrders = orders.filter(orderTxn => orderTxn.type === 'undersized');

      let movedOrders = orders.filter(orderTxn => orderTxn.type === 'moved');

      let disabledOrders = orders.filter(orderTxn => orderTxn.type === 'disabled');

      if (!this.passiveMode) {
        movedOrders.forEach(async (orderTxn) => {
          let protocolMessage = this._computeProtocolMessage('r5', [orderTxn.id, orderTxn.movedToAddress], 'DEX has moved');
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, protocolMessage, {type: 'r5', originOrderId: orderTxn.id});
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig refund transaction for moved DEX order ID ${
                orderTxn.id
              } to ${
                orderTxn.sourceWalletAddress
              } on chain ${
                orderTxn.sourceChain
              } because of error: ${
                error.message
              }`
            );
          }
        });

        disabledOrders.forEach(async (orderTxn) => {
          let protocolMessage = this._computeProtocolMessage('r6', [orderTxn.id], 'DEX has been disabled');
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, protocolMessage, {type: 'r6', originOrderId: orderTxn.id});
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig refund transaction for disabled DEX order ID ${
                orderTxn.id
              } to ${
                orderTxn.sourceWalletAddress
              } on chain ${
                orderTxn.sourceChain
              } because of error: ${
                error.message
              }`
            );
          }
        });

        invalidOrders.forEach(async (orderTxn) => {
          let reasonMessage = 'Invalid order';
          if (orderTxn.reason) {
            reasonMessage += ` - ${orderTxn.reason}`;
          }
          let protocolMessage = this._computeProtocolMessage('r1', [orderTxn.id], reasonMessage);
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, protocolMessage, {type: 'r1', originOrderId: orderTxn.id});
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig refund transaction for invalid order ID ${
                orderTxn.id
              } to ${
                orderTxn.sourceWalletAddress
              } on chain ${
                orderTxn.sourceChain
              } because of error: ${
                error.message
              }`
            );
          }
        });

        oversizedOrders.forEach(async (orderTxn) => {
          let protocolMessage = this._computeProtocolMessage('r1', [orderTxn.id], 'Oversized order');
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, protocolMessage, {type: 'r1', originOrderId: orderTxn.id});
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig refund transaction for oversized order ID ${
                orderTxn.id
              } to ${
                orderTxn.sourceWalletAddress
              } on chain ${
                orderTxn.sourceChain
              } because of error: ${
                error.message
              }`
            );
          }
        });

        undersizedOrders.forEach(async (orderTxn) => {
          let protocolMessage = this._computeProtocolMessage('r1', [orderTxn.id], 'Undersized order');
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, protocolMessage, {type: 'r1', originOrderId: orderTxn.id});
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig refund transaction for undersized order ID ${
                orderTxn.id
              } to ${
                orderTxn.sourceWalletAddress
              } on chain ${
                orderTxn.sourceChain
              } because of error: ${
                error.message
              }`
            );
          }
        });
      }

      let expiredOrders;
      if (chainSymbol === this.baseChainSymbol) {
        expiredOrders = this.tradeEngine.expireBidOrders(chainHeight);
      } else {
        expiredOrders = this.tradeEngine.expireAskOrders(chainHeight);
      }
      expiredOrders.forEach(async (expiredOrder) => {
        this.logger.info(
          `Chain ${chainSymbol}: Order ${expiredOrder.id} at height ${expiredOrder.height} expired`
        );
        if (this.passiveMode) {
          return;
        }
        let refundTimestamp;
        if (expiredOrder.expiryHeight === chainHeight) {
          refundTimestamp = latestBlockTimestamp;
        } else {
          try {
            let expiryBlock = await this._getBlockAtHeight(chainSymbol, expiredOrder.expiryHeight);
            if (!expiryBlock) {
              throw new Error(
                `No block found at height ${expiredOrder.expiryHeight}`
              );
            }
            refundTimestamp = expiryBlock.timestamp;
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to create multisig refund transaction for expired order ID ${
                expiredOrder.id
              } to ${
                expiredOrder.sourceWalletAddress
              } on chain ${
                expiredOrder.sourceChain
              } because it could not calculate the timestamp due to error: ${
                error.message
              }`
            );
            return;
          }
        }
        let protocolMessage = this._computeProtocolMessage('r2', [expiredOrder.id], 'Expired order');
        try {
          await this.refundOrder(
            expiredOrder,
            refundTimestamp,
            expiredOrder.expiryHeight,
            protocolMessage,
            {type: 'r2', originOrderId: expiredOrder.id}
          );
        } catch (error) {
          this.logger.error(
            `Chain ${chainSymbol}: Failed to post multisig refund transaction for expired order ID ${
              expiredOrder.id
            } to ${
              expiredOrder.sourceWalletAddress
            } on chain ${
              expiredOrder.sourceChain
            } because of error: ${
              error.message
            }`
          );
        }
      });

      closeOrders.forEach(async (orderTxn) => {
        let targetOrder = this.tradeEngine.getOrder(orderTxn.orderIdToClose);
        if (!targetOrder) {
          this.logger.warn(
            `Failed to close order with ID ${orderTxn.orderIdToClose} because it could not be found`
          );
          return;
        }
        let refundTxn = {
          sourceChain: targetOrder.sourceChain,
          sourceWalletAddress: targetOrder.sourceWalletAddress,
          height: orderTxn.height
        };
        if (refundTxn.sourceChain === this.baseChainSymbol) {
          refundTxn.sourceChainAmount = targetOrder.valueRemaining;
        } else {
          refundTxn.sourceChainAmount = targetOrder.sizeRemaining;
        }
        // Also send back any amount which was sent as part of the close order.
        refundTxn.sourceChainAmount += orderTxn.sourceChainAmount;

        let result;
        try {
          result = this.tradeEngine.addCloseOrder(orderTxn);
        } catch (error) {
          this.logger.error(error);
          return;
        }
        if (this.passiveMode) {
          return;
        }
        let protocolMessage = this._computeProtocolMessage('r3', [targetOrder.id, orderTxn.id], 'Closed order');
        try {
          await this.execRefundTransaction(
            refundTxn,
            latestBlockTimestamp,
            protocolMessage,
            {type: 'r3', originOrderId: targetOrder.id, closerOrderId: orderTxn.id}
          );
        } catch (error) {
          this.logger.error(
            `Chain ${chainSymbol}: Failed to post multisig refund transaction for closed order ID ${
              targetOrder.id
            } to ${
              targetOrder.sourceWalletAddress
            } on chain ${
              targetOrder.sourceChain
            } because of error: ${
              error.message
            }`
          );
        }
      });

      limitAndMarketOrders.forEach(async (orderTxn) => {
        let result;
        try {
          result = this.tradeEngine.addOrder(orderTxn);
        } catch (error) {
          this.logger.warn(error);
          return;
        }
        this.logger.info(
          `Chain ${chainSymbol}: Added order ${orderTxn.id} to the trade matching engine`
        );

        if (this.passiveMode) {
          return;
        }

        let takerTargetChain = result.taker.targetChain;
        let takerChainOptions = this.options.chains[takerTargetChain];
        let takerTargetChainModuleAlias = takerChainOptions.moduleAlias;
        let takerAddress = result.taker.targetWalletAddress;
        let takerAmount = takerTargetChain === this.baseChainSymbol ? result.takeValue : result.takeSize;
        let feeCalc = this.bigIntFeeCalculators[takerTargetChain];
        takerAmount -= BigInt(takerChainOptions.exchangeFeeBase);
        takerAmount -= feeCalc.multiplyBigIntByDecimal(takerAmount, takerChainOptions.exchangeFeeRate);

        (async () => {
          if (!result.makers.length) {
            return;
          }
          if (takerAmount <= 0n) {
            this.logger.warn(
              `Chain ${chainSymbol}: Did not post the taker trade order ${orderTxn.id} because the amount after fees was less than or equal to 0`
            );
            return;
          }
          let takerTxn = {
            recipientAddress: takerAddress,
            amount: takerAmount.toString(),
            fee: takerChainOptions.exchangeFeeBase.toString(),
            timestamp: latestBlockTimestamp,
            height: latestChainHeights[takerTargetChain]
          };
          let protocolMessage = this._computeProtocolMessage(
            't1',
            [result.taker.sourceChain, result.taker.id, result.makers.length],
            'Orders taken'
          );
          try {
            await this.execMultisigTransaction(
              takerTargetChain,
              takerTxn,
              protocolMessage,
              {type: 't1', originOrderId: result.taker.id, takerOrderId: result.taker.id, makerCount: result.makers.length}
            );
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig transaction of taker ${takerAddress} on chain ${takerTargetChain} because of error: ${error.message}`
            );
          }
        })();

        (async () => {
          if (orderTxn.type === 'market') {
            let refundTxn = {
              sourceChain: result.taker.sourceChain,
              sourceWalletAddress: result.taker.sourceWalletAddress,
              height: orderTxn.height
            };
            if (result.taker.sourceChain === this.baseChainSymbol) {
              refundTxn.sourceChainAmount = result.taker.valueRemaining;
            } else {
              refundTxn.sourceChainAmount = result.taker.sizeRemaining;
            }
            if (refundTxn.sourceChainAmount <= 0n) {
              return;
            }
            let protocolMessage = this._computeProtocolMessage('r4', [orderTxn.id], 'Unmatched market order part');
            try {
              await this.execRefundTransaction(refundTxn, latestBlockTimestamp, protocolMessage, {type: 'r4', originOrderId: orderTxn.id});
            } catch (error) {
              this.logger.error(
                `Chain ${chainSymbol}: Failed to post multisig market order refund transaction of taker ${takerAddress} on chain ${takerTargetChain} because of error: ${error.message}`
              );
            }
          }
        })();

        result.makers.forEach(async (makerOrder) => {
          let makerChainOptions = this.options.chains[makerOrder.targetChain];
          let makerAddress = makerOrder.targetWalletAddress;
          let makerAmount = makerOrder.targetChain === this.baseChainSymbol ? makerOrder.lastValueTaken : makerOrder.lastSizeTaken;
          let feeCalc = this.bigIntFeeCalculators[makerOrder.targetChain];
          makerAmount -= BigInt(makerChainOptions.exchangeFeeBase);
          makerAmount -= feeCalc.multiplyBigIntByDecimal(makerAmount, makerChainOptions.exchangeFeeRate);

          if (makerAmount <= 0n) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post the maker trade order ${makerOrder.id} because the amount after fees was less than or equal to 0`
            );
            return;
          }
          let makerTxn = {
            recipientAddress: makerAddress,
            amount: makerAmount.toString(),
            fee: makerChainOptions.exchangeFeeBase.toString(),
            timestamp: latestBlockTimestamp,
            height: latestChainHeights[makerOrder.targetChain]
          };
          let protocolMessage = this._computeProtocolMessage(
            't2',
            [makerOrder.sourceChain, makerOrder.id, result.taker.id],
            'Order made'
          );
          try {
            await this.execMultisigTransaction(
              makerOrder.targetChain,
              makerTxn,
              protocolMessage,
              {type: 't2', originOrderId: makerOrder.id, makerOrderId: makerOrder.id, takerOrderId: result.taker.id}
            );
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig transaction of maker ${makerAddress} on chain ${makerOrder.targetChain} because of error: ${error.message}`
            );
          }
        });
      });

      this.processedHeights = {...latestChainHeights};
    }

    let processDividends = async ({chainSymbol, chainHeight, toHeight, latestBlockTimestamp}) => {
      let chainOptions = this.options.chains[chainSymbol];
      let fromHeight = toHeight - chainOptions.dividendHeightInterval;
      let { readMaxBlocks } = chainOptions;
      if (fromHeight < 1) {
        fromHeight = 1;
      }

      let contributionData = {};
      let currentBlock = await this._getBlockAtHeight(chainSymbol, fromHeight);

      while (currentBlock) {
        let blocksToProcess = await this._getBlocksBetweenHeights(chainSymbol, currentBlock.height, toHeight, readMaxBlocks);
        for (let block of blocksToProcess) {
          let outboundTxns = await this._getOutboundTransactionsFromBlock(chainSymbol, chainOptions.walletAddress, block.id);
          outboundTxns.forEach((txn) => {
            let contributionList = this._computeContributions(chainSymbol, txn, chainOptions.exchangeFeeRate);
            contributionList.forEach((contribution) => {
              if (!contributionData[contribution.walletAddress]) {
                contributionData[contribution.walletAddress] = 0n;
              }
              contributionData[contribution.walletAddress] += BigInt(contribution.amount);
            });
          });
        }
        currentBlock = blocksToProcess[blocksToProcess.length - 1];
      }
      let { memberCount } = this.multisigWalletInfo[chainSymbol];
      let dividendList = await this.computeDividends({
        chainSymbol,
        contributionData,
        chainOptions,
        memberCount,
        fromHeight,
        toHeight
      });
      await Promise.all(
        dividendList.map(async (dividend) => {
          let txnAmount = dividend.amount - BigInt(chainOptions.exchangeFeeBase);
          let dividendTxn = {
            recipientAddress: dividend.walletAddress,
            amount: txnAmount.toString(),
            fee: chainOptions.exchangeFeeBase.toString(),
            timestamp: latestBlockTimestamp,
            height: chainHeight
          };
          let protocolMessage = this._computeProtocolMessage('d1', [fromHeight + 1, toHeight], 'Member dividend');
          try {
            await this.execMultisigTransaction(chainSymbol, dividendTxn, protocolMessage);
          } catch (error) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post multisig dividend transaction to member address ${dividend.walletAddress} because of error: ${error.message}`
            );
          }
        })
      );
    };

    let isInForkRecovery = false;

    let processBlockchains = async () => {
      if (lastProcessedTimestamp == null) {
        return 0;
      }
      if (isInForkRecovery) {
        if (this.isForked) {
          return 0;
        }
        if (this.updater.activeUpdate) {
          // If there was a fork in one of the blockchains during a DEX update,
          // revert the update. This will cause the module process to restart,
          // resync from the last safe snapshot and then try to apply the update again.
          this.logger.error(
            `DEX module recovered from a blockchain fork while update ${
              this.updater.activeUpdate.id
            } was in progress - The incomplete update will be reverted and the DEX module will relaunch and try again`
          );
          this.updater.revertActiveUpdate();
          process.exit();
        }
        isInForkRecovery = false;
        this.pendingTransfers.clear();
        lastProcessedTimestamp = await this.revertToLastSnapshot();
      }
      let orderedChainSymbols = [
        this.baseChainSymbol,
        this.quoteChainSymbol
      ];

      let [baseChainLastProcessedHeight, quoteChainLastProcessedHeight] = await Promise.all(
        orderedChainSymbols.map(async (chainSymbol) => {
          try {
            let lastProcessedBlock = await this._getLastBlockAtTimestamp(chainSymbol, lastProcessedTimestamp);
            return lastProcessedBlock.height;
          } catch (error) {
            if (error.sourceError && error.sourceError.name === 'BlockDidNotExistError') {
              return 0;
            }
            throw error;
          }
        })
      );

      let latestProcessedChainHeights = {
        [this.baseChainSymbol]: baseChainLastProcessedHeight,
        [this.quoteChainSymbol]: quoteChainLastProcessedHeight
      };

      let [baseChainBlocks, quoteChainBlocks] = await Promise.all(
        orderedChainSymbols.map(async (chainSymbol) => {
          let chainOptions = this.options.chains[chainSymbol];
          let lastProcessedheight = latestProcessedChainHeights[chainSymbol];
          let maxBlockHeight = await this._getMaxBlockHeight(chainSymbol);
          let maxSafeBlockHeight;

          if (
            chainSymbol === this.baseChainSymbol &&
            this.options.dexDisabledFromHeight != null &&
            maxBlockHeight >= this.options.dexDisabledFromHeight &&
            maxBlockHeight < this.options.dexDisabledFromHeight + this.options.dexDisabledRefundHeightOffset
          ) {
            maxSafeBlockHeight = this.options.dexDisabledFromHeight - 1;
          } else {
            maxSafeBlockHeight = maxBlockHeight - chainOptions.requiredConfirmations;
          }

          let timestampedBlockList = await this._getBlocksBetweenHeights(
            chainSymbol,
            lastProcessedheight,
            maxSafeBlockHeight,
            chainOptions.readMaxBlocks
          );
          return timestampedBlockList
            .filter(block => block.timestamp >= lastProcessedTimestamp)
            .map(block => ({...block, chainSymbol}));
        })
      );

      for (let chainSymbol of orderedChainSymbols) {
        let chainBlockList = chainSymbol === this.baseChainSymbol ? baseChainBlocks : quoteChainBlocks;
        let lastSkippedChainBlock = this.lastSkippedBlocks[chainSymbol];
        if (lastSkippedChainBlock && lastSkippedChainBlock.timestamp >= lastProcessedTimestamp) {
          if (chainBlockList.length) {
            let lastChainBlock = chainBlockList[chainBlockList.length - 1];
            if (lastSkippedChainBlock.timestamp > lastChainBlock.timestamp) {
              chainBlockList.push({
                ...lastSkippedChainBlock,
                chainSymbol,
                isSkipped: true
              });
            }
          } else {
            chainBlockList.push({
              ...lastSkippedChainBlock,
              chainSymbol,
              isSkipped: true
            });
          }
        }
      }

      if (!baseChainBlocks.length || !quoteChainBlocks.length) {
        return 0;
      }

      let lastBaseChainBlock = baseChainBlocks[baseChainBlocks.length - 1];
      let lastQuoteChainBlock = quoteChainBlocks[quoteChainBlocks.length - 1];

      let highestTimestampOfShortestChain = Math.min(lastBaseChainBlock.timestamp, lastQuoteChainBlock.timestamp);
      while (baseChainBlocks.length > 0 && baseChainBlocks[baseChainBlocks.length - 1].timestamp > highestTimestampOfShortestChain) {
        baseChainBlocks.pop();
      }
      while (quoteChainBlocks.length > 0 && quoteChainBlocks[quoteChainBlocks.length - 1].timestamp > highestTimestampOfShortestChain) {
        quoteChainBlocks.pop();
      }

      let orderedBlockList = baseChainBlocks.concat(quoteChainBlocks);

      orderedBlockList.sort((a, b) => {
        let timestampA = a.timestamp;
        let timestampB = b.timestamp;
        if (timestampA < timestampB) {
          return -1;
        }
        if (timestampA > timestampB) {
          return 1;
        }
        if (a.chainSymbol === this.baseChainSymbol) {
          return -1;
        }
        return 1;
      });

      for (let block of orderedBlockList) {
        if (isInForkRecovery) {
          return orderedBlockList.length;
        }
        latestProcessedChainHeights[block.chainSymbol] = block.height;
        try {
          if (!block.isSkipped) {
            await processBlock({
              chainSymbol: block.chainSymbol,
              chainHeight: block.height,
              latestChainHeights: {...latestProcessedChainHeights},
              blockData: {...block}
            });
          }
          if (block.chainSymbol === this.quoteChainSymbol) {
            lastProcessedTimestamp = block.timestamp;
          }
        } catch (error) {
          this.logger.error(
            `Encountered the following error while processing block with id ${
              block.id
            } on chain ${block.chainSymbol} at height ${block.height}: ${error.stack}`
          );
          return orderedBlockList.length;
        }
      }

      if (lastQuoteChainBlock.timestamp > highestTimestampOfShortestChain) {
        lastProcessedTimestamp = highestTimestampOfShortestChain;
      }

      return orderedBlockList.length;
    };

    this._processBlockchains = true;

    let startProcessingBlockchains = async () => {
      while (this._processBlockchains) {
        let blockCount;
        try {
          blockCount = await processBlockchains();
        } catch (error) {
          this.logger.error(
            `Failed to process blockchains because of error: ${error.message}`
          );
          blockCount = 0;
        }
        if (blockCount <= 0) {
          await wait(this.options.readBlocksInterval);
        }
      }
    };

    startProcessingBlockchains();

    let progressingChains = {};

    this.chainSymbols.forEach((chainSymbol) => {
      progressingChains[chainSymbol] = true;
    });

    let areAllChainsProgressing = () => {
      return Object.keys(progressingChains).every((chainSymbol) => progressingChains[chainSymbol]);
    }

    this.chainSymbols.forEach(async (chainSymbol) => {
      let chainOptions = this.options.chains[chainSymbol];
      let chainModuleAlias = chainOptions.moduleAlias;

      // This is to detect forks in the underlying blockchains.

      if (chainOptions.useBlocksChangeChannel) {
        // This approach supports compatibility with older Lisk chain module interface.

        let lastSeenChainHeight = 0;

        channel.subscribe(`${chainModuleAlias}:blocks:change`, async (event) => {
          let chainHeight = parseInt(event.data.height);

          progressingChains[chainSymbol] = chainHeight > lastSeenChainHeight;
          lastSeenChainHeight = chainHeight;

          // If starting without a snapshot, use the timestamp of the first new block.
          if (lastProcessedTimestamp == null) {
            lastProcessedTimestamp = this._normalizeTimestamp(chainSymbol, parseInt(event.data.timestamp));
          }
          if (areAllChainsProgressing()) {
            this.isForked = false;
          } else {
            this.isForked = true;
            isInForkRecovery = true;
          }
        });
      } else {
        // This approach is recommended and is the default.

        channel.subscribe(`${chainModuleAlias}:chainChanges`, async (event) => {
          let type = event.data.type;
          if (type !== 'addBlock' && type !== 'removeBlock' && type !== 'skipBlock') {
            return;
          }

          if (type === 'skipBlock') {
            let skippedBlock = event.data.block;
            this.lastSkippedBlocks[chainSymbol] = {
              timestamp: this._normalizeTimestamp(chainSymbol, parseInt(skippedBlock.timestamp)),
              height: skippedBlock.height
            };
          }

          progressingChains[chainSymbol] = type !== 'removeBlock';

          // If starting without a snapshot, use the timestamp of the first new block.
          if (lastProcessedTimestamp == null) {
            lastProcessedTimestamp = this._normalizeTimestamp(chainSymbol, parseInt(event.data.block.timestamp));
          }

          if (areAllChainsProgressing()) {
            this.isForked = false;
          } else {
            this.isForked = true;
            isInForkRecovery = true;
          }
        });
      }
    });
    channel.publish(`${this.alias}:bootstrap`);
  }

  _computeContributions(chainSymbol, transaction, exchangeFeeRate) {
    transaction = {...transaction};
    if (!transaction.asset) {
      transaction.asset = {};
    }
    if (!transaction.message) {
      return [];
    }
    let txnData = transaction.message;
    // Only trade transactions (e.g. t1 and t2) are counted.
    if (txnData.charAt(0) !== 't') {
      return [];
    }

    let feeCalc = this.bigIntFeeCalculators[chainSymbol];
    let amountBeforeFee = feeCalc.divideBigIntByDecimal(BigInt(transaction.amount), 1 - exchangeFeeRate);
    let memberSignatures = transaction.signatures || [];

    return memberSignatures.map((signaturePacket) => {
      let { signerAddress } = signaturePacket;
      if (!signerAddress) {
        return null;
      }
      return {
        walletAddress: signerAddress,
        amount: amountBeforeFee
      };
    }).filter(dividend => !!dividend);
  }

  _isLimitOrderTooSmallToConvert(chainSymbol, amount, price) {
    if (chainSymbol === this.baseChainSymbol) {
      let quoteChainValue = this.bigIntPriceCalculator.divideBigIntByDecimal(amount, price);
      let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
      return quoteChainValue <= this.chainExchangeFeeBases[this.quoteChainSymbol];
    }
    let baseChainValue = this.bigIntPriceCalculator.multiplyBigIntByDecimal(amount, price);
    let baseChainOptions = this.options.chains[this.baseChainSymbol];
    return baseChainValue <= this.chainExchangeFeeBases[this.baseChainSymbol];
  }

  _isMarketOrderTooSmallToConvert(chainSymbol, amount) {
    if (chainSymbol === this.baseChainSymbol) {
      let { price: quoteChainPrice } = this.tradeEngine.peekAsks() || {};
      let quoteChainValue = this.bigIntPriceCalculator.divideBigIntByDecimal(amount, quoteChainPrice);
      let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
      return quoteChainValue <= this.chainExchangeFeeBases[this.quoteChainSymbol];
    }
    let { price: baseChainPrice } = this.tradeEngine.peekBids() || {};
    let baseChainValue = this.bigIntPriceCalculator.multiplyBigIntByDecimal(amount, baseChainPrice);
    let baseChainOptions = this.options.chains[this.baseChainSymbol];
    return baseChainValue <= this.chainExchangeFeeBases[this.baseChainSymbol];
  }

  _sha1(string) {
    return crypto.createHash('sha1').update(string).digest('hex');
  }

  _transactionComparator(a, b) {
    // The sort order cannot be predicted before the block is forged.
    if (a.sortKey < b.sortKey) {
      return -1;
    }
    if (a.sortKey > b.sortKey) {
      return 1;
    }

    // This should never happen unless there is a hash collision.
    this.logger.error(
      `Failed to compare transactions ${
        a.id
      } and ${
        b.id
      } from block ID ${
        blockId
      } because they had the same sortKey - This may lead to nondeterministic output`
    );
    return 0;
  }

  _normalizeListTimestamps(chainSymbol, objectList) {
    for (let obj of objectList) {
      obj.timestamp = this._normalizeTimestamp(chainSymbol, obj.timestamp);
    }
  }

  _normalizeObjectTimestamp(chainSymbol, obj) {
    obj.timestamp = this._normalizeTimestamp(chainSymbol, obj.timestamp);
  }

  // Normalize a timestamp to make it line up with the other chain.
  _normalizeTimestamp(chainSymbol, timestamp) {
    let transform = this.timestampTransforms[chainSymbol];
    timestamp = Math.round(timestamp * transform.multiplier);
    timestamp += transform.offset;
    return timestamp;
  }

  // Denormalize a timestamp to put it back in its original state.
  _denormalizeTimestamp(chainSymbol, timestamp) {
    let transform = this.timestampTransforms[chainSymbol];
    timestamp -= transform.offset;
    timestamp = Math.round(timestamp / transform.multiplier);
    return timestamp;
  }

  async _getMultisigWalletMembers(chainSymbol, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMultisigWalletMembers`, {walletAddress});
  }

  async _getMinMultisigRequiredSignatures(chainSymbol, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMinMultisigRequiredSignatures`, {walletAddress});
  }

  async _getOutboundTransactions(chainSymbol, walletAddress, fromTimestamp, limit) {
    fromTimestamp = this._denormalizeTimestamp(chainSymbol, fromTimestamp);
    let chainOptions = this.options.chains[chainSymbol];
    let transactions = await this.channel.invoke(`${chainOptions.moduleAlias}:getOutboundTransactions`, {walletAddress, fromTimestamp, limit});
    this._normalizeListTimestamps(chainSymbol, transactions);
    return transactions;
  }

  async _getInboundTransactionsFromBlock(chainSymbol, walletAddress, blockId) {
    let chainOptions = this.options.chains[chainSymbol];
    let txns = await this.channel.invoke(`${chainOptions.moduleAlias}:getInboundTransactionsFromBlock`, {walletAddress, blockId});

    let transactions = txns.map(txn => ({
      ...txn,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));

    this._normalizeListTimestamps(chainSymbol, transactions);
    return transactions;
  }

  async _getOutboundTransactionsFromBlock(chainSymbol, walletAddress, blockId) {
    let chainOptions = this.options.chains[chainSymbol];
    let txns = await this.channel.invoke(`${chainOptions.moduleAlias}:getOutboundTransactionsFromBlock`, {walletAddress, blockId});

    let transactions = txns.map(txn => ({
      ...txn,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));

    this._normalizeListTimestamps(chainSymbol, transactions);
    return transactions;
  }

  async _getLastBlockAtTimestamp(chainSymbol, timestamp) {
    timestamp = this._denormalizeTimestamp(chainSymbol, timestamp);
    let chainOptions = this.options.chains[chainSymbol];
    let block = await this.channel.invoke(`${chainOptions.moduleAlias}:getLastBlockAtTimestamp`, {timestamp});
    this._normalizeObjectTimestamp(chainSymbol, block);
    return block;
  }

  async _getMaxBlockHeight(chainSymbol) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMaxBlockHeight`, {});
  }

  async _getBlocksBetweenHeights(chainSymbol, fromHeight, toHeight, limit) {
    let chainOptions = this.options.chains[chainSymbol];
    let blocks = await this.channel.invoke(`${chainOptions.moduleAlias}:getBlocksBetweenHeights`, {fromHeight, toHeight, limit});
    this._normalizeListTimestamps(chainSymbol, blocks);
    return blocks;
  }

  async _getBlockAtHeight(chainSymbol, height) {
    let chainOptions = this.options.chains[chainSymbol];
    let block = await this.channel.invoke(`${chainOptions.moduleAlias}:getBlockAtHeight`, {height});
    this._normalizeObjectTimestamp(chainSymbol, block);
    return block;
  }

  async _getBaseChainBlockTimestamp(height) {
    let firstBaseChainBlock = await this._getBlockAtHeight(this.baseChainSymbol, height);
    return firstBaseChainBlock.timestamp;
  };

  async refundOrderBook(snapshot, timestamp, movedToAddresses) {
    let allOrders = snapshot.orderBook.bidLimitOrders.concat(snapshot.orderBook.askLimitOrders);
    await Promise.all(
      allOrders.map(async (order) => {
        let movedToAddress = movedToAddresses[order.sourceChain];
        if (movedToAddress) {
          let protocolMessage = this._computeProtocolMessage('r5', [order.id, movedToAddress], 'DEX has moved');
          await this.refundOrder(
            order,
            timestamp,
            snapshot.chainHeights[order.sourceChain],
            protocolMessage,
            {type: 'r5', originOrderId: order.id}
          );
        } else {
          let protocolMessage = this._computeProtocolMessage('r6', [order.id], 'DEX has been disabled');
          allOrders.map(async (order) => {
            await this.refundOrder(
              order,
              timestamp,
              snapshot.chainHeights[order.sourceChain],
              protocolMessage,
              {type: 'r6', originOrderId: order.id}
            );
          })
        }
      })
    );
  }

  async refundOrder(order, timestamp, refundHeight, reason, extraTransferData) {
    let refundTxn = {
      sourceChain: order.sourceChain,
      sourceWalletAddress: order.sourceWalletAddress,
      height: refundHeight
    };
    if (order.sourceChain === this.baseChainSymbol) {
      refundTxn.sourceChainAmount = order.valueRemaining;
    } else {
      refundTxn.sourceChainAmount = order.sizeRemaining;
    }
    await this.execRefundTransaction(refundTxn, timestamp, reason, extraTransferData);
  }

  _computeProtocolMessage(code, args, reasonMessage) {
    let sanitizedArgs = args.map(arg => String(arg).slice(0, this.protocolMaxArgumentLength));
    let messageHeaderParts = [code, ...sanitizedArgs];
    let messageHeader = messageHeaderParts.join(',');
    let messageParts = [messageHeader];
    if (!this.protocolExcludeReason && reasonMessage) {
      messageParts.push(reasonMessage);
    }
    return messageParts.join(': ');
  }

  async execRefundTransaction(txn, timestamp, reason, extraTransferData) {
    let refundChainOptions = this.options.chains[txn.sourceChain];
    let refundAmount = txn.sourceChainAmount - BigInt(refundChainOptions.exchangeFeeBase);

    // Refunds do not charge the exchangeFeeRate.
    if (refundAmount <= 0n) {
      throw new Error(
        'Failed to make refund because amount was less than or equal to 0'
      );
    }

    let refundTxn = {
      recipientAddress: txn.sourceWalletAddress,
      amount: refundAmount.toString(),
      fee: refundChainOptions.exchangeFeeBase.toString(),
      timestamp,
      height: txn.height
    };
    await this.execMultisigTransaction(
      txn.sourceChain,
      refundTxn,
      reason,
      extraTransferData
    );
  }

  // Broadcast the signature to all DEX nodes with a matching baseAddress and quoteAddress
  async _broadcastSignaturesToSubnet(signatureDataList) {
    let actionRouteString = `${this.alias}?baseAddress=${this.baseAddress}&quoteAddress=${this.quoteAddress}`;
    try {
      await this.channel.invoke('network:emit', {
        event: `${actionRouteString}:signatures`,
        data: signatureDataList
      });
    } catch (error) {
      this.logger.error(
        `Error encountered while attempting to broadcast signatures to the network - ${error.message}`
      );
    }
  }

  async execMultisigTransaction(targetChain, transactionData, message, extraTransferData) {
    let chainTimestamp = this._denormalizeTimestamp(targetChain, transactionData.timestamp);
    let chainCrypto = this.chainCrypto[targetChain];
    let {
      transaction: preparedTxn,
      signature: multisigSignaturePacket
    } = await chainCrypto.prepareTransaction({
      recipientAddress: transactionData.recipientAddress,
      amount: transactionData.amount,
      fee: transactionData.fee,
      timestamp: chainTimestamp,
      message
    });

    let processedSignerAddressSet = new Set([multisigSignaturePacket.signerAddress]);
    preparedTxn.signatures.push(multisigSignaturePacket);

    // If the pendingTransfers map already has a transaction with the specified id, delete the existing entry so
    // that when it is re-inserted, it will be added at the end of the queue.
    // To perform expiry using an iterator, it's essential that the insertion order is maintained.
    if (this.pendingTransfers.has(preparedTxn.id)) {
      this.pendingTransfers.delete(preparedTxn.id);
    }
    let transfer = {
      id: preparedTxn.id,
      transaction: preparedTxn,
      recipientAddress: transactionData.recipientAddress,
      targetChain,
      processedSignerAddressSet,
      height: transactionData.height,
      timestamp: Date.now(),
      ...extraTransferData
    };
    this.pendingTransfers.set(preparedTxn.id, transfer);
  }

  _getUpdateSnapshotFilePath(updateId) {
    return path.join(this.options.orderBookUpdateSnapshotDirPath, `snapshot-${updateId}.json`);
  }

  async loadSnapshot() {
    let serializedSafeSnapshot = await readFile(
      this.options.orderBookSnapshotFilePath,
      {encoding: 'utf8'}
    );
    let safeSnapshot = JSON.parse(serializedSafeSnapshot);
    let snapshot;

    if (this.updater.activeUpdate) {
      let updateSnapshotFilePath = this._getUpdateSnapshotFilePath(this.updater.activeUpdate.id);
      let serializedUpdateSnapshot = await readFile(
        updateSnapshotFilePath,
        {encoding: 'utf8'}
      );
      snapshot = JSON.parse(serializedUpdateSnapshot);
    } else {
      snapshot = safeSnapshot;
    }

    snapshot.orderBook.askLimitOrders.forEach((order) => {
      if (order.orderId != null) {
        order.id = order.orderId;
        delete order.orderId;
      }
    });
    snapshot.orderBook.bidLimitOrders.forEach((order) => {
      if (order.orderId != null) {
        order.id = order.orderId;
        delete order.orderId;
      }
    });
    this.lastSnapshot = safeSnapshot;
    this.tradeEngine.setSnapshot(snapshot.orderBook);
    let baseChainHeight = snapshot.chainHeights[this.baseChainSymbol];
    return this._getBaseChainBlockTimestamp(baseChainHeight);
  }

  async revertToLastSnapshot() {
    if (!this.lastSnapshot) {
      this.tradeEngine.clear();
      return;
    }
    this.tradeEngine.setSnapshot(this.lastSnapshot.orderBook);
    let baseChainHeight = this.lastSnapshot.chainHeights[this.baseChainSymbol];
    return this._getBaseChainBlockTimestamp(baseChainHeight);
  }

  async saveSnapshot(snapshot, filePath) {
    if (filePath == null) {
      filePath = this.options.orderBookSnapshotFilePath;
    }
    let baseChainHeight = snapshot.chainHeights[this.baseChainSymbol];
    let serializedSnapshot = JSON.stringify(snapshot);
    await writeFile(filePath, serializedSnapshot);

    try {
      await writeFile(
        path.join(
          this.options.orderBookSnapshotBackupDirPath,
          `snapshot-${baseChainHeight}.json`
        ),
        serializedSnapshot
      );
      let allSnapshots = await readdir(this.options.orderBookSnapshotBackupDirPath);
      let heightRegex = /[0-9]+/g;
      allSnapshots.sort((a, b) => {
        let snapshotHeightA = parseInt(a.match(heightRegex)[0] || 0);
        let snapshotHeightB = parseInt(b.match(heightRegex)[0] || 0);
        if (snapshotHeightA > snapshotHeightB) {
          return -1;
        }
        if (snapshotHeightA < snapshotHeightB) {
          return 1;
        }
        return 0;
      });
      let snapshotsToDelete = allSnapshots.slice(this.options.orderBookSnapshotBackupMaxCount || 200, allSnapshots.length);
      await Promise.all(
        snapshotsToDelete.map(async (fileName) => {
          await unlink(
            path.join(this.options.orderBookSnapshotBackupDirPath, fileName)
          );
        })
      );
    } catch (error) {
      this.logger.error(
        `Failed to backup snapshot in directory ${
          this.options.orderBookSnapshotBackupDirPath
        } because of error: ${
          error.message
        }`
      );
    }
  }

  async unload() {
    this._processBlockchains = false;
    clearInterval(this._multisigExpiryInterval);
    clearInterval(this._multisigFlushInterval);
    clearInterval(this._signatureFlushInterval);
    clearInterval(this._tradeHistoryUpdateInterval);
    await Promise.all(
      this.chainSymbols.map(async (chainSymbol) => {
        return this.chainCrypto[chainSymbol].unload();
      })
    );
  }
};

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
}

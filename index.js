'use strict';

const crypto = require('crypto');
const defaultConfig = require('./defaults/config');
const TradeEngine = require('./trade-engine');
const ChainCrypto = require('./chain-crypto');
const fs = require('fs');
const util = require('util');
const path = require('path');
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const unlink = util.promisify(fs.unlink);
const mkdir = util.promisify(fs.mkdir);
const packageJSON = require('./package.json');

const DEFAULT_MODULE_ALIAS = 'lisk_dex';
const { LISK_DEX_PASSWORD } = process.env;
const CIPHER_ALGORITHM = 'aes-192-cbc';
const CIPHER_KEY = LISK_DEX_PASSWORD ? crypto.scryptSync(LISK_DEX_PASSWORD, 'salt', 24) : undefined;
const CIPHER_IV = Buffer.alloc(16, 0);
const DEFAULT_MULTISIG_READY_DELAY = 5000;

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
        members: {},
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

    if (this.options.priceDecimalPrecision == null) {
      this.validPriceRegex = new RegExp('^([0-9]+\.?|[0-9]*\.[0-9]+)$');
    } else {
      this.validPriceRegex = new RegExp(`^([0-9]+\.?|[0-9]*\.[0-9]{1,${this.options.priceDecimalPrecision}})$`);
    }

    if (this.options.chainsWhitelistPath) {
      let chainsWhitelist = require(path.join(process.cwd(), this.options.chainsWhitelistPath));
      this.chainsWhitelist = new Set([this.baseChainSymbol, this.quoteChainSymbol].concat(chainsWhitelist));
    } else {
      this.chainsWhitelist = new Set([this.baseChainSymbol, this.quoteChainSymbol]);
    }

    this.tradeEngine = new TradeEngine({
      baseCurrency: this.baseChainSymbol,
      quoteCurrency: this.quoteChainSymbol,
      baseOrderHeightExpiry: baseChainOptions.orderHeightExpiry,
      quoteOrderHeightExpiry: quoteChainOptions.orderHeightExpiry
    });
    this.processedHeights = {
      [this.baseChainSymbol]: 0,
      [this.quoteChainSymbol]: 0
    };

    this.chainCrypto = {};

    this.chainSymbols.forEach((chainSymbol) => {
      let chainOptions = this.options.chains[chainSymbol];

      let ChainCryptoClass;
      if (chainOptions.chainCryptoLibPath) {
        ChainCryptoClass = require(chainOptions.chainCryptoLibPath);
      } else {
        ChainCryptoClass = ChainCrypto;
      }

      this.chainCrypto[chainSymbol] = new ChainCryptoClass({
        chainSymbol
      });

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

  _execQueryAgainstIterator(query, sourceIterator, idExtractorFn) {
    query = query || {};
    let {after, before, limit, sort, ...filterMap} = query;
    if (sort && !this.options.apiEnableSorting) {
      let error = new Error('Sorting is disabled');
      error.name = 'InvalidQueryError';
      throw error;
    }
    let filterFields = Object.keys(filterMap);
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
      let isCapturing = false;
      for (let item of iterator) {
        if (isCapturing) {
          let itemMatchesFilter = filterFields.every(
            field => String(item[field]) === String(filterMap[field])
          );
          if (itemMatchesFilter) {
            result.push(item);
          }
        } else if (idExtractorFn(item) === after) {
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
      for (let item of iterator) {
        if (idExtractorFn(item) === before) {
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
            priceDecimalPrecision: this.options.priceDecimalPrecision,
            chainsWhitelist: [...this.chainsWhitelist],
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
          let bidIterator = this.tradeEngine.getBidIterator();
          return this._execQueryAgainstIterator(query, bidIterator, item => item.id);
        }
      },
      getAsks: {
        handler: (action) => {
          let query = {...action.params};
          // Optimization.
          if (query.sort === 'price:asc') {
            delete query.sort;
          }
          let askIterator = this.tradeEngine.getAskIterator();
          return this._execQueryAgainstIterator(query, askIterator, item => item.id);
        }
      },
      getOrders: {
        handler: (action) => {
          let orderIterator = this.tradeEngine.getOrderIterator();
          return this._execQueryAgainstIterator(action.params, orderIterator, item => item.id);
        }
      },
      getPendingTransfers: {
        handler: (action) => {
          let transferList = this._execQueryAgainstIterator(
            action.params,
            this.pendingTransfers.values(),
            item => item.id
          );
          return transferList.map(transfer => ({
            id: transfer.id,
            transaction: transfer.transaction,
            recipientId: transfer.recipientId,
            senderPublicKey: transfer.senderPublicKey,
            targetChain: transfer.targetChain,
            collectedSignatures: [...transfer.processedSignatureSet.values()],
            contributors: [...transfer.contributors],
            timestamp: transfer.timestamp
          }));
        }
      }
    };
  }

  _getChainInfo(chainSymbol) {
    let chainOptions = this.options.chains[chainSymbol];
    let multisigWalletInfo = this.multisigWalletInfo[chainSymbol];
    return {
      walletAddressSystem: chainOptions.walletAddressSystem,
      walletAddress: chainOptions.walletAddress,
      multisigMembers: Object.values(multisigWalletInfo.members),
      multisigRequiredSignatureCount: multisigWalletInfo.requiredSignatureCount,
      minOrderAmount: chainOptions.minOrderAmount,
      exchangeFeeBase: chainOptions.exchangeFeeBase,
      exchangeFeeRate: chainOptions.exchangeFeeRate,
      requiredConfirmations: chainOptions.requiredConfirmations,
      orderHeightExpiry: chainOptions.orderHeightExpiry
    };
  }

  _getSignatureQuota(targetChain, transaction) {
    return transaction.signatures.length - (this.multisigWalletInfo[targetChain] || {}).requiredSignatureCount;
  }

  _verifySignature(targetChain, publicKey, transaction, signatureToVerify) {
    let memberWalletAddress = this.multisigWalletInfo[targetChain].members[publicKey];
    if (!memberWalletAddress) {
      return false;
    }
    return this.chainCrypto[targetChain].verifyTransactionSignature(transaction, signatureToVerify, publicKey);
  }

  _processSignature(signatureData) {
    let transfer = this.pendingTransfers.get(signatureData.transactionId);
    let signature = signatureData.signature;
    let publicKey = signatureData.publicKey;
    if (!transfer) {
      return;
    }
    let {transaction, processedSignatureSet, contributors, targetChain} = transfer;
    if (processedSignatureSet.has(signature)) {
      return;
    }

    let isValidSignature = this._verifySignature(targetChain, publicKey, transaction, signature);
    if (!isValidSignature) {
      return;
    }

    processedSignatureSet.add(signature);
    transaction.signatures.push(signature);

    let memberAddress = this.chainCrypto[targetChain].getAddressFromPublicKey(publicKey);
    contributors.add(memberAddress);

    let signatureQuota = this._getSignatureQuota(targetChain, transaction);
    if (signatureQuota >= 0 && transfer.readyTimestamp == null) {
      transfer.readyTimestamp = Date.now();
    }
  }

  expireMultisigTransactions() {
    let now = Date.now();
    for (let [txnId, txnData] of this.pendingTransfers) {
      if (now - txnData.timestamp < this.options.multisigExpiry) {
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
      for (let signature of transfer.transaction.signatures) {
        signaturesToBroadcast.push({
          signature,
          transactionId: transfer.transaction.id,
          publicKey: transfer.publicKey
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
      let modulePrefix;
      if (chainOptions.moduleAlias === 'chain' || chainOptions.moduleAlias === 'ldem_lisk_chain') {
        modulePrefix = '';
      } else {
        modulePrefix = `${chainOptions.moduleAlias}:`;
      }
      try {
        await this.channel.invoke(
          `network:emit`,
          {
            event: `${modulePrefix}postTransactions`,
            data: {
              transactions,
              nonce: `DEXO2wTkjqplHw2l`
            }
          }
        );
      } catch (error) {
        this.logger.error(
          `Error encountered while attempting to post transactions to the ${targetChain} network - ${error.message}`
        );
      }
    }
  }

  async load(channel) {
    this.channel = channel;

    this._multisigExpiryInterval = setInterval(() => {
      this.expireMultisigTransactions();
    }, this.options.multisigExpiryCheckInterval);

    this._multisigFlushInterval = setInterval(() => {
      this.flushPendingMultisigTransactions();
    }, this.options.multisigFlushInterval);

    this._signatureFlushInterval = setInterval(() => {
      this.flushPendingSignatures();
    }, this.options.signatureFlushInterval);

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
      signatureDataList.forEach(signatureData => this._processSignature(signatureData || {}));
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
          multisigMembers.forEach((member) => {
            this.multisigWalletInfo[chainSymbol].members[member.dependentId] = this.chainCrypto[chainSymbol].getAddressFromPublicKey(member.dependentId);
          });
          this.multisigWalletInfo[chainSymbol].memberCount = multisigMembers.length;
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

    let processBlock = async ({chainSymbol, chainHeight, latestChainHeights, isLastBlock, blockData}) => {
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
      let minOrderAmount = chainOptions.minOrderAmount;

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
        this._getInboundTransactions(chainSymbol, blockData.id, chainOptions.walletAddress),
        this._getOutboundTransactions(chainSymbol, blockData.id, chainOptions.walletAddress)
      ]);

      let [inboundTxns, outboundTxns] = blockTransactions;

      outboundTxns.forEach((txn) => {
        this.pendingTransfers.delete(txn.id);
      });

      let orders = inboundTxns.map((txn) => {
        let orderTxn = {...txn};
        orderTxn.sourceChain = chainSymbol;
        orderTxn.sourceWalletAddress = orderTxn.senderId;
        let amount = parseInt(orderTxn.amount);

        let transferMessageString = orderTxn.message == null ? '' : orderTxn.message;

        if (transferMessageString === 'credit') {
          // The credit operation does nothing - The DEX wallet will simply accept the tokens.
          orderTxn.type = 'credit';
          return orderTxn;
        }

        if (amount > Number.MAX_SAFE_INTEGER) {
          orderTxn.type = 'oversized';
          orderTxn.sourceChainAmount = BigInt(orderTxn.amount);
          this.logger.debug(
            `Chain ${chainSymbol}: Incoming order ${orderTxn.id} amount ${orderTxn.sourceChainAmount.toString()} was too large - Maximum order amount is ${Number.MAX_SAFE_INTEGER}`
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
          let isChainWhitelisted = this.chainsWhitelist.has(targetChain);
          orderTxn.isNonRefundable = isChainWhitelisted;
          if (!isChainWhitelisted) {
            this.logger.debug(
              `Chain ${chainSymbol}: Incoming order ${orderTxn.id} has an invalid target chain ${targetChain}`
            );
          }
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

        if (dataParts[1] === 'limit') {
          // E.g. clsk,limit,.5,9205805648791671841L
          let priceString = dataParts[2];
          let price = Number(priceString);
          let targetWalletAddress = dataParts[3];
          if (!this.validPriceRegex.test(priceString) || isNaN(price)) {
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
            this.logger.error(
              `Chain ${chainSymbol}: Failed to close order with ID ${targetOrderId} because it could not be found`
            );
            return orderTxn;
          }
          if (targetOrder.sourceChain !== orderTxn.sourceChain) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Wrong chain';
            this.logger.error(
              `Chain ${chainSymbol}: Could not close order ID ${targetOrderId} because it is on a different chain`
            );
            return orderTxn;
          }
          if (targetOrder.sourceWalletAddress !== orderTxn.sourceWalletAddress) {
            orderTxn.type = 'invalid';
            orderTxn.reason = 'Not authorized';
            this.logger.error(
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
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, `r5,${orderTxn.id},${orderTxn.movedToAddress}: DEX has moved`);
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
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, `r6,${orderTxn.id}: DEX has been disabled`);
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
          if (orderTxn.isNonRefundable) {
            return;
          }
          let reasonMessage = 'Invalid order';
          if (orderTxn.reason) {
            reasonMessage += ` - ${orderTxn.reason}`;
          }
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, `r1,${orderTxn.id}: ${reasonMessage}`);
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
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, `r1,${orderTxn.id}: Oversized order`);
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
          try {
            await this.execRefundTransaction(orderTxn, latestBlockTimestamp, `r1,${orderTxn.id}: Undersized order`);
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
        try {
          await this.refundOrder(
            expiredOrder,
            refundTimestamp,
            expiredOrder.expiryHeight,
            `r2,${expiredOrder.id}: Expired order`
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
          result = this.tradeEngine.closeOrder(orderTxn.orderIdToClose);
        } catch (error) {
          this.logger.error(error);
          return;
        }
        if (this.passiveMode) {
          return;
        }
        try {
          await this.execRefundTransaction(refundTxn, latestBlockTimestamp, `r3,${targetOrder.id},${orderTxn.id}: Closed order`);
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

        if (result.takeSize <= 0 || this.passiveMode) {
          return;
        }

        let takerTargetChain = result.taker.targetChain;
        let takerChainOptions = this.options.chains[takerTargetChain];
        let takerTargetChainModuleAlias = takerChainOptions.moduleAlias;
        let takerAddress = result.taker.targetWalletAddress;
        let takerAmount = takerTargetChain === this.baseChainSymbol ? result.takeValue : result.takeSize;
        takerAmount -= takerChainOptions.exchangeFeeBase;
        takerAmount -= takerAmount * takerChainOptions.exchangeFeeRate;
        takerAmount = Math.floor(takerAmount);

        if (takerAmount <= 0) {
          this.logger.error(
            `Chain ${chainSymbol}: Failed to post the taker trade order ${orderTxn.id} because the amount after fees was less than or equal to 0`
          );
          return;
        }

        (async () => {
          let takerTxn = {
            amount: takerAmount.toString(),
            recipientId: takerAddress,
            height: latestChainHeights[takerTargetChain],
            timestamp: orderTxn.timestamp + 1
          };
          try {
            await this.execMultisigTransaction(
              takerTargetChain,
              takerTxn,
              `t1,${result.taker.sourceChain},${result.taker.id}: Orders taken`
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
            if (refundTxn.sourceChainAmount <= 0) {
              return;
            }
            try {
              await this.execRefundTransaction(refundTxn, latestBlockTimestamp, `r4,${orderTxn.id}: Unmatched market order part`);
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
          makerAmount -= makerChainOptions.exchangeFeeBase;
          makerAmount -= makerAmount * makerChainOptions.exchangeFeeRate;
          makerAmount = Math.floor(makerAmount);

          if (makerAmount <= 0) {
            this.logger.error(
              `Chain ${chainSymbol}: Failed to post the maker trade order ${makerOrder.id} because the amount after fees was less than or equal to 0`
            );
            return;
          }

          let makerTxn = {
            amount: makerAmount.toString(),
            recipientId: makerAddress,
            height: latestChainHeights[makerOrder.targetChain],
            timestamp: orderTxn.timestamp + 1
          };
          try {
            await this.execMultisigTransaction(
              makerOrder.targetChain,
              makerTxn,
              `t2,${makerOrder.sourceChain},${makerOrder.id},${result.taker.id}: Order made`
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
      let {readMaxBlocks} = chainOptions;
      if (fromHeight < 1) {
        fromHeight = 1;
      }

      let contributionData = {};
      let currentBlock = await this._getBlockAtHeight(chainSymbol, fromHeight);

      while (currentBlock) {
        let blocksToProcess = await this._getBlocksBetweenHeights(chainSymbol, currentBlock.height, toHeight, readMaxBlocks);
        for (let block of blocksToProcess) {
          let outboundTxns = await this._getOutboundTransactions(chainSymbol, block.id, chainOptions.walletAddress);
          outboundTxns.forEach((txn) => {
            let contributionList = this._computeContributions(chainSymbol, txn, chainOptions.exchangeFeeRate, chainOptions.exchangeFeeBase);
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
      let {memberCount} = this.multisigWalletInfo[chainSymbol];
      let dividendList = await this.computeDividends({
        chainSymbol,
        contributionData,
        chainOptions: this.options.chains[chainSymbol],
        memberCount,
        fromHeight,
        toHeight
      });
      await Promise.all(
        dividendList.map(async (dividend) => {
          let txnAmount = dividend.amount - BigInt(chainOptions.exchangeFeeBase);
          let dividendTxn = {
            amount: txnAmount.toString(),
            recipientId: dividend.walletAddress,
            height: chainHeight,
            timestamp: latestBlockTimestamp
          };
          try {
            await this.execMultisigTransaction(
              chainSymbol,
              dividendTxn,
              `d1,${fromHeight + 1},${toHeight}: Member dividend`
            );
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
          let lastProcessedBlock = await this._getLastBlockAtTimestamp(chainSymbol, lastProcessedTimestamp);
          return lastProcessedBlock.height;
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
          return timestampedBlockList.map(block => ({
            ...block,
            chainSymbol
          }));
        })
      );

      if (baseChainBlocks.length <= 0 || quoteChainBlocks.length <= 0) {
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

      if (baseChainBlocks.length > 0) {
        let lastBaseChainBlockToProcess = baseChainBlocks[baseChainBlocks.length - 1];
        lastBaseChainBlockToProcess.isLastBlock = true;
      }
      let isQuoteChainSegmentIncomplete = true;
      if (quoteChainBlocks.length > 0) {
        let lastQuoteChainBlockToProcess = quoteChainBlocks[quoteChainBlocks.length - 1];
        lastQuoteChainBlockToProcess.isLastBlock = true;
        isQuoteChainSegmentIncomplete = (
          lastQuoteChainBlockToProcess.timestamp !== highestTimestampOfShortestChain &&
          lastQuoteChainBlock.timestamp > highestTimestampOfShortestChain
        );
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
          await processBlock({
            chainSymbol: block.chainSymbol,
            chainHeight: block.height,
            latestChainHeights: {...latestProcessedChainHeights},
            isLastBlock: block.isLastBlock,
            blockData: {...block}
          });
          if (block.chainSymbol === this.quoteChainSymbol) {
            lastProcessedTimestamp = block.timestamp;
          }
        } catch (error) {
          this.logger.error(
            `Encountered the following error while processing block id ${block.id} on chain ${block.chainSymbol} at height ${block.height}: ${error.stack}`
          );
          return orderedBlockList.length;
        }
      }
      if (isQuoteChainSegmentIncomplete) {
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

      if (chainOptions.useChainChangesChannel) {
        // This approach is recommended.

        channel.subscribe(`${chainModuleAlias}:chainChanges`, async (event) => {
          let type = event.data.type;
          if (type !== 'addBlock' && type !== 'removeBlock') {
            return;
          }

          progressingChains[chainSymbol] = type === 'addBlock';

          // If starting without a snapshot, use the timestamp of the first new block.
          if (lastProcessedTimestamp == null) {
            lastProcessedTimestamp = parseInt(event.data.block.timestamp);
          }

          if (areAllChainsProgressing()) {
            this.isForked = false;
          } else {
            this.isForked = true;
            isInForkRecovery = true;
          }
        });
      } else {
        // This approach supports compatibility with the current Lisk chain module interface.

        let lastSeenChainHeight = 0;

        channel.subscribe(`${chainModuleAlias}:blocks:change`, async (event) => {
          let chainHeight = parseInt(event.data.height);

          progressingChains[chainSymbol] = chainHeight > lastSeenChainHeight;
          lastSeenChainHeight = chainHeight;

          // If starting without a snapshot, use the timestamp of the first new block.
          if (lastProcessedTimestamp == null) {
            lastProcessedTimestamp = parseInt(event.data.timestamp);
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
    transaction.asset.data = txnData;

    let memberSignatures = transaction.signatures ? transaction.signatures.split(',') : [];
    let amountBeforeFee = Math.floor(transaction.amount / (1 - exchangeFeeRate));

    return memberSignatures.map((signature) => {
      let walletAddress = this._getMemberWalletAddress(chainSymbol, transaction, signature);
      if (!walletAddress) {
        return null;
      }
      return {
        walletAddress,
        amount: amountBeforeFee
      };
    }).filter(dividend => !!dividend);
  }

  _getMemberWalletAddress(chainSymbol, transaction, signature) {
    let memberPublicKey = Object.keys(this.multisigWalletInfo[chainSymbol].members).find((publicKey) => {
      return this._verifySignature(chainSymbol, publicKey, transaction, signature);
    });
    if (!memberPublicKey) {
      return null;
    }
    return this.chainCrypto[chainSymbol].getAddressFromPublicKey(memberPublicKey);
  }

  _isLimitOrderTooSmallToConvert(chainSymbol, amount, price) {
    if (chainSymbol === this.baseChainSymbol) {
      let quoteChainValue = Math.floor(amount / price);
      let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
      return quoteChainValue <= quoteChainOptions.exchangeFeeBase;
    }
    let baseChainValue = Math.floor(amount * price);
    let baseChainOptions = this.options.chains[this.baseChainSymbol];
    return baseChainValue <= baseChainOptions.exchangeFeeBase;
  }

  _isMarketOrderTooSmallToConvert(chainSymbol, amount) {
    if (chainSymbol === this.baseChainSymbol) {
      let {price: quoteChainPrice} = this.tradeEngine.peekAsks() || {};
      let quoteChainValue = Math.floor(amount / quoteChainPrice);
      let quoteChainOptions = this.options.chains[this.quoteChainSymbol];
      return quoteChainValue <= quoteChainOptions.exchangeFeeBase;
    }
    let {price: baseChainPrice} = this.tradeEngine.peekBids() || {};
    let baseChainValue = Math.floor(amount * baseChainPrice);
    let baseChainOptions = this.options.chains[this.baseChainSymbol];
    return baseChainValue <= baseChainOptions.exchangeFeeBase;
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

    // This should never happen unless there is an md5 hash collision.
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

  async _getMultisigWalletMembers(chainSymbol, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMultisigWalletMembers`, {walletAddress});
  }

  async _getMinMultisigRequiredSignatures(chainSymbol, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMinMultisigRequiredSignatures`, {walletAddress});
  }

  async _getInboundTransactions(chainSymbol, blockId, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    let txns = await this.channel.invoke(`${chainOptions.moduleAlias}:getInboundTransactions`, {walletAddress, blockId});

    return txns.map(txn => ({
      ...txn,
      senderPublicKey: txn.senderPublicKey,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));
  }

  async _getOutboundTransactions(chainSymbol, blockId, walletAddress) {
    let chainOptions = this.options.chains[chainSymbol];
    let txns = await this.channel.invoke(`${chainOptions.moduleAlias}:getOutboundTransactions`, {walletAddress, blockId});

    return txns.map(txn => ({
      ...txn,
      senderPublicKey: txn.senderPublicKey,
      sortKey: this._sha1(txn.id + blockId)
    })).sort((a, b) => this._transactionComparator(a, b));
  }

  async _getLastBlockAtTimestamp(chainSymbol, timestamp) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getLastBlockAtTimestamp`, {timestamp});
  }

  async _getMaxBlockHeight(chainSymbol) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getMaxBlockHeight`, {});
  }

  async _getBlocksBetweenHeights(chainSymbol, fromHeight, toHeight, limit) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getBlocksBetweenHeights`, {fromHeight, toHeight, limit});
  }

  async _getBlockAtHeight(chainSymbol, height) {
    let chainOptions = this.options.chains[chainSymbol];
    return this.channel.invoke(`${chainOptions.moduleAlias}:getBlockAtHeight`, {height});
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
          await this.refundOrder(
            order,
            timestamp,
            snapshot.chainHeights[order.sourceChain],
            `r5,${order.id},${movedToAddress}: DEX has moved`
          );
        } else {
          allOrders.map(async (order) => {
            await this.refundOrder(
              order,
              timestamp,
              snapshot.chainHeights[order.sourceChain],
              `r6,${order.id}: DEX has been disabled`
            );
          })
        }
      })
    );
  }

  async refundOrder(order, timestamp, refundHeight, reason) {
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
    await this.execRefundTransaction(refundTxn, timestamp, reason);
  }

  async execRefundTransaction(txn, timestamp, reason) {
    let refundChainOptions = this.options.chains[txn.sourceChain];
    let flooredAmount = Math.floor(txn.sourceChainAmount);
    let refundAmount = BigInt(flooredAmount) - BigInt(refundChainOptions.exchangeFeeBase);
    // Refunds do not charge the exchangeFeeRate.

    if (refundAmount <= 0n) {
      throw new Error(
        'Failed to make refund because amount was less than or equal to 0'
      );
    }

    let refundTxn = {
      amount: refundAmount.toString(),
      recipientId: txn.sourceWalletAddress,
      height: txn.height,
      timestamp
    };
    await this.execMultisigTransaction(
      txn.sourceChain,
      refundTxn,
      reason
    );
  }

  // Broadcast the signature to all DEX nodes with a matching baseAddress and quoteAddress
  async _broadcastSignaturesToSubnet(signatureDataList) {
    let actionRouteString = `${this.alias}?baseAddress=${this.baseAddress}&quoteAddress=${this.quoteAddress}`;
    this.channel.invoke('network:emit', {
      event: `${actionRouteString}:signatures`,
      data: signatureDataList
    });
  }

  async execMultisigTransaction(targetChain, transactionData, message) {
    let chainOptions = this.options.chains[targetChain];
    let chainCrypto = this.chainCrypto[targetChain];
    let {transaction: preparedTxn, signature: multisigTxnSignature} = chainCrypto.prepareTransaction(
      {
        ...transactionData,
        message
      },
      chainOptions
    );

    let publicKey = chainCrypto.getPublicKeyFromPassphrase(chainOptions.passphrase);
    let walletAddress = chainCrypto.getAddressFromPublicKey(publicKey);

    let processedSignatureSet = new Set([multisigTxnSignature]);

    let contributors = new Set();
    contributors.add(walletAddress);

    // If the pendingTransfers map already has a transaction with the specified id, delete the existing entry so
    // that when it is re-inserted, it will be added at the end of the queue.
    // To perform expiry using an iterator, it's essential that the insertion order is maintained.
    if (this.pendingTransfers.has(preparedTxn.id)) {
      this.pendingTransfers.delete(preparedTxn.id);
    }
    this.pendingTransfers.set(preparedTxn.id, {
      id: preparedTxn.id,
      transaction: preparedTxn,
      recipientId: preparedTxn.recipientId,
      senderPublicKey: preparedTxn.senderPublicKey,
      targetChain,
      processedSignatureSet,
      contributors,
      publicKey,
      height: transactionData.height,
      timestamp: Date.now()
    });
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
      if (order.value == null) {
        order.value = order.size * order.price;
        order.valueRemaining = order.sizeRemaining * order.price;
        delete order.size;
        delete order.sizeRemaining;
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
  }
};

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
}

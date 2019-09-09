'use strict';

const defaultConfig = require('./defaults/config');
const BaseModule = require('lisk-framework/src/modules/base_module');
const { createStorageComponent } = require('lisk-framework/src/components/storage');
const { createLoggerComponent } = require('lisk-framework/src/components/logger');
const TradeEngine = require('./trade-engine');
const liskTransactions = require('@liskhq/lisk-transactions');
const liskCryptography = require('@liskhq/lisk-cryptography');
const fs = require('fs');
const util = require('util');
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);

const WritableConsumableStream = require('writable-consumable-stream');

const MODULE_ALIAS = 'lisk_dex';

/**
 * Lisk DEX module specification
 *
 * @namespace Framework.Modules
 * @type {module.LiskDEXModule}
 */
module.exports = class LiskDEXModule extends BaseModule {
  constructor(options) {
    super({...defaultConfig, ...options});
    this.chainSymbols = Object.keys(this.options.chains);
    if (this.chainSymbols.length !== 2) {
      throw new Error('The DEX module must operate on exactly 2 chains only');
    }
    this.progressingChains = {};
    this.currentProcessedHeights = {};
    this.lastSnapshotHeights = {};
    this.chainSymbols.forEach((chainSymbol) => {
      this.currentProcessedHeights[chainSymbol] = 0;
      this.lastSnapshotHeights[chainSymbol] = 0;
      this.progressingChains[chainSymbol] = true;
    });

    this.baseChainSymbol = this.options.baseChain;
    this.quoteChainSymbol = this.chainSymbols.find(chain => chain !== this.baseChainSymbol);
    this.tradeEngine = new TradeEngine({
      baseCurrency: this.baseChainSymbol,
      quoteCurrency: this.quoteChainSymbol
    });
  }

  static get alias() {
    return MODULE_ALIAS;
  }

  static get info() {
    return {
      author: 'Jonathan Gros-Dubois',
      version: '1.0.0',
      name: MODULE_ALIAS,
    };
  }

  static get migrations() {
    return [];
  }

  static get defaults() {
    return defaultConfig;
  }

  get events() {
    return [
      'bootstrap',
    ];
  }

  get actions() {
    // TODO: Expose actions for HTTP API.
    return {};
  }

  async load(channel) {
    let loggerConfig = await channel.invoke(
      'app:getComponentConfig',
      'logger',
    );
    this.logger = createLoggerComponent(loggerConfig);
    try {
      await this.loadSnapshot();
    } catch (error) {
      this.logger.error(
        `Failed to load initial snapshot because of error: ${error.message} - DEX node will start with an empty order book.`
      );
    }

    let storageConfigOptions = await channel.invoke(
      'app:getComponentConfig',
      'storage',
    );
    this.chainSymbols.forEach(async (chainSymbol) => {
      let chainOptions = this.options.chains[chainSymbol];
      let chainModuleAlias = chainOptions.moduleAlias;
      let storageConfig = {
        ...storageConfigOptions,
        database: chainOptions.database,
      };
      let storage = createStorageComponent(storageConfig, this.logger);
      await storage.bootstrap();

      let blockProcessingStream = new WritableConsumableStream();

      (async () => {
        for await (let event of blockProcessingStream) {
          let processedHeight = this.currentProcessedHeights[chainSymbol] || event.chainHeight;
          while (processedHeight <= event.chainHeight) {
            let targetHeight = processedHeight - this.options.requiredConfirmations;

            let finishProcessing = async () => {
              this.currentProcessedHeights[chainSymbol]++;
              processedHeight = this.currentProcessedHeights[chainSymbol];
              if (chainSymbol === this.baseChainSymbol) {
                let lastSnapshotHeight = this.lastSnapshotHeights[chainSymbol];
                if (targetHeight > lastSnapshotHeight + this.options.orderBookSnapshotFinality) {
                  try {
                    await this.saveSnapshot();
                  } catch (error) {
                    this.logger.error(`Failed to save snapshot because of error: ${error.message}`);
                  }
                }
              }
            };

            this.logger.trace(
              `Processing block at height ${targetHeight}`
            );

            let blockData = (
              await storage.adapter.db.query(
                'select blocks.id, blocks."numberOfTransactions", blocks.timestamp from blocks where height = $1',
                [targetHeight],
              )
            )[0];

            if (!blockData) {
              this.logger.error(
                `Failed to fetch block at height ${targetHeight}`
              );

              await finishProcessing();
              continue;
            }
            if (!blockData.numberOfTransactions) {
              this.logger.trace(
                `No transactions in block ${blockData.id} at height ${targetHeight}`
              );

              await finishProcessing();
              continue;
            }
            let latestBlockTimestamp = blockData.timestamp;

            let orders = await storage.adapter.db.query(
              'select trs.id, trs."senderId", trs."timestamp", trs."recipientId", trs."amount", trs."transferData" from trs where trs."blockId" = $1 and trs."transferData" is not null and trs."recipientId" = $2',
              [blockData.id, chainOptions.walletAddress],
            )
            .map((txn) => {
              let transferDataString = txn.transferData.toString('utf8');
              let dataParts = transferDataString.split(',');

              let orderTxn = {...txn};
              let amount = parseInt(orderTxn.amount);
              if (amount > Number.MAX_SAFE_INTEGER) {
                orderTxn.type = 'invalid';
                orderTxn.targetChain = targetChain;
                this.logger.debug(
                  `Incoming order ${orderTxn.id} amount ${amount} was too large - Maximum order amount is ${Number.MAX_SAFE_INTEGER}`
                );
                return orderTxn;
              }

              orderTxn.sourceChain = chainSymbol;
              orderTxn.sourceChainAmount = amount; // TODO: Consider switching to BigInt.
              orderTxn.sourceWalletAddress = orderTxn.senderId;

              let targetChain = dataParts[0];
              let isSupportedChain = this.options.chains[targetChain] && targetChain !== chainSymbol;
              if (!isSupportedChain) {
                orderTxn.type = 'invalid';
                orderTxn.targetChain = targetChain;
                this.logger.debug(
                  `Incoming order ${orderTxn.id} has an invalid target chain ${targetChain}`
                );
                return orderTxn;
              }

              if (dataParts[1] === 'limit') {
                // E.g. clsk,limit,.5,9205805648791671841L
                orderTxn.type = 'limit';
                orderTxn.height = targetHeight;
                orderTxn.price = parseFloat(dataParts[2]);
                orderTxn.targetChain = targetChain;
                orderTxn.targetWalletAddress = dataParts[3];
                if (chainSymbol === this.baseChainSymbol) {
                  orderTxn.side = 'bid';
                  orderTxn.size = Math.floor(amount / orderTxn.price); // TODO: Consider switching to BigInt.
                } else {
                  orderTxn.side = 'ask';
                  orderTxn.size = amount; // TODO: Consider switching to BigInt.
                }
              } else if (dataParts[1] === 'market') {
                // E.g. clsk,market,9205805648791671841L
                orderTxn.type = 'market';
                orderTxn.height = targetHeight;
                orderTxn.targetChain = targetChain;
                orderTxn.targetWalletAddress = dataParts[2];
                if (chainSymbol === this.baseChainSymbol) {
                  orderTxn.side = 'bid';
                  orderTxn.size = -1;
                  orderTxn.funds = amount; // TODO: Consider switching to BigInt.
                } else {
                  orderTxn.side = 'ask';
                  orderTxn.size = amount; // TODO: Consider switching to BigInt.
                  orderTxn.funds = -1; // TODO: Consider switching to BigInt.
                }
              } else if (dataParts[1] === 'cancel') {
                // E.g. clsk,cancel,1787318409505302601
                orderTxn.type = 'cancel';
                orderTxn.height = targetHeight;
                orderTxn.orderIdToCancel = dataParts[2];
              } else {
                orderTxn.type = 'invalid';
                this.logger.debug(
                  `Incoming transaction ${txn.id} is not a supported DEX order`
                );
              }
              return orderTxn;
            });

            let cancelOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'cancel';
            });

            let limitAndMarketOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'limit' || orderTxn.type === 'market';
            });

            let invalidOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'invalid';
            });

            await Promise.all(
              invalidOrders.map(async (orderTxn) => {
                try {
                  await this.makeRefundTransaction(orderTxn, latestBlockTimestamp);
                } catch (error) {
                  this.logger.error(
                    `Failed to post multisig refund transaction for invalid order ID ${
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
              })
            );

            let heightExpiryThreshold = targetHeight - this.options.orderHeightExpiry;
            if (heightExpiryThreshold > 0) {
              let expiredOrders = this.tradeEngine.expireOrders(heightExpiryThreshold);
              expiredOrders.forEach(async (expiredOrder) => {
                try {
                  await this.makeRefundTransaction(expiredOrder, latestBlockTimestamp);
                } catch (error) {
                  this.logger.error(
                    `Failed to post multisig refund transaction for expired order ID ${
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
                this.logger.trace(
                  `Order ${expiredOrder.id} at height ${expiredOrder.height} expired`
                );
              });
            }

            await Promise.all(
              cancelOrders.map(async (orderTxn) => {
                let targetOrder = this.tradeEngine.getOrder(orderTxn.orderIdToCancel);
                if (!targetOrder) {
                  this.logger.error(
                    `Failed to cancel order with ID ${orderTxn.orderIdToCancel} because it could not be found`
                  );
                  return;
                }
                if (targetOrder.sourceWalletAddress !== orderTxn.sourceWalletAddress) {
                  this.logger.error(
                    `Could not cancel order ID ${orderTxn.orderIdToCancel} because it belongs to a different account`
                  );
                  return;
                }
                targetOrder = {...targetOrder};
                // Also send back any amount which was sent as part of the cancel order.
                targetOrder.sourceChainAmount += orderTxn.sourceChainAmount;

                let result;
                try {
                  result = this.tradeEngine.cancelOrder(orderTxn.orderIdToCancel);
                } catch (error) {
                  this.logger.error(error);
                  return;
                }
                try {
                  await this.makeRefundTransaction(targetOrder);
                } catch (error) {
                  this.logger.error(
                    `Failed to post multisig refund transaction for canceled order ID ${
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
              })
            );

            await Promise.all(
              limitAndMarketOrders.map(async (orderTxn) => {
                let result;
                try {
                  result = this.tradeEngine.addOrder(orderTxn);
                } catch (error) {
                  this.logger.error(error);
                  return;
                }

                if (result.takeSize > 0) {
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
                      `Failed to take the trade order ${orderTxn.id} because the amount after fees was less than or equal to 0`
                    );
                    return;
                  }

                  (async () => {
                    let takerTxn = {
                      amount: takerAmount.toString(),
                      recipientId: takerAddress,
                      timestamp: orderTxn.timestamp
                    };
                    try {
                      await this.makeMultiSigTransaction(
                        takerTargetChain,
                        takerTxn
                      );
                    } catch (error) {
                      this.logger.error(
                        `Failed to post multisig transaction of taker ${takerAddress} on chain ${takerTargetChain} because of error: ${error.message}`
                      );
                    }
                  })();

                  await Promise.all(
                    result.makers.map(async (makerOrder) => {
                      let makerChainOptions = this.options.chains[makerOrder.targetChain];
                      let makerTargetChainModuleAlias = makerChainOptions.moduleAlias;
                      let makerAddress = makerOrder.targetWalletAddress;
                      let makerAmount = makerOrder.targetChain === this.baseChainSymbol ?
                      makerOrder.valueTaken : makerOrder.sizeTaken;
                      makerAmount -= makerChainOptions.exchangeFeeBase;
                      makerAmount -= makerAmount * makerChainOptions.exchangeFeeRate;
                      makerAmount = Math.floor(makerAmount);

                      if (makerAmount <= 0) {
                        this.logger.error(
                          `Failed to make the trade order ${makerOrder.id} because the amount after fees was less than or equal to 0`
                        );
                        return;
                      }

                      (async () => {
                        let makerTxn = {
                          amount: makerAmount.toString(),
                          recipientId: makerAddress,
                          timestamp: orderTxn.timestamp
                        };
                        try {
                          await this.makeMultiSigTransaction(
                            makerOrder.targetChain,
                            makerTxn
                          );
                        } catch (error) {
                          this.logger.error(
                            `Failed to post multisig transaction of maker ${makerAddress} on chain ${makerOrder.targetChain} because of error: ${error.message}`
                          );
                        }
                      })();
                    })
                  );
                }
              })
            );

            await finishProcessing();
          }
        }
      })();

      let lastSeenChainHeight = 0;

      channel.subscribe(`${chainModuleAlias}:blocks:change`, (event) => {
        let chainHeight = parseInt(event.data.height);

        let wereAllChainsProgressing = this.areAllChainsProgressing();

        if (chainHeight > lastSeenChainHeight) {
          this.progressingChains[chainSymbol] = true;
        } else {
          this.progressingChains[chainSymbol] = false;
        }
        lastSeenChainHeight = chainHeight;

        if (!this.currentProcessedHeights[chainSymbol]) {
          this.currentProcessedHeights[chainSymbol] = chainHeight;
        }

        let areAllChainsProgressing = this.areAllChainsProgressing();

        if (wereAllChainsProgressing && !areAllChainsProgressing) {
          this.revertToLastSnapshot();
        }

        if (areAllChainsProgressing) {
          blockProcessingStream.write({chainHeight});
        }
      });
    });
    channel.publish(`${MODULE_ALIAS}:bootstrap`);
  }

  async makeRefundTransaction(orderTxn, timestamp) {
    let refundChainOptions = this.options.chains[orderTxn.sourceChain];
    let refundAmount = orderTxn.sourceChainAmount;
    refundAmount -= refundChainOptions.exchangeFeeBase;
    // Refunds do not charge the exchangeFeeRate.
    refundAmount = Math.floor(refundAmount);

    let refundTxn = {
      amount: refundAmount.toString(),
      recipientId: orderTxn.sourceWalletAddress,
      timestamp: timestamp == null ? orderTxn.timestamp : timestamp
    };
    await this.makeMultiSigTransaction(
      orderTxn.sourceChain,
      refundTxn
    );
  }

  async makeMultiSigTransaction(targetChain, transactionData) {
    let chainOptions = this.options.chains[targetChain];
    let targetModuleAlias = chainOptions.moduleAlias;
    let txn = {
      type: 0,
      amount: transactionData.amount.toString(),
      recipientId: transactionData.recipientId,
      fee: liskTransactions.constants.TRANSFER_FEE.toString(),
      asset: {},
      timestamp: transactionData.timestamp,
      senderPublicKey: liskCryptography.getAddressAndPublicKeyFromPassphrase(chainOptions.sharedPassphrase).publicKey
    };
    let signedTxn = liskTransactions.utils.prepareTransaction(txn, chainOptions.sharedPassphrase);
    let multiSigTxnSignature = liskTransactions.utils.multiSignTransaction(signedTxn, chainOptions.passphrase);
    let publicKey = liskCryptography.getAddressAndPublicKeyFromPassphrase(chainOptions.passphrase).publicKey;

    await channel.invoke(`${targetModuleAlias}:postTransaction`, { transaction: signedTxn });
    await wait(this.options.signatureBroadcastDelay);
    await channel.invoke(`${targetModuleAlias}:postSignature`, {
      signature: {
        transactionId: signedTxn.id,
        publicKey: publicKey,
        signature: multiSigTxnSignature
      }
    });
  }

  async loadSnapshot() {
    let serializedSnapshot = await readFile(this.options.orderBookSnapshotFilePath, {encoding: 'utf8'});
    let snapshot = JSON.parse(serializedSnapshot);
    this.lastSnapshot = snapshot;
    this.tradeEngine.setSnapshot(snapshot.orderBook);
    this.lastSnapshotHeights = snapshot.chainHeights;
    this.currentProcessedHeights = {};
    Object.keys(this.lastSnapshotHeights).forEach((chainSymbol) => {
      this.currentProcessedHeights[chainSymbol] = this.lastSnapshotHeights[chainSymbol] + this.options.requiredConfirmations;
    });
  }

  async revertToLastSnapshot() {
    this.tradeEngine.setSnapshot(this.lastSnapshot.orderBook);
    this.lastSnapshotHeights = this.lastSnapshot.chainHeights;
    this.currentProcessedHeights = {};
    Object.keys(this.lastSnapshotHeights).forEach((chainSymbol) => {
      this.currentProcessedHeights[chainSymbol] = this.lastSnapshotHeights[chainSymbol] + this.options.requiredConfirmations;
    });
  }

  async saveSnapshot() {
    let snapshot = {};
    snapshot.orderBook = this.tradeEngine.getSnapshot();
    snapshot.chainHeights = {};
    Object.keys(this.currentProcessedHeights).forEach((chainSymbol) => {
      let targetHeight = this.currentProcessedHeights[chainSymbol] - this.options.requiredConfirmations;
      if (targetHeight < 0) {
        targetHeight = 0;
      }
      snapshot.chainHeights[chainSymbol] = targetHeight;
    });
    this.lastSnapshot = snapshot;
    let serializedSnapshot = JSON.stringify(snapshot);
    await writeFile(this.options.orderBookSnapshotFilePath, serializedSnapshot);
    this.lastSnapshotHeights = snapshot.chainHeights;
  }

  areAllChainsProgressing() {
    return Object.keys(this.progressingChains).every((chainSymbol) => this.progressingChains[chainSymbol]);
  }

  async unload() {
  }
};

function wait(duration) {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
}

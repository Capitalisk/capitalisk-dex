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
    this.channel = channel;

    let loggerConfig = await channel.invoke(
      'app:getComponentConfig',
      'logger',
    );
    this.logger = createLoggerComponent({...loggerConfig, ...this.options.logger});
    try {
      await this.loadSnapshot();
    } catch (error) {
      this.logger.error(
        `Failed to load initial snapshot because of error: ${error.message} - DEX node will start with an empty order book`
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
          if (!this.currentProcessedHeights[chainSymbol]) {
            this.currentProcessedHeights[chainSymbol] = event.chainHeight;
          }
          while (this.currentProcessedHeights[chainSymbol] <= event.chainHeight) {
            let isOnTopHeight = this.currentProcessedHeights[chainSymbol] === event.chainHeight;
            let targetHeight = this.currentProcessedHeights[chainSymbol] - this.options.requiredConfirmations;

            let finishProcessing = async () => {
              this.currentProcessedHeights[chainSymbol]++;
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
              `Chain ${chainSymbol}: Processing block at height ${targetHeight}`
            );

            let blockData = (
              await storage.adapter.db.query(
                'select blocks.id, blocks."numberOfTransactions", blocks.timestamp from blocks where height = $1',
                [targetHeight],
              )
            )[0];

            if (!blockData) {
              this.logger.error(
                `Chain ${chainSymbol}: Failed to fetch block at height ${targetHeight}`
              );

              await finishProcessing();
              continue;
            }
            if (!blockData.numberOfTransactions) {
              this.logger.trace(
                `Chain ${chainSymbol}: No transactions in block ${blockData.id} at height ${targetHeight}`
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
              let orderTxn = {...txn};
              orderTxn.orderId = orderTxn.id;
              orderTxn.sourceChain = chainSymbol;
              orderTxn.sourceWalletAddress = orderTxn.senderId;
              let amount = parseInt(orderTxn.amount);

              if (amount > Number.MAX_SAFE_INTEGER) {
                orderTxn.type = 'oversized';
                orderTxn.sourceChainAmount = BigInt(orderTxn.amount);
                this.logger.debug(
                  `Chain ${chainSymbol}: Incoming order ${orderTxn.orderId} amount ${orderTxn.sourceChainAmount.toString()} was too large - Maximum order amount is ${Number.MAX_SAFE_INTEGER}`
                );
                return orderTxn;
              }

              orderTxn.sourceChainAmount = amount;

              if (isOnTopHeight) {
                if (this.options.dexMovedToAddress) {
                  orderTxn.type = 'moved';
                  orderTxn.movedToAddress = this.options.dexMovedToAddress;
                  this.logger.debug(
                    `Chain ${chainSymbol}: Cannot process order ${orderTxn.orderId} because the DEX has moved to the address ${this.options.dexMovedToAddress}`
                  );
                  return orderTxn;
                }
                if (this.options.dexDisabled) {
                  orderTxn.type = 'disabled';
                  this.logger.debug(
                    `Chain ${chainSymbol}: Cannot process order ${orderTxn.orderId} because the DEX has been disabled`
                  );
                  return orderTxn;
                }
              }

              let transferDataString = txn.transferData.toString('utf8');
              let dataParts = transferDataString.split(',');

              let targetChain = dataParts[0];
              let isSupportedChain = this.options.chains[targetChain] && targetChain !== chainSymbol;
              if (!isSupportedChain) {
                orderTxn.type = 'invalid';
                orderTxn.targetChain = targetChain;
                this.logger.debug(
                  `Chain ${chainSymbol}: Incoming order ${orderTxn.orderId} has an invalid target chain ${targetChain}`
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
                  orderTxn.size = Math.floor(amount / orderTxn.price);
                } else {
                  orderTxn.side = 'ask';
                  orderTxn.size = amount;
                }
              } else if (dataParts[1] === 'market') {
                // E.g. clsk,market,9205805648791671841L
                orderTxn.type = 'market';
                orderTxn.height = targetHeight;
                orderTxn.targetChain = targetChain;
                orderTxn.targetWalletAddress = dataParts[2];
                if (chainSymbol === this.baseChainSymbol) {
                  orderTxn.side = 'bid';
                  orderTxn.size = 0;
                  orderTxn.funds = amount;
                } else {
                  orderTxn.side = 'ask';
                  orderTxn.size = amount;
                  orderTxn.funds = 0;
                }
              } else if (dataParts[1] === 'cancel') {
                // E.g. clsk,cancel,1787318409505302601
                orderTxn.type = 'cancel';
                orderTxn.height = targetHeight;
                orderTxn.orderIdToCancel = dataParts[2];
              } else {
                orderTxn.type = 'invalid';
                this.logger.debug(
                  `Chain ${chainSymbol}: Incoming transaction ${orderTxn.orderId} is not a supported DEX order`
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

            let oversizedOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'oversized';
            });

            let movedOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'moved';
            });

            let disabledOrders = orders.filter((orderTxn) => {
              return orderTxn.type === 'disabled';
            });

            await Promise.all(
              movedOrders.map(async (orderTxn) => {
                try {
                  await this.makeRefundTransaction(orderTxn, latestBlockTimestamp, `m1,${orderTxn.movedToAddress}: DEX has moved`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for moved DEX order ID ${
                      orderTxn.orderId
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

            await Promise.all(
              disabledOrders.map(async (orderTxn) => {
                try {
                  await this.makeRefundTransaction(orderTxn, latestBlockTimestamp, `d1: DEX has been disabled`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for disabled DEX order ID ${
                      orderTxn.orderId
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

            await Promise.all(
              invalidOrders.map(async (orderTxn) => {
                try {
                  await this.makeRefundTransaction(orderTxn, latestBlockTimestamp, `r1,${orderTxn.orderId}: Invalid order`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for invalid order ID ${
                      orderTxn.orderId
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

            await Promise.all(
              oversizedOrders.map(async (orderTxn) => {
                try {
                  await this.makeRefundTransaction(orderTxn, latestBlockTimestamp, `r1,${orderTxn.orderId}: Invalid order`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for oversized order ID ${
                      orderTxn.orderId
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
                let refundTxn = {
                  sourceChain: expiredOrder.sourceChain,
                  sourceWalletAddress: expiredOrder.sourceWalletAddress
                };
                if (refundTxn.sourceChain === this.baseChainSymbol) {
                  refundTxn.sourceChainAmount = expiredOrder.sizeRemaining * expiredOrder.price;
                } else {
                  refundTxn.sourceChainAmount = expiredOrder.sizeRemaining;
                }
                try {
                  await this.makeRefundTransaction(refundTxn, latestBlockTimestamp, `r2,${expiredOrder.orderId}: Expired order`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for expired order ID ${
                      expiredOrder.orderId
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
                  `Chain ${chainSymbol}: Order ${expiredOrder.orderId} at height ${expiredOrder.height} expired`
                );
              });
            }

            await Promise.all(
              cancelOrders.map(async (orderTxn) => {
                let targetOrder = this.tradeEngine.getOrder(orderTxn.orderIdToCancel);
                if (!targetOrder) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to cancel order with ID ${orderTxn.orderIdToCancel} because it could not be found`
                  );
                  return;
                }
                if (targetOrder.sourceChain !== orderTxn.sourceChain) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Could not cancel order ID ${orderTxn.orderIdToCancel} because it is on a different chain`
                  );
                  return;
                }
                if (targetOrder.sourceWalletAddress !== orderTxn.sourceWalletAddress) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Could not cancel order ID ${orderTxn.orderIdToCancel} because it belongs to a different account`
                  );
                  return;
                }
                let refundTxn = {
                  sourceChain: targetOrder.sourceChain,
                  sourceWalletAddress: targetOrder.sourceWalletAddress
                };
                if (refundTxn.sourceChain === this.baseChainSymbol) {
                  refundTxn.sourceChainAmount = targetOrder.sizeRemaining * targetOrder.price;
                } else {
                  refundTxn.sourceChainAmount = targetOrder.sizeRemaining;
                }
                // Also send back any amount which was sent as part of the cancel order.
                refundTxn.sourceChainAmount += orderTxn.sourceChainAmount;

                let result;
                try {
                  result = this.tradeEngine.cancelOrder(orderTxn.orderIdToCancel);
                } catch (error) {
                  this.logger.error(error);
                  return;
                }
                try {
                  await this.makeRefundTransaction(refundTxn, latestBlockTimestamp, `r3,${targetOrder.orderId},${orderTxn.orderId}: Canceled order`);
                } catch (error) {
                  this.logger.error(
                    `Chain ${chainSymbol}: Failed to post multisig refund transaction for canceled order ID ${
                      targetOrder.orderId
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
                  let takerAmount;
                  if (orderTxn.type === 'market') {
                    takerAmount = takerTargetChain === this.baseChainSymbol ? Math.abs(result.taker.fundsRemaining) : result.takeSize;
                  } else {
                    takerAmount = takerTargetChain === this.baseChainSymbol ? result.takeSize * result.taker.price : result.takeSize;
                  }
                  takerAmount -= takerChainOptions.exchangeFeeBase;
                  takerAmount -= takerAmount * takerChainOptions.exchangeFeeRate;
                  takerAmount = Math.floor(takerAmount);

                  if (takerAmount <= 0) {
                    this.logger.error(
                      `Chain ${chainSymbol}: Failed to take the trade order ${orderTxn.orderId} because the amount after fees was less than or equal to 0`
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
                        takerTxn,
                        `t1,${result.taker.sourceChain},${result.taker.orderId}: Orders taken`
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
                        sourceWalletAddress: result.taker.sourceWalletAddress
                      };
                      if (result.taker.sourceChain === this.baseChainSymbol) {
                        refundTxn.sourceChainAmount = result.taker.fundsRemaining;
                      } else {
                        refundTxn.sourceChainAmount = result.taker.sizeRemaining;
                      }
                      if (refundTxn.sourceChainAmount <= 0) {
                        return;
                      }
                      try {
                        await this.makeRefundTransaction(refundTxn, latestBlockTimestamp, `r4,${orderTxn.orderId}: Unmatched market order part`);
                      } catch (error) {
                        this.logger.error(
                          `Chain ${chainSymbol}: Failed to post multisig market order refund transaction of taker ${takerAddress} on chain ${takerTargetChain} because of error: ${error.message}`
                        );
                      }
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
                          `Chain ${chainSymbol}: Failed to make the trade order ${makerOrder.orderId} because the amount after fees was less than or equal to 0`
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
                            makerTxn,
                            `t2,${makerOrder.sourceChain},${makerOrder.orderId},${result.taker.orderId}: Order made`
                          );
                        } catch (error) {
                          this.logger.error(
                            `Chain ${chainSymbol}: Failed to post multisig transaction of maker ${makerAddress} on chain ${makerOrder.targetChain} because of error: ${error.message}`
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

  async makeRefundTransaction(orderTxn, timestamp, reason) {
    let refundChainOptions = this.options.chains[orderTxn.sourceChain];
    let refundAmount = BigInt(orderTxn.sourceChainAmount) - BigInt(refundChainOptions.exchangeFeeBase);
    // Refunds do not charge the exchangeFeeRate.

    if (refundAmount <= 0n) {
      throw new Error(
        'Failed to make refund because amount was less than 0'
      );
    }

    let refundTxn = {
      amount: refundAmount.toString(),
      recipientId: orderTxn.sourceWalletAddress,
      timestamp
    };
    await this.makeMultiSigTransaction(
      orderTxn.sourceChain,
      refundTxn,
      reason
    );
  }

  async makeMultiSigTransaction(targetChain, transactionData, message) {
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
    if (message != null) {
      txn.asset.data = message;
    }
    let signedTxn = liskTransactions.utils.prepareTransaction(txn, chainOptions.sharedPassphrase);
    let multiSigTxnSignature = liskTransactions.utils.multiSignTransaction(signedTxn, chainOptions.passphrase);
    let publicKey = liskCryptography.getAddressAndPublicKeyFromPassphrase(chainOptions.passphrase).publicKey;

    await this.channel.invoke(`${targetModuleAlias}:postTransaction`, { transaction: signedTxn });
    await wait(this.options.signatureBroadcastDelay);
    await this.channel.invoke(`${targetModuleAlias}:postSignature`, {
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
      this.currentProcessedHeights[chainSymbol] = this.lastSnapshotHeights[chainSymbol];
    });
  }

  async revertToLastSnapshot() {
    this.tradeEngine.setSnapshot(this.lastSnapshot.orderBook);
    this.lastSnapshotHeights = this.lastSnapshot.chainHeights;
    this.currentProcessedHeights = {};
    Object.keys(this.lastSnapshotHeights).forEach((chainSymbol) => {
      this.currentProcessedHeights[chainSymbol] = this.lastSnapshotHeights[chainSymbol];
    });
  }

  async saveSnapshot() {
    let snapshot = {};
    snapshot.orderBook = this.tradeEngine.getSnapshot();
    snapshot.chainHeights = {};
    Object.keys(this.currentProcessedHeights).forEach((chainSymbol) => {
      let targetHeight = this.currentProcessedHeights[chainSymbol];
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

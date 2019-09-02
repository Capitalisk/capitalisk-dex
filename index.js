'use strict';

const defaultConfig = require('./defaults/config');
// const { migrations } = require('./migrations');
const BaseModule = require('lisk-framework/src/modules/base_module');
const { createStorageComponent } = require('lisk-framework/src/components/storage');
const { createLoggerComponent } = require('lisk-framework/src/components/logger');
const TradeEngine = require('./trade-engine');
const liskTransactions = require('@liskhq/lisk-transactions');
const liskCryptography = require('@liskhq/lisk-cryptography');

const WritableConsumableStream = require('writable-consumable-stream');

const MODULE_ALIAS = 'lisk_dex';

// TODO 2: Add a way to sync with the chain from any height in the past in an idempotent way.
/**
 * Lisk DEX module specification
 *
 * @namespace Framework.Modules
 * @type {module.LiskDEXModule}
 */
module.exports = class LiskDEXModule extends BaseModule {
	constructor(options) {
		super({...defaultConfig, ...options});
    this.chainNames = Object.keys(this.options.chains);
		if (this.chainNames.length !== 2) {
			throw new Error('The DEX module must operate on exactly 2 chains only');
		}
		this.baseChainName = this.options.baseChain;
		this.quoteChainName = this.chainNames.find(chain => chain !== this.baseChainName);
		this.tradeEngine = new TradeEngine({
			baseCurrency: this.baseChainName,
			quoteCurrency: this.quoteChainName
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
		return {
			// calculateSupply: {
			// 	handler: action => this.chain.actions.calculateSupply(action),
			// },
		};
	}

	async load(channel) {
    let loggerConfig = await channel.invoke(
			'app:getComponentConfig',
			'logger',
		);
    this.logger = createLoggerComponent(loggerConfig);

    let storageConfigOptions = await channel.invoke(
      'app:getComponentConfig',
      'storage',
    );
    this.chainNames.forEach(async (chainName) => {
			let chainOptions = this.options.chains[chainName];
      let storageConfig = {
  			...storageConfigOptions,
  			database: chainOptions.database,
  		};
      let storage = createStorageComponent(storageConfig, this.logger); // TODO 2: Is this logger needed?
			await storage.bootstrap();

			let blockChangeStream = new WritableConsumableStream();

			(async () => {
				for await (let event of blockChangeStream) {
					let block = event.data;
	        let targetHeight = parseInt(block.height) - this.options.requiredConfirmations;
	        let blockData = (
						await storage.adapter.db.query(
							'select blocks.id, blocks."numberOfTransactions" from blocks where height = $1',
							[targetHeight],
						)
					)[0];

					if (!blockData) {
						this.logger.error(
							`Failed to fetch block at height ${targetHeight}`
						);

						return;
					}
					if (!blockData.numberOfTransactions) {
						this.logger.trace(
							`No transactions in block ${blockData.id} at height ${targetHeight}`
						);

						return;
					}
					let orders = await storage.adapter.db.query(
						'select trs.id, trs."senderId", trs."timestamp", trs."recipientId", trs."amount", trs."transferData" from trs where trs."blockId" = $1 and trs."transferData" is not null and trs."recipientId" = $2',
						[blockData.id, chainOptions.walletAddress],
					)
					.map((txn) => {
						// TODO 2: Check that this is the correct way to read bytea type from Postgres.
						let transferDataString = txn.transferData.toString('utf8');
						let dataParts = transferDataString.split(',');
						let orderTxn = {...txn};
						let targetChain = dataParts[1];
						let isSupportedChain = this.options.chains[targetChain] && targetChain !== chainName;

						if (dataParts[0] === 'limit' && isSupportedChain) {
							orderTxn.type = 'limit';
							orderTxn.price = parseInt(dataParts[2]);
							orderTxn.targetChain = targetChain;
							orderTxn.targetWalletAddress = dataParts[3];
							let amount = parseInt(orderTxn.amount);
							if (chainName === this.options.baseChain) {
								orderTxn.side = 'bid';
								orderTxn.size = Math.floor(amount / orderTxn.price); // TODO 2: Use BigInt instead.
							} else {
								orderTxn.side = 'ask';
								orderTxn.size = amount; // TODO 2: Use BigInt instead.
							}
						} else {
							this.logger.debug(
								`Incoming transaction ${txn.id} is not a valid limit order`
							);
						}
						return orderTxn;
					})
					.filter((orderTxn) => {
						return orderTxn.type === 'limit';
					});

					let baseChainOptions = this.options.chains[this.baseChainName];
					let quoteChainOptions = this.options.chains[this.quoteChainName];

					await Promise.all(
						orders.map(async (orderTxn) => {
							let result = this.tradeEngine.addOrder(orderTxn);
							if (result.takeSize > 0) {
								let takerAddress = result.taker.targetWalletAddress;
								let takerTxn = {
									type: 0,
									amount: result.takeSize,
									recipientId: takerAddress,
									fee: liskTransactions.constants.TRANSFER_FEE.toString(),
									asset: {},
									timestamp: orderTxn.timestamp,
									senderPublicKey: liskCryptography.getAddressAndPublicKeyFromPassphrase(quoteChainOptions.sharedPassphrase).publicKey
								};
								let takerSignedTxn = liskTransactions.utils.prepareTransaction(takerTxn, quoteChainOptions.sharedPassphrase);
								let takerMultiSigTxnSignature = liskTransactions.utils.multiSignTransaction(takerSignedTxn, quoteChainOptions.passphrase);

								try {
									await channel.invoke(`${this.quoteChainName}:postTransaction`, { transaction: takerSignedTxn });
									await channel.invoke(`${this.quoteChainName}:postSignature`, { signature: takerMultiSigTxnSignature });
								} catch (error) {
									this.logger.error(
										`Failed to post multisig transaction of taker ${takerAddress} on chain ${this.quoteChainName} because of error: ${error.message}`
									);
									return;
								}

								await Promise.all(
									result.makers.map(async (makerOrder) => {
										let makerAddress = makerOrder.targetWalletAddress;

										let makerTxn = {
											type: 0,
											amount: makerOrder.valueRemoved,
											recipientId: makerAddress,
											fee: liskTransactions.constants.TRANSFER_FEE.toString(),
											asset: {},
											timestamp: orderTxn.timestamp,
											senderPublicKey: liskCryptography.getAddressAndPublicKeyFromPassphrase(baseChainOptions.sharedPassphrase).publicKey
										};
										let makerSignedTxn = liskTransactions.utils.prepareTransaction(makerTxn, baseChainOptions.sharedPassphrase);
										let makerMultiSigTxnSignature = liskTransactions.utils.multiSignTransaction(makerSignedTxn, baseChainOptions.passphrase);

										try {
											await channel.invoke(`${this.baseChainName}:postTransaction`, { transaction: makerSignedTxn });
											await channel.invoke(`${this.baseChainName}:postSignature`, { signature: makerMultiSigTxnSignature });
										} catch (error) {
											this.logger.error(
												`Failed to post multisig transaction of maker ${makerAddress} on chain ${this.baseChainName} because of error: ${error.message}`
											);
											return;
										}
									})
								);
							}
						})
					);
				}
			})();

			// TODO 2: Use stream with for-await-of loop to guarantee that events are always processed sequentially to completion.
      channel.subscribe(`${chainName}:blocks:change`, (event) => {
				blockChangeStream.write(event);
      });
    });
		channel.publish(`${MODULE_ALIAS}:bootstrap`);
	}

	async unload() {
	}
};

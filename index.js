'use strict';

const defaultConfig = require('./defaults/config');
// const { migrations } = require('./migrations');
const BaseModule = require('lisk-framework/src/modules/base_module');
const { createStorageComponent } = require('lisk-framework/src/components/storage');
const { createLoggerComponent } = require('lisk-framework/src/components/logger');
const TradeEngine = require('./trade-engine');

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
    this.chainNames = Object.keys(this.options.chains);
		if (this.chainNames.length !== 2) {
			throw new Error('The DEX module must operate on exactly 2 chains only');
		}
		this.tradeEngine = new TradeEngine({
			baseCurrency: this.options.baseChain,
			quoteCurrency: this.chainNames.find(chain => chain !== this.options.baseChain)
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
			// console.log('**************RESULT:', storageConfig); // TODO 2-----
      let storage = createStorageComponent(storageConfig, this.logger); // TODO 2: Is this logger needed?
			await storage.bootstrap();

      channel.subscribe(`${chainName}:blocks:change`, async (event) => {
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
					'select trs.id, trs."senderId", trs."recipientId", trs."amount", trs."transferData" from trs where trs."blockId" = $1 and trs."transferData" is not null and trs."recipientId" = $2',
					[blockData.id, chainOptions.walletAddress],
				)
				.map((txn) => {
					// let dataString = Buffer.from(txn.transferData.replace(/^\\x/, ''), 'hex').toString('utf8')
					// TODO 2: Check that this is the correct way to read bytea type from Postgres.
					let transferDataString = txn.transferData.toString('utf8');
					let dataParts = transferDataString.split(',');
					let orderTxn = {...txn};
					let targetChain = dataParts[1];
					let isSupportedChain = this.options.chains[targetChain] && targetChain !== chainName;

					if (dataParts[0] === 'limit' && isSupportedChain) {
						orderTxn.type = 'limit';
						orderTxn.price = parseInt(dataParts[2]);
						orderTxn.targetWalletAddress = dataParts[3];
						let amount = parseInt(orderTxn.amount);
						if (chainName === this.options.baseChain) {
							orderTxn.side = 'bid';
							orderTxn.size = Math.floor(amount / orderTxn.price); // TODO 2: Use BitInt instead.
						} else {
							orderTxn.side = 'ask';
							orderTxn.size = amount; // TODO 2: Use BitInt instead.
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

				orders.forEach((orderTxn) => {
					let result = this.tradeEngine.addOrder(orderTxn);
					// TODO 222: IMPLEMENT NOW
				});
      });
    });
		channel.publish(`${MODULE_ALIAS}:bootstrap`);
	}

	async unload() {
	}
};

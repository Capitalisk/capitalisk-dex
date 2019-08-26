'use strict';

const defaultConfig = require('./defaults/config');
// const { migrations } = require('./migrations');
const BaseModule = require('lisk-framework/src/modules/base_module');
const { createStorageComponent } = require('lisk-framework/src/components/storage');
const { createLoggerComponent } = require('lisk-framework/src/components/logger');

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
    const loggerConfig = await channel.invoke(
			'app:getComponentConfig',
			'logger',
		);
    this.logger = createLoggerComponent(loggerConfig);

    const storageConfigOptions = await channel.invoke(
      'app:getComponentConfig',
      'storage',
    );
    this.chainNames.forEach(async (chainName) => {
      const storageConfig = {
  			...storageConfigOptions,
  			database: this.options.chains[chainName].database,
  		};
			// console.log('**************RESULT:', storageConfig); // TODO 2-----
      const storage = createStorageComponent(storageConfig, this.logger); // TODO 2: Is this logger needed?
			await storage.bootstrap();

      channel.subscribe(`${chainName}:blocks:change`, async (event) => {
				const block = event.data;
        const targetHeight = parseInt(block.height) - this.options.requiredConfirmations;
        const blockData = (
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
				const transactions = await storage.adapter.db.query(
					'select trs.id, trs."senderId", trs."recipientId", trs."amount", trs."transferData" from trs where trs."blockId" = $1',
					[blockData.id],
				);

        console.log('------------RESULT:', transactions); // TODO 2 -------------------
      });
    });
		channel.publish(`${MODULE_ALIAS}:bootstrap`);
	}

	async unload() {
	}
};

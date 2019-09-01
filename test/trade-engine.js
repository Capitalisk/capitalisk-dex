const assert = require('assert');
const TradeEngine = require('../trade-engine');

describe('TradeEngine unit tests', async () => {
  let tradeEngine;

  beforeEach(async () => {
    tradeEngine = new TradeEngine({
      baseCurrency: 'chain',
      quoteCurrency: 'capitalisk'
    });
  });

  describe('Order matching', async () => {

    it('Bid order is made with greater size but same price as ask', async () => {
      let result;

      tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .1,
        targetWalletAddress: '22245678912345678222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .1,
        targetWalletAddress: '11145678912345678111L',
        side: 'bid',
        size: 1000
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.size, 1000);
    });

    it('Bid order is made with greater size and higher price as ask', async () => {
      let result;

      tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .1,
        targetWalletAddress: '22245678912345678222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .2,
        targetWalletAddress: '11145678912345678111L',
        side: 'bid',
        size: 1000
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.size, 1000);
    });

  });
});

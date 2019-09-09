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

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .1,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'limit',
        price: .1,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '22222222211111111111L',
        side: 'bid',
        size: 1000
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.size, 1000);
    });

    it('Bid order is made with greater size and higher price than ask', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .1,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'limit',
        price: .2,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '33311111111222222333L',
        side: 'bid',
        size: 1000
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.size, 1000);
    });

    it('Multiple bid orders in series', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .5,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 4000
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '44411111111222222444L',
        side: 'bid',
        size: 8
      });

      assert.equal(result.makers[0].valueTaken, 4);

      result = tradeEngine.addOrder({
        orderId: 'order2',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '55511111111222222555L',
        side: 'bid',
        size: 40
      });

      assert.equal(result.makers[0].valueTaken, 20);

      result = tradeEngine.addOrder({
        orderId: 'order3',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '66611111111222222666L',
        side: 'bid',
        size: 12
      });

      assert.equal(result.makers[0].valueTaken, 6);
    });

    it('Order with the same ID is not added', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .1,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      let error = null;

      try {
        result = tradeEngine.addOrder({
          orderId: 'order0',
          type: 'limit',
          price: .1,
          targetChain: 'lsk',
          targetWalletAddress: '22245678912345678222L',
          senderId: '55511111111222222777L',
          side: 'ask',
          size: 100
        });
      } catch (err) {
        error = err;
      }

      assert.notEqual(error, null);
      assert.equal(error.name, 'DuplicateOrderError');
    });

    it('Can get and set snapshot of the order book', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .5,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '88811111111222222333L',
        side: 'bid',
        size: 8
      });

      result = tradeEngine.addOrder({
        orderId: 'order2',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '11555111111222226662L',
        side: 'bid',
        size: 40
      });

      let snapshot = tradeEngine.getSnapshot();
      tradeEngine.clear();

      let snapshotAfterClear = tradeEngine.getSnapshot();

      assert.equal(JSON.stringify(snapshotAfterClear.askLimitOrders), '[]');
      assert.equal(JSON.stringify(snapshotAfterClear.bidLimitOrders), '[]');

      tradeEngine.setSnapshot(snapshot);

      result = tradeEngine.addOrder({
        orderId: 'order3',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '77711111111222442222L',
        side: 'bid',
        size: 12
      });

      assert.equal(result.makers[0].valueTaken, 6);

      result = tradeEngine.addOrder({
        orderId: 'order4',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '33331111111222222999L',
        side: 'bid',
        size: 50
      });
      assert.equal(result.makers[0].valueTaken, 20);
    });

    it('Market bid order', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .5,
        targetChain: 'lsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'market',
        targetChain: 'clsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '22222222211111111111L',
        side: 'bid',
        size: -1,
        funds: 10
      });

      assert.equal(result.takeSize, 20);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.fundsRemaining, 0);
    });

    it('Market ask order', async () => {
      let result;

      result = tradeEngine.addOrder({
        orderId: 'order0',
        type: 'limit',
        price: .5,
        targetChain: 'clsk',
        targetWalletAddress: '22245678912345678222L',
        senderId: '11111111111222222222L',
        side: 'bid',
        size: 100
      });

      result = tradeEngine.addOrder({
        orderId: 'order1',
        type: 'market',
        targetChain: 'lsk',
        targetWalletAddress: '11145678912345678111L',
        senderId: '22222222211111111111L',
        side: 'ask',
        size: 50,
        funds: -1
      });

      assert.equal(result.takeSize, 50);
      assert.equal(result.takeValue, 25);
      assert.equal(result.taker.orderId, 'order1');
      assert.equal(result.taker.sizeRemaining, 0);
    });

  });
});

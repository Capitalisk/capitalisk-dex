const assert = require('assert');
const TradeEngine = require('../trade-engine');

describe('TradeEngine unit tests', async () => {
  let tradeEngine;

  beforeEach(async () => {
    tradeEngine = new TradeEngine({
      baseCurrency: 'lsk',
      quoteCurrency: 'clsk',
      baseOrderHeightExpiry: 100,
      quoteOrderHeightExpiry: 100
    });
  });

  describe('Order matching', async () => {

    it('Bid order is made with greater size but same price as ask', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .1,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .1,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'bid',
        value: 100
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.taker.id, 'order1');
      assert.equal(result.taker.valueRemaining, 90);
    });

    it('Bid order is made with greater size and higher price than ask', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .1,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .2,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '33311111111222222333L',
        side: 'bid',
        value: 200
      });

      assert.equal(result.takeSize, 100);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.id, 'order1');
      assert.equal(result.taker.value, 200);
    });

    it('Multiple bid orders in series', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 4000
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '44411111111222222444L',
        side: 'bid',
        value: 4
      });

      assert.equal(result.makers[0].lastValueTaken, 4);

      result = tradeEngine.addOrder({
        id: 'order2',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '55511111111222222555L',
        side: 'bid',
        value: 20
      });

      assert.equal(result.makers[0].lastValueTaken, 20);

      result = tradeEngine.addOrder({
        id: 'order3',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '66611111111222222666L',
        side: 'bid',
        value: 6
      });

      assert.equal(result.makers[0].lastValueTaken, 6);
    });

    it('Order with the same ID is not added', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .1,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      let error = null;

      try {
        result = tradeEngine.addOrder({
          id: 'order0',
          type: 'limit',
          price: .1,
          sourceChain: 'clsk',
          targetChain: 'lsk',
          height: 1,
          targetWalletAddress: '22245678912345678222L',
          senderAddress: '55511111111222222777L',
          side: 'ask',
          size: 100
        });
      } catch (err) {
        error = err;
      }

      assert.notEqual(error, null);
      assert.equal(error.name, 'DuplicateOrderError');
    });

    it('Can get bid and ask iterators', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .7,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order2',
        type: 'limit',
        price: .6,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order3',
        type: 'limit',
        price: .4,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 100
      });

      result = tradeEngine.addOrder({
        id: 'order4',
        type: 'limit',
        price: .2,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 100
      });

      result = tradeEngine.addOrder({
        id: 'order5',
        type: 'limit',
        price: .3,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 100
      });

      let bidOrders = [...tradeEngine.getBidIteratorFromMax()];
      let askOrders = [...tradeEngine.getAskIteratorFromMin()];

      assert.equal(JSON.stringify(bidOrders.map(bid => bid.price)), JSON.stringify([.4, .3, .2]));
      assert.equal(JSON.stringify(askOrders.map(ask => ask.price)), JSON.stringify([.5, .6, .7]));
    });

    it('Can get and set snapshot of the order book', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '88811111111222222333L',
        side: 'bid',
        value: 4
      });

      result = tradeEngine.addOrder({
        id: 'order2',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '11555111111222226662L',
        side: 'bid',
        value: 20
      });

      let snapshot = tradeEngine.getSnapshot();
      tradeEngine.clear();

      let snapshotAfterClear = tradeEngine.getSnapshot();

      assert.equal(JSON.stringify(snapshotAfterClear.askLimitOrders), '[]');
      assert.equal(JSON.stringify(snapshotAfterClear.bidLimitOrders), '[]');

      tradeEngine.setSnapshot(snapshot);

      result = tradeEngine.addOrder({
        id: 'order3',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '77711111111222442222L',
        side: 'bid',
        value: 6
      });

      assert.equal(result.makers[0].lastValueTaken, 6);

      result = tradeEngine.addOrder({
        id: 'order4',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '33331111111222222999L',
        side: 'bid',
        value: 25
      });
      assert.equal(result.makers[0].lastValueTaken, 20);
    });

    it('The lastSize and lastSizeTaken property should be updated on both taker and makers', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 14
      });

      result = tradeEngine.addOrder({
        id: 'order841',
        type: 'limit',
        price: .55,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'ask',
        size: 1.18
      });

      result = tradeEngine.addOrder({
        id: 'order3',
        type: 'limit',
        price: .6,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'ask',
        size: 1.67
      });

      result = tradeEngine.addOrder({
        id: 'order567',
        type: 'limit',
        price: .56,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'bid',
        value: 1.9992
      });

      result = tradeEngine.addOrder({
        id: 'order630',
        type: 'limit',
        price: .56,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'ask',
        size: 6
      });

      assert.notEqual(result.makers[0], null);
      assert.equal(result.makers[0].lastSizeTaken, 2.4110714285714283);
    });

    it('Market bid order', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 100
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'market',
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'bid',
        value: 10
      });

      assert.equal(result.takeSize, 20);
      assert.equal(result.takeValue, 10);
      assert.equal(result.taker.id, 'order1');
      assert.equal(result.taker.valueRemaining, 0);
    });

    it('Market ask order', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 50
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'market',
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'ask',
        size: 50
      });

      assert.equal(result.takeSize, 50);
      assert.equal(result.takeValue, 25);
      assert.equal(result.taker.id, 'order1');
      assert.equal(result.taker.sizeRemaining, 0);
    });

    it('Large market bid order', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'ask',
        size: 4000000000
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'market',
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'bid',
        value: 2000000000
      });

      assert.equal(result.takeSize, 4000000000);
      assert.equal(result.takeValue, 2000000000);
    });

    it('Large market ask order', async () => {
      let result;

      result = tradeEngine.addOrder({
        id: 'order0',
        type: 'limit',
        price: .5,
        sourceChain: 'lsk',
        targetChain: 'clsk',
        height: 1,
        targetWalletAddress: '22245678912345678222L',
        senderAddress: '11111111111222222222L',
        side: 'bid',
        value: 500000000
      });

      result = tradeEngine.addOrder({
        id: 'order1',
        type: 'market',
        sourceChain: 'clsk',
        targetChain: 'lsk',
        height: 1,
        targetWalletAddress: '11145678912345678111L',
        senderAddress: '22222222211111111111L',
        side: 'ask',
        size: 2000000000
      });

      assert.equal(result.takeSize, 1000000000);
      assert.equal(result.takeValue, 500000000);
    });
  });
});

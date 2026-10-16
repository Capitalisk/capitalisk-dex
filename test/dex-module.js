const assert = require('assert');
const TradeEngine = require('../trade-engine');
const BigIntCalculator = require('../big-int-calculator');
const {
  createDEXModule,
  addBid,
  addAsk,
  createLockedOrderBook
} = require('./fixtures/dex-module');

describe('DEX module unit tests', async () => {
  let dexModule;

  beforeEach(async () => {
    dexModule = createDEXModule();
  });

  describe('Market orders against an empty order book', async () => {

    it('Does not throw when there are no asks to price a base chain market order against', async () => {
      assert.equal(dexModule.tradeEngine.peekAsks(), undefined);

      let error;
      let isTooSmall;
      try {
        isTooSmall = dexModule._isMarketOrderTooSmallToConvert('lsk', 100000000n);
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      // The order is valid; it simply has nothing to match against, so it must be
      // allowed through to the trade engine where the unmatched part is refunded.
      assert.equal(isTooSmall, false);
    });

    it('Does not throw when there are no bids to price a quote chain market order against', async () => {
      assert.equal(dexModule.tradeEngine.peekBids(), undefined);

      let error;
      let isTooSmall;
      try {
        isTooSmall = dexModule._isMarketOrderTooSmallToConvert('clsk', 100000000n);
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(isTooSmall, false);
    });

    it('An unmatched market order keeps its full amount so that it can be refunded', async () => {
      let error;
      let result;
      try {
        result = addBid(dexModule.tradeEngine, {id: 'market0', value: 100000000n, type: 'market'});
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(result.makers.length, 0);
      assert.equal(result.taker.valueRemaining, 100000000n);
    });

    it('Still rejects a market order which is too small to convert when the book is not empty', async () => {
      addAsk(dexModule.tradeEngine, {id: 'ask0', size: 100000000n, price: 1});

      // A quote chain value of 1000 does not even cover the 1000 exchangeFeeBase.
      assert.equal(dexModule._isMarketOrderTooSmallToConvert('lsk', 1000n), true);
      assert.equal(dexModule._isMarketOrderTooSmallToConvert('lsk', 100000000n), false);
    });

    it('The big int calculator yields NaN instead of throwing on a non-finite decimal', async () => {
      let calculator = new BigIntCalculator({decimalPrecision: 4});

      for (let decimalValue of [undefined, NaN, Infinity, -Infinity]) {
        let error;
        let multiplied;
        let divided;
        try {
          multiplied = calculator.multiplyBigIntByDecimal(100n, decimalValue);
          divided = calculator.divideBigIntByDecimal(100n, decimalValue);
        } catch (err) {
          error = err;
        }
        assert.equal(error, null, `multiply/divide threw for a decimal of ${decimalValue}`);
        assert.equal(Number.isNaN(multiplied), true);
        assert.equal(Number.isNaN(divided), true);
      }
    });

    it('The big int calculator treats a decimal of zero as zero rather than as invalid', async () => {
      let calculator = new BigIntCalculator({decimalPrecision: 4});

      // A fee rate of 0 means no fee; only division has to reject it.
      assert.equal(calculator.multiplyBigIntByDecimal(100n, 0), 0n);
      assert.equal(Number.isNaN(calculator.divideBigIntByDecimal(100n, 0)), true);
    });

    it('The big int calculator yields NaN instead of dividing by zero', async () => {
      let calculator = new BigIntCalculator({decimalPrecision: 4});

      let error;
      let divided;
      try {
        // 0.000001 rounds down to 0 price units at a decimal precision of 4.
        divided = calculator.divideBigIntByDecimal(100n, 0.000001);
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(Number.isNaN(divided), true);
    });
  });

  describe('Full order book refund', async () => {
    let snapshot;

    beforeEach(async () => {
      addBid(dexModule.tradeEngine, {id: 'bid0', value: 5000000n, price: 2});
      addAsk(dexModule.tradeEngine, {id: 'ask0', size: 3000000n, price: 5});
      // This is the snapshot format which processBlock passes to scheduleRefundOrderBook;
      // getSnapshot serializes every amount as a string so that it can be saved as JSON.
      snapshot = {
        orderBook: dexModule.tradeEngine.getSnapshot(),
        chainHeights: {
          lsk: 100,
          clsk: 200
        }
      };
    });

    it('Refunds every order of a serialized snapshot when the DEX is disabled', async () => {
      assert.equal(typeof snapshot.orderBook.bidLimitOrders[0].valueRemaining, 'string');

      let error;
      try {
        dexModule.scheduleRefundOrderBook(snapshot, 12345, {lsk: null, clsk: null});
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(dexModule.scheduledTransferInfos.length, 2);

      let [bidRefund, askRefund] = dexModule.scheduledTransferInfos;

      assert.equal(bidRefund.targetChain, 'lsk');
      assert.equal(bidRefund.transactionData.recipientAddress, 'bid0SourceWallet');
      assert.equal(bidRefund.transactionData.amount, '4999000');
      assert.equal(bidRefund.transactionData.height, 100);
      assert.equal(bidRefund.message, 'r6,bid0: DEX has been disabled');

      assert.equal(askRefund.targetChain, 'clsk');
      assert.equal(askRefund.transactionData.recipientAddress, 'ask0SourceWallet');
      assert.equal(askRefund.transactionData.amount, '2999000');
      assert.equal(askRefund.transactionData.height, 200);
      assert.equal(askRefund.message, 'r6,ask0: DEX has been disabled');
    });

    it('Refunds every order of a serialized snapshot when the DEX has moved', async () => {
      let error;
      try {
        dexModule.scheduleRefundOrderBook(snapshot, 12345, {lsk: 'lskNewAddress', clsk: 'clskNewAddress'});
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(dexModule.scheduledTransferInfos.length, 2);
      assert.equal(dexModule.scheduledTransferInfos[0].message, 'r5,bid0,lskNewAddress: DEX has moved');
      assert.equal(dexModule.scheduledTransferInfos[1].message, 'r5,ask0,clskNewAddress: DEX has moved');
    });

    it('Refunds orders which are held in the trade engine as big ints', async () => {
      let liveOrder = dexModule.tradeEngine.getOrder('bid0');
      assert.equal(typeof liveOrder.valueRemaining, 'bigint');

      let error;
      try {
        dexModule.scheduleRefundOrder(
          liveOrder,
          12345,
          100,
          dexModule._computeProtocolMessage('lsk', 'r2', [liveOrder.id], 'Expired order'),
          {type: 'r2', originOrderId: liveOrder.id},
          'Failed to refund expired order'
        );
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(dexModule.scheduledTransferInfos.length, 1);
      assert.equal(dexModule.scheduledTransferInfos[0].transactionData.amount, '4999000');
    });
  });

  describe('Limit order price validation', async () => {

    it('Accepts ordinary prices', async () => {
      for (let priceString of ['1', '2', '0.5', '.5', '1.0001', '100', '0.0001']) {
        assert.equal(dexModule._isLimitPriceValid(priceString), true, `rejected the price ${priceString}`);
      }
    });

    it('Accepts the largest price which can be represented at the configured precision', async () => {
      assert.equal(dexModule._isLimitPriceValid('900719925474'), true);
    });

    it('Rejects a price which is too large to represent at the configured precision', async () => {
      assert.equal(dexModule._isLimitPriceValid('900719925475'), false);
      assert.equal(dexModule._isLimitPriceValid('1000000000000'), false);
    });

    it('Rejects a price which overflows to Infinity', async () => {
      assert.equal(dexModule._isLimitPriceValid('9'.repeat(400)), false);
    });

    it('Rejects malformed and zero prices', async () => {
      for (let priceString of ['', '0', '-1', 'abc', '1e12', '0.00001', '1.2.3', undefined]) {
        assert.equal(dexModule._isLimitPriceValid(priceString), false, `accepted the price ${priceString}`);
      }
    });

    it('Rejects prices beyond the configured precision', async () => {
      let lowPrecisionModule = createDEXModule({priceDecimalPrecision: 2});
      assert.equal(lowPrecisionModule._isLimitPriceValid('0.01'), true);
      assert.equal(lowPrecisionModule._isLimitPriceValid('0.001'), false);
      assert.equal(lowPrecisionModule._isLimitPriceValid('90071992547409'), true);
      assert.equal(lowPrecisionModule._isLimitPriceValid('90071992547410'), false);
    });

    it('Never accepts a price which the trade engine would reject', async () => {
      let candidatePrices = [
        '1', '0.5', '.5', '0.0001', '900719925474', '900719925475',
        '1000000000000', '9'.repeat(400), '0', '0.00001'
      ];

      for (let [index, priceString] of candidatePrices.entries()) {
        if (!dexModule._isLimitPriceValid(priceString)) {
          continue;
        }
        let tradeEngine = new TradeEngine({
          baseCurrency: 'lsk',
          quoteCurrency: 'clsk',
          baseOrderHeightExpiry: 100,
          quoteOrderHeightExpiry: 100,
          priceDecimalPrecision: 4
        });
        let error;
        try {
          addAsk(tradeEngine, {id: `ask${index}`, size: 100000000n, price: Number(priceString)});
        } catch (err) {
          error = err;
        }
        assert.equal(error, null, `the trade engine rejected the accepted price ${priceString}`);
      }
    });
  });

  describe('getOrderBook action', async () => {

    it('Does not merge bid levels into ask levels when the order book is locked', async () => {
      createLockedOrderBook(dexModule.tradeEngine, 1000);

      let error;
      let orderBook;
      try {
        orderBook = dexModule.actions.getOrderBook.handler({params: {}});
      } catch (err) {
        error = err;
      }

      assert.equal(error, null);
      assert.equal(orderBook.length, 2);
      assert.equal(orderBook[0].side, 'ask');
      assert.equal(orderBook[0].price, 1000);
      assert.equal(orderBook[0].sizeRemaining, '500000');
      assert.equal(orderBook[0].valueRemaining, undefined);
      assert.equal(orderBook[1].side, 'bid');
      assert.equal(orderBook[1].price, 1000);
      assert.equal(orderBook[1].valueRemaining, '999');
      assert.equal(orderBook[1].sizeRemaining, undefined);
    });

    it('Reports asks above the bids from the highest ask price down to the lowest bid price', async () => {
      addBid(dexModule.tradeEngine, {id: 'bid0', value: 100000n, price: 1});
      addBid(dexModule.tradeEngine, {id: 'bid1', value: 200000n, price: 2});
      addAsk(dexModule.tradeEngine, {id: 'ask0', size: 300000n, price: 5});
      addAsk(dexModule.tradeEngine, {id: 'ask1', size: 400000n, price: 6});

      let orderBook = dexModule.actions.getOrderBook.handler({params: {}});

      assert.equal(orderBook.length, 4);
      assert.deepEqual(
        orderBook.map(level => [level.side, level.price]),
        [['ask', 6], ['ask', 5], ['bid', 2], ['bid', 1]]
      );
      assert.equal(orderBook[0].sizeRemaining, '400000');
      assert.equal(orderBook[2].valueRemaining, '200000');
    });

    it('Aggregates orders which share a price into a single level', async () => {
      addBid(dexModule.tradeEngine, {id: 'bid0', value: 100000n, price: 2});
      addBid(dexModule.tradeEngine, {id: 'bid1', value: 200000n, price: 2});
      addAsk(dexModule.tradeEngine, {id: 'ask0', size: 300000n, price: 5});
      addAsk(dexModule.tradeEngine, {id: 'ask1', size: 400000n, price: 5});

      let orderBook = dexModule.actions.getOrderBook.handler({params: {}});

      assert.equal(orderBook.length, 2);
      assert.equal(orderBook[0].side, 'ask');
      assert.equal(orderBook[0].sizeRemaining, '700000');
      assert.equal(orderBook[1].side, 'bid');
      assert.equal(orderBook[1].valueRemaining, '300000');
    });
  });
});

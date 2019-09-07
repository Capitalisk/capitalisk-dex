const LimitOrder = require('limit-order-book').LimitOrder;
const LimitOrderBook = require('limit-order-book').LimitOrderBook;

class TradeEngine {
  constructor(options) {
    this.baseCurrency = options.baseCurrency;
    this.quoteCurrency = options.quoteCurrency;
    this.market = `${this.quoteCurrency}/${this.baseCurrency}`;
    this.orderBook = new LimitOrderBook();
  }

  addOrder(order) {
    let limitQueue = order.side === 'ask' ? this.orderBook.askLimits : this.orderBook.bidLimits;
    let limit = limitQueue.map[order.price] || {map: {}};
    let existingOrder = limit.map[order.id];

    if (existingOrder) {
      let error = new Error(`An order with ID ${order.id} already exists`);
      error.name = 'DuplicateOrderError';
      throw error;
    }

    let limitOrder = new LimitOrder(order.id, order.side, order.price, order.size);
    limitOrder.targetChain = order.targetChain;
    limitOrder.targetWalletAddress = order.targetWalletAddress;
    let result = this.orderBook.add(limitOrder);
    result.makers.forEach((makerOrder) => {
      makerOrder.valueTaken = makerOrder.valueRemoved;
      makerOrder.sizeTaken = makerOrder.size - makerOrder.sizeRemaining;

      // These need to be reset for the next time or else they will accumulate.
      makerOrder.valueRemoved = 0;
      makerOrder.size = makerOrder.sizeRemaining;
    });
    return result;
  }

  getSnapshot() {
    let askLimitOrders = [];
    let askLimitsMap = this.orderBook.askLimits.map;
    Object.keys(askLimitsMap).forEach((price) => {
      let limit = askLimitsMap[price];
      let limitMap = limit.map;
      Object.keys(limitMap).forEach((orderId) => {
        askLimitOrders.push(limitMap[orderId]);
      });
    });

    let bidLimitOrders = [];
    let bidLimitsMap = this.orderBook.bidLimits.map;
    Object.keys(bidLimitsMap).forEach((price) => {
      let limit = bidLimitsMap[price];
      let limitMap = limit.map;
      Object.keys(limitMap).forEach((orderId) => {
        bidLimitOrders.push(limitMap[orderId]);
      });
    });

    return {
      askLimitOrders,
      bidLimitOrders
    };
  }

  setSnapshot(snapshot) {
    this.clear();
    snapshot.askLimitOrders.forEach((order) => {
      this.addOrder(order);
    });
    snapshot.bidLimitOrders.forEach((order) => {
      this.addOrder(order);
    });
  }

  clear() {
    this.orderBook.clear();
  }

  // TODO: Implement.
  cancelOrder() {

  }
}

module.exports = TradeEngine;

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
    let limitOrder = new LimitOrder(order.id, order.side, order.price, order.size);
    limitOrder.targetChain = order.targetChain;
    limitOrder.targetWalletAddress = order.targetWalletAddress;
    return this.orderBook.add(limitOrder);
  }

  // TODO: Implement.
  cancelOrder() {

  }
}

module.exports = TradeEngine;

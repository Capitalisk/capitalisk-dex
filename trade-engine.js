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

  // TODO: Implement.
  cancelOrder() {

  }
}

module.exports = TradeEngine;

const LimitOrder = require('limit-order-book').LimitOrder;
const MarketOrder = require('limit-order-book').MarketOrder;
const LimitOrderBook = require('limit-order-book').LimitOrderBook;

// This is necessary to fix a bug in the underlying library.
MarketOrder.prototype.getSizeRemainingFor = function (price) {
  var fundsTakeSize = Math.floor(this.fundsRemaining / price);

  if (this.funds > 0 && this.size > 0) {
    return Math.min(fundsTakeSize, this.sizeRemaining);
  } else if (this.funds > 0) {
    return fundsTakeSize;
  } else if (this.size > 0) {
    return this.sizeRemaining;
  } else {
    return 0;
  }
};

class TradeEngine {
  constructor(options) {
    this.baseCurrency = options.baseCurrency;
    this.quoteCurrency = options.quoteCurrency;
    this.orderHeightExpiry = options.orderHeightExpiry;
    this.market = `${this.quoteCurrency}/${this.baseCurrency}`;
    this.orderBook = new LimitOrderBook();

    this._asksExpiryMap = new Map();
    this._bidsExpiryMap = new Map();
  }

  expireBidOrders(heightThreshold) {
    let expiredOrders = [];
    for (let [orderId, order] of this._bidsExpiryMap) {
      if (order.expiryHeight > heightThreshold) {
        break;
      }
      expiredOrders.push(order);
      this._bidsExpiryMap.delete(orderId);
    }
    return expiredOrders;
  }

  expireAskOrders(heightThreshold) {
    let expiredOrders = [];
    for (let [orderId, order] of this._asksExpiryMap) {
      if (order.expiryHeight > heightThreshold) {
        break;
      }
      expiredOrders.push(order);
      this._asksExpiryMap.delete(orderId);
    }
    return expiredOrders;
  }

  addOrder(order) {
    let limitQueue = order.side === 'ask' ? this.orderBook.askLimits : this.orderBook.bidLimits;
    let limit = limitQueue.map[order.price] || {map: {}};
    let existingOrder = limit.map[order.orderId];

    if (existingOrder) {
      let error = new Error(`An order with ID ${order.orderId} already exists`);
      error.name = 'DuplicateOrderError';
      throw error;
    }

    let newOrder = this._createOrderInstance(order);
    newOrder.type = order.type;
    newOrder.targetChain = order.targetChain;
    newOrder.targetWalletAddress = order.targetWalletAddress;
    newOrder.senderId = order.senderId;
    newOrder.sourceChain = order.sourceChain;
    newOrder.sourceChainAmount = order.sourceChainAmount;
    newOrder.sourceWalletAddress = order.sourceWalletAddress;
    newOrder.height = order.height;
    newOrder.expiryHeight = order.height + this.orderHeightExpiry;
    newOrder.timestamp = order.timestamp;

    let result = this.orderBook.add(newOrder);

    result.makers.forEach((makerOrder) => {
      makerOrder.sizeTaken = makerOrder.lastSize == null ? makerOrder.size - makerOrder.sizeRemaining : makerOrder.lastSize - makerOrder.sizeRemaining;
      makerOrder.valueTaken = makerOrder.sizeTaken * makerOrder.price;
      makerOrder.lastSize = makerOrder.sizeRemaining;

      if (makerOrder.sizeRemaining <= 0) {
        if (makerOrder.side === 'ask') {
          this._asksExpiryMap.delete(makerOrder.orderId);
        } else {
          this._bidsExpiryMap.delete(makerOrder.orderId);
        }
      }
    });

    if (newOrder.type !== 'market' && result.taker.sizeRemaining > 0) {
      if (newOrder.side === 'ask') {
        this._asksExpiryMap.set(newOrder.orderId, newOrder);
      } else {
        this._bidsExpiryMap.set(newOrder.orderId, newOrder);
      }
    }

    return result;
  }

  getOrder(orderId) {
    let askOrder = this._asksExpiryMap.get(orderId);
    let bidOrder = this._bidsExpiryMap.get(orderId);
    return askOrder || bidOrder;
  }

  cancelOrder(orderId) {
    let order = this.getOrder(orderId);
    if (!order) {
      throw new Error(
        `An order with ID ${orderId} could not be found`
      );
    }

    let result = this.orderBook.remove(order.side, order.price, orderId);
    if (order.side === 'ask') {
      this._asksExpiryMap.delete(orderId);
    } else {
      this._bidsExpiryMap.delete(orderId);
    }
    return result;
  }

  peekBids() {
    return this.orderBook.bidLimits.peek();
  }

  peekAsks() {
    return this.orderBook.askLimits.peek();
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

  _createOrderInstance(order) {
    let newOrder;
    if (order.type === 'market') {
      newOrder = new MarketOrder(order.orderId, order.side, order.size, order.funds);
    } else {
      newOrder = new LimitOrder(order.orderId, order.side, order.price, order.size);
    }
    return newOrder;
  }

  setSnapshot(snapshot) {
    this.clear();
    snapshot.askLimitOrders.sort((a, b) => {
      if (a.height > b.height) {
        return 1;
      }
      if (a.height < b.height) {
        return -1;
      }
      return 0;
    });
    snapshot.bidLimitOrders.sort((a, b) => {
      if (a.height > b.height) {
        return 1;
      }
      if (a.height < b.height) {
        return -1;
      }
      return 0;
    });
    snapshot.askLimitOrders.forEach((order) => {
      let newOrder = this._createOrderInstance(order);
      this._asksExpiryMap.set(newOrder.orderId, newOrder);
      Object.keys(order).forEach((key) => {
        newOrder[key] = order[key];
      });
      this.orderBook.askLimits.addOrder(newOrder);
    });
    snapshot.bidLimitOrders.forEach((order) => {
      let newOrder = this._createOrderInstance(order);
      this._bidsExpiryMap.set(newOrder.orderId, newOrder);
      Object.keys(order).forEach((key) => {
        newOrder[key] = order[key];
      });
      this.orderBook.bidLimits.addOrder(newOrder);
    });
  }

  clear() {
    this._asksExpiryMap.clear();
    this._bidsExpiryMap.clear();
    this.orderBook.clear();
  }
}

module.exports = TradeEngine;

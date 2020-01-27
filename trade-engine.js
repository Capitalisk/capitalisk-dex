const ProperOrderBook = require('proper-order-book');

class TradeEngine {
  constructor(options) {
    this.baseCurrency = options.baseCurrency;
    this.quoteCurrency = options.quoteCurrency;
    this.baseOrderHeightExpiry = options.baseOrderHeightExpiry;
    this.quoteOrderHeightExpiry = options.quoteOrderHeightExpiry;
    this.market = `${this.quoteCurrency}/${this.baseCurrency}`;
    this.orderBook = new ProperOrderBook();

    this._askMap = new Map();
    this._bidMap = new Map();
    this._orderMap = new Map();
  }

  expireBidOrders(heightThreshold) {
    let expiredOrders = [];
    for (let [orderId, order] of this._bidMap) {
      if (order.expiryHeight > heightThreshold) {
        break;
      }
      expiredOrders.push(order);
      this._bidMap.delete(orderId);
      this._orderMap.delete(orderId);
    }
    return expiredOrders;
  }

  expireAskOrders(heightThreshold) {
    let expiredOrders = [];
    for (let [orderId, order] of this._askMap) {
      if (order.expiryHeight > heightThreshold) {
        break;
      }
      expiredOrders.push(order);
      this._askMap.delete(orderId);
      this._orderMap.delete(orderId);
    }
    return expiredOrders;
  }

  addOrder(order) {
    let existingOrder = this.orderBook.has(order.id);

    if (existingOrder) {
      let error = new Error(`An order with ID ${order.id} already exists`);
      error.name = 'DuplicateOrderError';
      throw error;
    }

    let orderHeightExpiry;
    if (order.sourceChain === this.quoteCurrency) {
      orderHeightExpiry = this.quoteOrderHeightExpiry;
    } else {
      orderHeightExpiry = this.baseOrderHeightExpiry;
    }

    let newOrder = {};
    newOrder.id = order.id;
    newOrder.side = order.side;
    if (order.size != null) {
      newOrder.size = order.size;
    }
    if (order.value != null) {
      newOrder.value = order.value;
    }
    if (order.price != null) {
      newOrder.price = order.price;
    }
    newOrder.type = order.type;
    newOrder.targetChain = order.targetChain;
    newOrder.targetWalletAddress = order.targetWalletAddress;
    newOrder.senderId = order.senderId;
    newOrder.sourceChain = order.sourceChain;
    newOrder.sourceChainAmount = order.sourceChainAmount;
    newOrder.sourceWalletAddress = order.sourceWalletAddress;
    newOrder.height = order.height;
    newOrder.expiryHeight = order.height + orderHeightExpiry;
    newOrder.timestamp = order.timestamp;

    let result = this.orderBook.add(newOrder);

    result.makers.forEach((makerOrder) => {
      if (makerOrder.side === 'ask') {
        if (makerOrder.sizeRemaining <= 0) {
          this._askMap.delete(makerOrder.id);
          this._orderMap.delete(makerOrder.id);
        }
      } else {
        if (makerOrder.valueRemaining <= 0) {
          this._bidMap.delete(makerOrder.id);
          this._orderMap.delete(makerOrder.id);
        }
      }
    });

    if (newOrder.type !== 'market') {
      if (newOrder.side === 'ask') {
        if (result.taker.sizeRemaining > 0) {
          this._askMap.set(newOrder.id, newOrder);
          this._orderMap.set(newOrder.id, newOrder);
        }
      } else if (result.taker.valueRemaining > 0) {
        this._bidMap.set(newOrder.id, newOrder);
        this._orderMap.set(newOrder.id, newOrder);
      }
    }

    return result;
  }

  getOrder(orderId) {
    return this._orderMap.get(orderId);
  }

  closeOrder(orderId) {
    let order = this.getOrder(orderId);
    if (!order) {
      throw new Error(
        `An order with ID ${orderId} could not be found`
      );
    }

    let result = this.orderBook.remove(orderId);
    if (order.side === 'ask') {
      this._askMap.delete(orderId);
    } else {
      this._bidMap.delete(orderId);
    }
    this._orderMap.delete(orderId);
    return result;
  }

  peekBids() {
    return this.orderBook.getMaxBid();
  }

  peekAsks() {
    return this.orderBook.getMinAsk();
  }

  getBidIterator() {
    return this._bidMap.values();
  }

  getAskIterator() {
    return this._askMap.values();
  }

  getOrderIterator() {
    return this._orderMap.values();
  }

  getBids() {
    return [...this.getBidIterator()];
  }

  getAsks() {
    return [...this.getAskIterator()];
  }

  getOrders() {
    return [...this.getOrderIterator()];
  }

  getSnapshot() {
    let askLimitOrders = this.getAsks().map(order => ({...order}));
    let bidLimitOrders = this.getBids().map(order => ({...order}));

    return {
      askLimitOrders,
      bidLimitOrders
    };
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
      let newOrder = {...order};
      this._askMap.set(newOrder.id, newOrder);
      this._orderMap.set(newOrder.id, newOrder);
      this.orderBook.add(newOrder);
    });
    snapshot.bidLimitOrders.forEach((order) => {
      let newOrder = {...order};
      this._bidMap.set(newOrder.id, newOrder);
      this._orderMap.set(newOrder.id, newOrder);
      this.orderBook.add(newOrder);
    });
  }

  clear() {
    this._askMap.clear();
    this._bidMap.clear();
    this._orderMap.clear();
    this.orderBook.clear();
  }
}

module.exports = TradeEngine;

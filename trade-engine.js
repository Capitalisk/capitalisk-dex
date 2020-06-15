const ProperOrderBook = require('proper-order-book');
const crypto = require('crypto');

const EMPTY_ORDER_BOOK_HASH = '0000000000000000000000000000000000000000';
const emptyGenerator = function * () {};

class TradeEngine {
  constructor(options) {
    this.baseCurrency = options.baseCurrency;
    this.quoteCurrency = options.quoteCurrency;
    this.baseOrderHeightExpiry = options.baseOrderHeightExpiry;
    this.quoteOrderHeightExpiry = options.quoteOrderHeightExpiry;
    this.baseMinPartialTake = options.baseMinPartialTake;
    this.quoteMinPartialTake = options.quoteMinPartialTake;
    this.market = `${this.quoteCurrency}/${this.baseCurrency}`;
    this.orderBook = new ProperOrderBook({
      minPartialTakeValue: this.baseMinPartialTake,
      minPartialTakeSize: this.quoteMinPartialTake
    });

    this._askMap = new Map();
    this._bidMap = new Map();
    this._orderMap = new Map();
    this._sourceWalletOrderMap = new Map();

    this.orderBookHash = EMPTY_ORDER_BOOK_HASH;

    this._resetProcessedHeightsInfo();
  }

  _sha1(string) {
    return crypto.createHash('sha1').update(string).digest('hex');
  }

  _resetProcessedHeightsInfo() {
    this.lastProcessedHeightsInfo = {
      [this.baseCurrency]: {
        height: 0,
        orderIds: new Set()
      },
      [this.quoteCurrency]: {
        height: 0,
        orderIds: new Set()
      }
    };
  }

  expireBidOrders(heightThreshold) {
    let expiredOrders = [];
    for (let [orderId, order] of this._bidMap) {
      if (order.expiryHeight > heightThreshold) {
        break;
      }
      expiredOrders.push(order);
      this._removeFromOrderBook(orderId);
      this._bidMap.delete(orderId);
      this._orderMap.delete(orderId);
      this._removeFromWalletOrderMap(order.sourceWalletAddress, orderId);
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
      this._removeFromOrderBook(orderId);
      this._askMap.delete(orderId);
      this._orderMap.delete(orderId);
      this._removeFromWalletOrderMap(order.sourceWalletAddress, orderId);
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

    let lastChainProcessedHeightInfo = this.lastProcessedHeightsInfo[order.sourceChain];
    let topChainHeight = lastChainProcessedHeightInfo.height;
    if (order.height > topChainHeight) {
      lastChainProcessedHeightInfo.height = order.height;
      lastChainProcessedHeightInfo.orderIds = new Set([order.id]);
    } else if (order.height < topChainHeight) {
      let error = new Error(
        `Could not process order with ID ${
          order.id
        } because it was below the last processed height of ${
          topChainHeight
        } for the chain ${order.sourceChain}`
      );
      error.name = 'HeightAlreadyProcessedError';
      throw error;
    } else {
      if (lastChainProcessedHeightInfo.orderIds.has(order.id)) {
        let error = new Error(
          `Could not process order with ID ${
            order.id
          } because it was already processed`
        );
        error.name = 'OrderAlreadyProcessedError';
        throw error;
      }
      lastChainProcessedHeightInfo.orderIds.add(order.id);
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

    let result = this._addToOrderBook(newOrder);

    result.makers.forEach((makerOrder) => {
      if (makerOrder.side === 'ask') {
        if (makerOrder.sizeRemaining <= 0) {
          this._askMap.delete(makerOrder.id);
          this._orderMap.delete(makerOrder.id);
          this._removeFromWalletOrderMap(makerOrder.sourceWalletAddress, makerOrder.id);
        }
      } else {
        if (makerOrder.valueRemaining <= 0) {
          this._bidMap.delete(makerOrder.id);
          this._orderMap.delete(makerOrder.id);
          this._removeFromWalletOrderMap(makerOrder.sourceWalletAddress, makerOrder.id);
        }
      }
    });

    if (newOrder.type !== 'market') {
      if (newOrder.side === 'ask') {
        if (result.taker.sizeRemaining > 0) {
          this._askMap.set(newOrder.id, newOrder);
          this._orderMap.set(newOrder.id, newOrder);
          this._addToWalletOrderMap(newOrder);
        }
      } else if (result.taker.valueRemaining > 0) {
        this._bidMap.set(newOrder.id, newOrder);
        this._orderMap.set(newOrder.id, newOrder);
        this._addToWalletOrderMap(newOrder);
      }
    }

    return result;
  }

  _addToWalletOrderMap(order) {
    if (!this._sourceWalletOrderMap.has(order.sourceWalletAddress)) {
      this._sourceWalletOrderMap.set(order.sourceWalletAddress, new Map());
    }
    let orderMap = this._sourceWalletOrderMap.get(order.sourceWalletAddress);
    orderMap.set(order.id, order);
  }

  _removeFromWalletOrderMap(sourceWalletAddress, orderId) {
    let orderMap = this._sourceWalletOrderMap.get(sourceWalletAddress);
    if (orderMap) {
      let result = orderMap.delete(orderId);
      if (!orderMap.size) {
        this._sourceWalletOrderMap.delete(sourceWalletAddress);
      }
      return result;
    }
    return false;
  }

  getSourceWalletOrderIterator(sourceWalletAddress) {
    let orderMap = this._sourceWalletOrderMap.get(sourceWalletAddress);
    return orderMap ? orderMap.values() : emptyGenerator();
  }

  *getSourceWalletBidIterator(sourceWalletAddress) {
    let orderIterator = this.getSourceWalletOrderIterator(sourceWalletAddress);
    for (let order of orderIterator) {
      if (order.side === 'bid') {
        yield order;
      }
    }
  }

  *getSourceWalletAskIterator(sourceWalletAddress) {
    let orderIterator = this.getSourceWalletOrderIterator(sourceWalletAddress);
    for (let order of orderIterator) {
      if (order.side === 'ask') {
        yield order;
      }
    }
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

    let result = this._removeFromOrderBook(orderId);
    if (order.side === 'ask') {
      this._askMap.delete(orderId);
    } else {
      this._bidMap.delete(orderId);
    }
    this._orderMap.delete(orderId);
    this._removeFromWalletOrderMap(order.sourceWalletAddress, orderId);
    return result;
  }

  peekBids() {
    return this.orderBook.getMaxBid();
  }

  peekAsks() {
    return this.orderBook.getMinAsk();
  }

  getBidIteratorFromMin() {
    return this.orderBook.getBidIteratorFromMin();
  }

  getBidIteratorFromMax() {
    return this.orderBook.getBidIteratorFromMax();
  }

  getAskIteratorFromMin() {
    return this.orderBook.getAskIteratorFromMin();
  }

  getAskIteratorFromMax() {
    return this.orderBook.getAskIteratorFromMax();
  }

  getBidLevelIteratorFromMin() {
    return this.orderBook.getBidLevelIteratorFromMin();
  }

  getBidLevelIteratorFromMax() {
    return this.orderBook.getBidLevelIteratorFromMax();
  }

  getAskLevelIteratorFromMin() {
    return this.orderBook.getAskLevelIteratorFromMin();
  }

  getAskLevelIteratorFromMax() {
    return this.orderBook.getAskLevelIteratorFromMax();
  }

  getOrderIterator() {
    return this._orderMap.values();
  }

  getBids() {
    return [...this.getBidIteratorFromMax()];
  }

  getAsks() {
    return [...this.getAskIteratorFromMin()];
  }

  getOrders() {
    return [...this.getOrderIterator()];
  }

  getSnapshot() {
    let askLimitOrders = this.getAsks().map(order => ({...order}));
    let bidLimitOrders = this.getBids().map(order => ({...order}));

    return {
      orderBookHash: this.orderBookHash,
      askLimitOrders,
      bidLimitOrders
    };
  }

  _addToOrderBook(order) {
    this.orderBookHash = this._sha1(`${this.orderBookHash}+${order.id}`);
    return this.orderBook.add(order);
  }

  _removeFromOrderBook(orderId) {
    this.orderBookHash = this._sha1(`${this.orderBookHash}-${orderId}`);
    return this.orderBook.remove(orderId);
  }

  setSnapshot(snapshot) {
    this.clear();
    snapshot.askLimitOrders.sort((a, b) => {
      if (a.expiryHeight > b.expiryHeight) {
        return 1;
      }
      if (a.expiryHeight < b.expiryHeight) {
        return -1;
      }
      return 0;
    });
    snapshot.bidLimitOrders.sort((a, b) => {
      if (a.expiryHeight > b.expiryHeight) {
        return 1;
      }
      if (a.expiryHeight < b.expiryHeight) {
        return -1;
      }
      return 0;
    });
    snapshot.askLimitOrders.forEach((order) => {
      let newOrder = {...order};
      this._addToOrderBook(newOrder);
      this._askMap.set(newOrder.id, newOrder);
      this._orderMap.set(newOrder.id, newOrder);
      this._addToWalletOrderMap(newOrder);
    });
    snapshot.bidLimitOrders.forEach((order) => {
      let newOrder = {...order};
      this._addToOrderBook(newOrder);
      this._bidMap.set(newOrder.id, newOrder);
      this._orderMap.set(newOrder.id, newOrder);
      this._addToWalletOrderMap(newOrder);
    });
    if (snapshot.orderBookHash) {
      this.orderBookHash = snapshot.orderBookHash
    }
  }

  clear() {
    this.orderBookHash = EMPTY_ORDER_BOOK_HASH;
    this._resetProcessedHeightsInfo();
    this._askMap.clear();
    this._bidMap.clear();
    this._orderMap.clear();
    this._sourceWalletOrderMap.clear();
    this.orderBook.clear();
  }
}

module.exports = TradeEngine;

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
    this.market = `${this.quoteCurrency}/${this.baseCurrency}`;
    this.orderBook = new LimitOrderBook();

    this._orderNodeLookup = {};
    this._expiryLinkedListHead = {prev: null, order: null};
    this._expiryLinkedListTail = {next: null, order: null};
    this._expiryLinkedListHead.next = this._expiryLinkedListTail;
    this._expiryLinkedListTail.prev = this._expiryLinkedListHead;
  }

  _addToExpiryList(order) {
    let newNode = {
      prev: this._expiryLinkedListTail.prev,
      next: this._expiryLinkedListTail,
      order
    };
    this._expiryLinkedListTail.prev = newNode;
    newNode.prev.next = newNode;
    this._orderNodeLookup[order.orderId] = newNode;
  }

  _removeFromExpiryList(orderId) {
    let node = this._orderNodeLookup[orderId];
    if (!node) {
      return;
    }
    delete this._orderNodeLookup[orderId];

    node.next.prev = node.prev;
    node.prev.next = node.next;

    node.prev = null;
    node.next = null;
  }

  _clearExpiryList() {
    this._orderNodeLookup = {};
    this._expiryLinkedListHead.next = this._expiryLinkedListTail;
    this._expiryLinkedListTail.prev = this._expiryLinkedListHead;
  }

  expireOrders(heightThreshold) {
    let currentNode = this._expiryLinkedListHead.next;
    let expiredOrders = [];
    while (currentNode && currentNode.order && currentNode.order.height < heightThreshold) {
      let orderId = currentNode.order.orderId;
      currentNode = currentNode.next;
      expiredOrders.push(currentNode.order);
      this._removeFromExpiryList(orderId);
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

    this._addToExpiryList(order);

    let newOrder = this._createOrderInstance(order);

    newOrder.type = order.type;
    newOrder.targetChain = order.targetChain;
    newOrder.targetWalletAddress = order.targetWalletAddress;
    newOrder.senderId = order.senderId;
    newOrder.sourceChain = order.sourceChain;
    newOrder.sourceChainAmount = order.sourceChainAmount;
    newOrder.sourceWalletAddress = order.sourceWalletAddress;
    newOrder.timestamp = order.timestamp;

    let result = this.orderBook.add(newOrder);

    result.makers.forEach((makerOrder) => {
      makerOrder.sizeTaken = makerOrder.lastSize == null ? makerOrder.size - makerOrder.sizeRemaining : makerOrder.lastSize - makerOrder.sizeRemaining;
      makerOrder.valueTaken = makerOrder.sizeTaken * makerOrder.price;
      makerOrder.lastSize = makerOrder.sizeRemaining;

      if (makerOrder.sizeRemaining <= 0) {
        this._removeFromExpiryList(order.orderId);
      }
    });
    return result;
  }

  getOrder(orderId) {
    let orderNode = this._orderNodeLookup[orderId];
    if (!orderNode) {
      return null;
    }
    return orderNode.order;
  }

  cancelOrder(orderId) {
    let orderNode = this._orderNodeLookup[orderId];
    if (!orderNode) {
      throw new Error(
        `An order with ID ${orderId} could not be found`
      );
    }
    let order = orderNode.order;

    let result = this.orderBook.remove(order.side, order.price, orderId);
    this._removeFromExpiryList(orderId);
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
    snapshot.askLimitOrders.forEach((order) => {
      this._addToExpiryList(order);
      let newOrder = this._createOrderInstance(order);
      Object.keys(order).forEach((key) => {
        newOrder[key] = order[key];
      });
      this.orderBook.askLimits.addOrder(newOrder);
    });
    snapshot.bidLimitOrders.forEach((order) => {
      this._addToExpiryList(order);
      let newOrder = this._createOrderInstance(order);
      Object.keys(order).forEach((key) => {
        newOrder[key] = order[key];
      });
      this.orderBook.bidLimits.addOrder(newOrder);
    });
  }

  clear() {
    this._clearExpiryList();
    this.orderBook.clear();
  }
}

module.exports = TradeEngine;

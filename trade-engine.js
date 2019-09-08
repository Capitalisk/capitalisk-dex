const LimitOrder = require('limit-order-book').LimitOrder;
const LimitOrderBook = require('limit-order-book').LimitOrderBook;

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
    this._orderNodeLookup[order.id] = newNode;
  }

  _removeFromExpiryList(orderId) {
    let node = this._orderNodeLookup[orderId];
    delete this._orderNodeLookup[orderId];

    node.next.prev = node.prev;
    node.prev.next = node.next;

    node.prev = null;
    node.next = null;
  }

  expireOrders(heightThreshold) {
    let currentNode = this._expiryLinkedListHead.next;
    let expiredOrders = [];
    while (currentNode && currentNode.order && currentNode.order.height < heightThreshold) {
      let orderId = currentNode.order.id;
      currentNode = currentNode.next;
      expiredOrders.push(currentNode.order);
      this._removeFromExpiryList(orderId);
    }
    return expiredOrders;
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

    this._addToExpiryList(order);

    let limitOrder = new LimitOrder(order.id, order.side, order.price, order.size);
    limitOrder.targetChain = order.targetChain;
    limitOrder.targetWalletAddress = order.targetWalletAddress;
    limitOrder.senderId = order.senderId;
    let result = this.orderBook.add(limitOrder);

    result.makers.forEach((makerOrder) => {
      makerOrder.valueTaken = makerOrder.valueRemoved;
      makerOrder.sizeTaken = makerOrder.size - makerOrder.sizeRemaining;

      // These need to be reset for the next time or else they will accumulate.
      makerOrder.valueRemoved = 0;
      makerOrder.size = makerOrder.sizeRemaining;

      if (makerOrder.sizeRemaining <= 0) {
        this._removeFromExpiryList(order.id);
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
}

module.exports = TradeEngine;

# lisk-dex
Decentralized exchange module for the Lisk network.

## DEX protocol

These status codes and messages appear in transactions created by the DEX (as part of the transaction data field):

### Refunds:

- r1,${orderId}: Invalid order
- r2,${orderId}: Expired order
- r3,${orderId}: Canceled order
- r4,${orderId}: Unmatched market order part

### Trades:

- t1,${takerChain},${takerOrderId}: Orders taken
- t2,${makerChain},${makerOrderId},${takerOrderId}: Order made

### Moved:

- m1,${walletAddress}: DEX has moved

### Stopped operating:

- d1: DEX has been disabled

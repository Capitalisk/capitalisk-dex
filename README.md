# lisk-dex
Decentralized exchange module for the Lisk network.

## DEX protocol

These status codes and messages appear in transactions created by the DEX (as part of the transaction data field):

### Refunds:

- r1,${orderId}: Invalid order
- r2,${orderId}: Expired order
- r3,${orderId},${cancelOrderId}: Canceled order
- r4,${orderId}: Unmatched market order part

### Trades:

- t1,${takerChain},${takerOrderId}: Orders taken
- t2,${makerChain},${makerOrderId},${takerOrderId}: Order made

### Moved DEX:

- m1,${walletAddress}: DEX has moved

### Disabled DEX:

- d1: DEX has been disabled

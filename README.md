# lisk-dex
Decentralized exchange module for the Lisk network. Note that lisk-dex is a community project and is not affiliated with Lightcurve.

## DEX protocol

These status codes and messages appear in transactions created by the DEX (as part of the transaction data field):

### Refunds:

- r1,${orderId}: Invalid order
- r2,${orderId}: Expired order
- r3,${orderId},${cancelOrderId}: Canceled order
- r4,${orderId}: Unmatched market order part
- r5,${orderId},${newWalletAddress}: DEX has moved
- r6,${orderId}: DEX has been disabled

### Trades:

- t1,${takerChain},${takerOrderId}: Orders taken
- t2,${makerChain},${makerOrderId},${takerOrderId}: Order made

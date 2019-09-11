# lisk-dex
Decentralized exchange module for the Lisk network.

## DEX protocol

These status codes and messages appear in transactions created by the DEX (as part of the transaction data field):

### Refunds:

- r1: Invalid order ${orderId}
- r2: Expired order ${orderId}
- r3: Canceled order ${orderId}
- r4: Unmatched market order part ${orderId}

### Trades:

- t1: Matched some orders on chain ${sourceChain}
- t2: Matched order ${orderId} on chain ${sourceChain}

### Moved:

- m1,4076631634347315024L: DEX has moved to a new address

### Stopped operating:

- s1: DEX has stopped operating

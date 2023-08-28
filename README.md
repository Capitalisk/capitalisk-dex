# capitalisk-dex
Decentralized exchange module for the Capitalisk network.

## DEX protocol

### Actions

To send an order to the DEX, a user needs to send a regular transfer transaction to the DEX's multisignature wallet address with one of the following commands in the transaction's `data` field.

- **Limit order**: `${targetChain},limit,${bidOrAskPrice},${targetWalletAddress}`
- **Market order**: `${targetChain},market,${targetWalletAddress}`
- **Close order**: `${targetChain},close,${orderId}`
- **Credit**: `credit`

### Parameters
- **targetChain** is the symbol of the opposite blockchain within a given market (e.g. LSH)
- **bidOrAskPrice** is a limit order price.
- **targetWalletAddress** is the wallet address on the opposite blockchain where tokens should be sent to.
- **orderId** is the order ID (which matches the blockchain transaction ID).

When making a limit or a market order, the DEX will use the amount of the underlying transaction to calculate the quantity of counterparty tokens to acquire.
When performing a close order, the amount is not relevant; in this case, any amount can be specified as part of the close transaction (less is better); in any case, whatever amount is specified (minus blockchain transaction fees) will be refunded to the user's wallet via an `r3` refund transaction.
The credit action allows users to send tokens to the DEX wallet without triggering any operation.

### Responses

These status codes and messages appear in transactions created by the DEX (as part of the transaction `data` field):

**Refunds**

- `r1,${orderId}: Invalid order`
- `r2,${orderId}: Expired order`
- `r3,${orderId},${closeOrderId}: Closed order`
- `r4,${orderId}: Unmatched market order part`
- `r5,${orderId},${newWalletAddress}: DEX has moved`
- `r6,${orderId}: DEX has been disabled`

**Trades**

- `t1,${takerChain},${takerOrderId},${makerOrderCount}: Orders taken`
- `t2,${makerChain},${makerOrderId},${takerOrderId}: Order made`

  - Taker is the account/wallet which takes the trade from someone else (from the maker).
  - Maker provides tokens for someone else to take (to the taker).
  - All market orders are takers.
  - Pending limit orders in the order book will be makers.
  - Limit orders can be makers or takers or both.
    1. If a limit order is filled immediately as soon as placed, it is a taker.
    2. If a limit order goes into pending state after placed, it will be a maker.
    3. If a limit order is partially filled after it is placed, it is a taker; then the unfilled portion will be added to the order book and will be a maker.
  - Takers are always matched against pending limit orders in the order book.
  - t1 transaction represents money going into taker's wallet.
  - t2 transaction represents money going into maker's wallet.
  - t1 has **makerOrderCount** denoting number of makers it matched against from a single taker order.
  - t2 is an individual maker transaction matched against a single taker order **takerOrderId**.
  - For both market and limit orders there is always **1 t1** and **1-N t2** transactions.

**Dividends**

- `d1,${fromHeight},${toHeight}: Member dividend`

### Behaviors

- If the DEX does not recognize a command/order from a user (or it is invalid for whatever reason), it will send an `r1` refund transaction back to the user's wallet address which will return the full amount of the original transaction minus any blockchain transaction fees incurred by the DEX.
- A DEX adheres to a fixed order expiry. If an order expires before being filled or closed, the DEX will send an `r2` refund transaction back to the user's wallet address which will return the unfilled portion of the original transaction minus any blockchain transaction fees incurred by the DEX.
- If a pending limit order is closed by a user using a `close` action, the unfilled portion of the original order transaction amount (minus blockchain transaction fees) will be refunded back to the user's wallet address using an `r3` refund transaction. A user may only close their own orders.
- If a market order is made which cannot be completely filled by counterparty limit orders, then any unmatched part of the market order (minus blockchain transaction fees) will be refunded back to the user's wallet address as an `r4` refund transaction.
- If the majority of DEX operators have agreed to move the DEX to a new multisig wallet address, the DEX will issue a full refund (minus blockchain transaction fees) for every pending order and also every new order which is sent to the DEX wallet address thereafter via `r5` transactions. The DEX should keep refunding all transactions that are sent to the old address for at least 6 months in order to give clients enough time to update their caches to point to the new address.
- If the majority of DEX operators have agreed to shut down the DEX, the DEX will issue a full refund (minus blockchain transaction fees) for every pending order and also every new order which is sent to the DEX wallet address thereafter via `r6` transactions. The DEX should keep refunding transactions sent to the last active address for at least 6 months to give clients enough time to update their caches to point to a different DEX. In practice, a DEX should not shut down because it does not align with financial incentives and it requires a high degree of coordination between members but this refund type exists anyway to account for unusual scenarios and use cases.
- In addition to basic blockchain fees, a DEX can charge an exchange fee as a percentage of the order value. All DEX members/nodes need to agree on the same percentage fee.

### Short protocol

- For trades and refunds, only the first part of message is mandatory, the part which begins with the column character is optional depending on the market implementation.
- If one of the blockchains involved in a market does not provide sufficient space in a transaction to store a full protocol message, order IDs and wallet addresses may be trimmed down to fit within the available space.

### Scope

This project is intended to be simple. Fewer lines of code and fewer dependencies means more security.
If additional features want to be added beyond the current feature set (e.g. such as loans, NFTs, liquidity pools and alternative matching engines), this project should be forked into a separate project 'distro'.

### Sponsors

Special thanks to Carolina delegate for being an early sponsor of this project.

## License

This product is issued under the AGPL v3 license with a potential copyleft exemption.

### AGPL by default

By default, this product is issued under `AGPL-3.0`.

### Copyleft exemption for CLSK token holders

If you own `100K CLSK` tokens (https://capitalisk.com/), then you are exempt from the AGPL copyleft requirement of making the code of your derived projects public. This exemption applies automatically from the moment that you acquire 100K or more CLSK tokens and it is valid so long as you continue to hold that amount of tokens.

If your CLSK balance falls below 100K, then you will be once again bound to the copyleft conditions of AGPL-3.0 after a grace period of 90 days; after this grace period, your derived project's code must be made public. Timestamps which can be used to prove ownership of CLSK tokens over time are recorded on the Capitalisk blockchain in a decentralized, immutable way so it is important that you hold 100K CLSK throughout your derived project's entire commercial life if you intend to keep the code private.

This exemption also applies to companies; in this case, the total CLSK holdings of the company plus those of its directors and board members must be greater than 100K multiplied by the maximum number of contributors which have worked on the project concurrently since the start of the project (e.g. according to records in the project's code repository). If a company falls out of compliance, the standard 90-day grace period applies before reverting back to the AGPL-3.0 license.

Derived code may optionally be distributed with the same copyleft exemption clause as this project; in this case, the CLSK token ownership requirement must be at least as high as this project.

The amount of CLSK tokens which need to be held to qualify for the copyleft exemption may be revised downwards in the future but never upwards.

# Polymarket Arb Bot — Core Mathematical Stack

This arbitrage bot does not simply "buy both sides" blindly. It employs a rigorous, institutional-grade quantitative stack that estimates edge, sizes positions, and dynamically manages inventory risk in real time. 

The trading loop executes through three distinct mathematical models for every single tick:

### 1. Bayesian Probability Updating
*Used to predict the true probability faster than the market updates.*
**Formula**: `P(H|D) = P(D|H) · P(H) / P(D)`

- **Implementation (`src/bayesian.rs`)**: On every Binance tick, the bot computes the logistic probability of the asset reaching the Polymarket strike. It then uses Bayesian recursive updating to mix this new empirical datum `P(D|H)` with its prior belief `P(H)`.
- **Why**: This drastically reduces latency noise from the CEX price while cleanly isolating the true probability drift.

### 2. Stoikov Inventory Management
*Used to manage quoting and inventory risk.*
**Formula**: `r = s - qγσ²(T - t)`

- **Implementation (`src/stoikov.rs`)**: `s` is the fair mid-price. `q` is our current long/short inventory. `γ` is our required risk aversion. `σ²` is the real-time calculated asset variance. `(T-t)` is time to expiry.
- **Why**: As we accumulate a position on Polymarket, our risk increases. The Stoikov model dynamically skews our "reservation price" (`r`) away from the mid price to demand a higher edge before we are willing to take on more of the same exposure.

### 3. Fractional Kelly Criterion
*Used to mathematically optimize position sizing.*
**Formula**: `f* = ((b · p) - q) / b`

- **Implementation (`src/kelly.rs`)**: `p` is our Bayesian posterior probability. `b` is the net odds offered by Polymarket. `q` is `1 - p`.
- **Why**: Even with an edge, betting too much causes ruin. The Kelly formula calculates the exact fraction of the portfolio to risk to maximize long-term geometric growth without risking bankruptcy. We apply a fractional multiplier (`0.5`) to smooth variance further.

---
### Real-time Architecture ("The Quant Path")
When the fast `SNIPER` path isn't triggered by raw latency, the bot evaluates the market through this exact pipeline:
1. Updates **Bayesian** posterior `P(H|D)`
2. Derives fair reservation price `r` from **Stoikov**
3. If Polymarket's entry price is cheaper than `r`, it allocates `f*` capital via **Kelly**.

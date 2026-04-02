# Signal Backtesting Results

## SPREAD_DISLOCATION
*Metric: Brent-WTI Spread (USD)*

| Horizon | Events | Hit rate | Avg magnitude when correct | Avg magnitude when incorrect | Best case | Worst case |
|---|---|---|---|---|---|---|
| 10 | 11 | 36.4% | +1.25 | +2.39 | -1.97 | +12.08 |
| 20 | 10 | 40.0% | +0.99 | +1.60 | -1.85 | +5.90 |
| 40 | 9 | 55.6% | +0.88 | +1.24 | -2.15 | +1.55 |

**Interpretation:** Brent-WTI Spread had 3 evaluable horizon sets after clustering. The average hit rate across 10/20/40 day horizons was 44.0%, so the signal did not show strong historical consistency.

## VOLATILITY_SPIKE
*Metric: Brent 30-Day Volatility (annualised vol)*

| Horizon | Events | Hit rate | Avg magnitude when correct | Avg magnitude when incorrect | Best case | Worst case |
|---|---|---|---|---|---|---|
| 10 | 1 | 0.0% | N/A | +16.16% | +16.16% | +16.16% |
| 20 | 0 | N/A | N/A | N/A | N/A | N/A |
| 40 | 0 | N/A | N/A | N/A | N/A | N/A |

**Interpretation:** Brent 30-Day Volatility had 1 evaluable horizon sets after clustering. The average hit rate across 10/20/40 day horizons was 0.0%, so the signal did not show strong historical consistency.

## CORRELATION_BREAKDOWN
*Metric: Brent vs WDS.AX Correlation (correlation)*

| Horizon | Events | Hit rate | Avg magnitude when correct | Avg magnitude when incorrect | Best case | Worst case |
|---|---|---|---|---|---|---|
| 10 | 7 | 28.6% | +0.440 | +0.241 | -0.409 | +0.449 |
| 20 | 7 | 42.9% | +0.564 | +0.377 | -0.525 | +0.727 |
| 40 | 7 | 71.4% | +0.422 | +0.334 | -0.234 | +0.650 |

**Interpretation:** Brent vs WDS.AX Correlation had 3 evaluable horizon sets after clustering. The average hit rate across 10/20/40 day horizons was 47.6%, so the signal did not show strong historical consistency.

## FX_DISLOCATION
*Metric: AUD/USD Z-score (z-score)*

| Horizon | Events | Hit rate | Avg magnitude when correct | Avg magnitude when incorrect | Best case | Worst case |
|---|---|---|---|---|---|---|
| 10 | 12 | 75.0% | +1.041 | +0.271 | -0.489 | +1.860 |
| 20 | 12 | 91.7% | +0.859 | +0.954 | -0.954 | +1.718 |
| 40 | 11 | 90.9% | +1.118 | +0.357 | -0.357 | +2.089 |

**Interpretation:** AUD/USD Z-score had 3 evaluable horizon sets after clustering. The average hit rate across 10/20/40 day horizons was 85.9%, so the signal historically produced a positive signal.

## Limitations

- Short data history and low event counts may make hit rates unstable.
- The analysis measures metric outcomes, not tradable profit and loss, and excludes transaction costs.
- We apply a 10-day cooldown to reduce double counting, but clustering can still bias results toward regimes.
- The event study uses future price and rolling metric series; it does not model execution, carry, or liquidity risk.
- Some events are excluded if there are insufficient pre- or post-event trading days.

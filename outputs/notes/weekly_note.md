# Weekly Desk Note

Date: 2026-04-01

## Weekly Returns
- AUDUSD=X: -2.24%
- BZ=F: 12.85%
- CL=F: 16.74%
- STO.AX: 0.00%
- WDS.AX: 1.24%

## Spread Overview

- Latest Brent-WTI spread: 9.90 USD
- 90-day mean spread: 5.26 USD
- 90-day spread standard deviation: 2.48 USD
- Current spread z-score: 1.87

## Volatility Regime

- BZ=F: latest 73.59% vs average 31.14% -> elevated
- WDS.AX: latest 35.08% vs average 23.53% -> elevated
- STO.AX: latest 32.25% vs average 26.28% -> elevated

## Correlation Update

- Brent vs WDS.AX: 0.96
- Brent vs STO.AX: 0.94

## Signals in the Last 7 Days

- 2026-03-30: VOLATILITY_SPIKE (0.736) | BZ=F=112.7800, CL=F=102.8800, AUDUSD=X=0.6851, WDS.AX=35.2200, STO.AX=8.0500
- 2026-03-27: VOLATILITY_SPIKE (0.736) | BZ=F=112.5700, CL=F=99.6400, AUDUSD=X=0.6887, WDS.AX=34.4700, STO.AX=7.9500
- 2026-03-27: SPREAD_DISLOCATION (3.184) | BZ=F=112.5700, CL=F=99.6400, AUDUSD=X=0.6887, WDS.AX=34.4700, STO.AX=7.9500
- 2026-03-26: VOLATILITY_SPIKE (0.743) | BZ=F=108.0100, CL=F=94.4800, AUDUSD=X=0.6943, WDS.AX=34.3800, STO.AX=7.8500
- 2026-03-26: SPREAD_DISLOCATION (3.685) | BZ=F=108.0100, CL=F=94.4800, AUDUSD=X=0.6943, WDS.AX=34.3800, STO.AX=7.8500
- 2026-03-25: VOLATILITY_SPIKE (0.733) | BZ=F=102.2200, CL=F=90.3200, AUDUSD=X=0.6997, WDS.AX=33.6200, STO.AX=7.6600
- 2026-03-25: SPREAD_DISLOCATION (3.276) | BZ=F=102.2200, CL=F=90.3200, AUDUSD=X=0.6997, WDS.AX=33.6200, STO.AX=7.6600

## Notes

- Monitor Brent-WTI spread versus the 90-day mean and volatility regime signals.
- Check AUD/USD dislocations for AUD-denominated energy equity exposure.

## Backtest Summary

- SPREAD_DISLOCATION: 10d 36.4%; 20d 40.0%; 40d 55.6%
- VOLATILITY_SPIKE: 10d 0.0%
- CORRELATION_BREAKDOWN: 10d 28.6%; 20d 42.9%; 40d 71.4%
- FX_DISLOCATION: 10d 75.0%; 20d 91.7%; 40d 90.9%

### Backtest Limitations

- Short data history and low event counts may make hit rates unstable.
- The analysis measures metric outcomes, not tradable profit and loss, and excludes transaction costs.
- We apply a 10-day cooldown to reduce double counting, but clustering can still bias results toward regimes.
- The event study uses future price and rolling metric series; it does not model execution, carry, or liquidity risk.
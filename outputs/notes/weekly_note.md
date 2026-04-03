# Weekly Desk Note

Date: 2026-04-03

## Weekly Returns
- AUDUSD=X: 0.38%
- BZ=F: -0.27%
- CL=F: 12.61%
- STO.AX: 2.93%
- WDS.AX: 1.60%

## Spread Overview

- Latest Brent-WTI spread: 1.33 USD
- 90-day mean spread: 5.33 USD
- 90-day spread standard deviation: 2.83 USD
- Current spread z-score: -1.42

## Volatility Regime

- BZ=F: latest 89.38% vs average 31.66% -> elevated
- WDS.AX: latest 35.28% vs average 23.56% -> elevated
- STO.AX: latest 31.94% vs average 26.28% -> elevated

## Correlation Update

- Brent vs WDS.AX: 0.96
- Brent vs STO.AX: 0.94

## Signals in the Last 7 Days

- 2026-04-02: VOLATILITY_SPIKE (0.894) | BZ=F=107.7200, CL=F=106.3900, AUDUSD=X=0.6879, WDS.AX=34.9300, STO.AX=8.0800
- 2026-04-01: VOLATILITY_SPIKE (0.886) | BZ=F=101.1600, CL=F=100.1200, AUDUSD=X=0.6921, WDS.AX=35.0900, STO.AX=7.9700
- 2026-03-31: SPREAD_DISLOCATION (4.190) | BZ=F=118.3500, CL=F=101.3800, AUDUSD=X=0.6846, WDS.AX=35.0500, STO.AX=7.9600
- 2026-03-30: VOLATILITY_SPIKE (0.736) | BZ=F=112.7800, CL=F=102.8800, AUDUSD=X=0.6851, WDS.AX=35.2200, STO.AX=8.0500
- 2026-03-27: VOLATILITY_SPIKE (0.736) | BZ=F=112.5700, CL=F=99.6400, AUDUSD=X=0.6887, WDS.AX=34.4700, STO.AX=7.9500
- 2026-03-27: SPREAD_DISLOCATION (3.184) | BZ=F=112.5700, CL=F=99.6400, AUDUSD=X=0.6887, WDS.AX=34.4700, STO.AX=7.9500

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
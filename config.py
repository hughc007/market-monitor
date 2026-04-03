from datetime import date, timedelta

# NOTE: BZ=F is the Brent front month futures contract. Near expiry the price diverges from the active contract.
# Use auto_adjust=True to handle rollovers automatically when downloading via yfinance.
INSTRUMENTS = {
    "BZ=F": {
        "display_name": "Brent Crude",
        "asset_class": "commodity",
    },
    "CL=F": {
        "display_name": "WTI Crude",
        "asset_class": "commodity",
    },
    "AUDUSD=X": {
        "display_name": "AUD/USD",
        "asset_class": "fx",
    },
    "WDS.AX": {
        "display_name": "Woodside Energy",
        "asset_class": "equity",
    },
    "STO.AX": {
        "display_name": "Santos",
        "asset_class": "equity",
    },
}

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=365 * 2)

SPREAD_ZSCORE_THRESHOLD = 2.0
VOL_SPIKE_MULTIPLIER = 1.5
CORR_BREAKDOWN_THRESHOLD = 0.3
FX_ZSCORE_THRESHOLD = 2.0

ROLLING_VOL_WINDOW = 30
ROLLING_CORR_WINDOW = 60
SPREAD_MEAN_WINDOW = 90

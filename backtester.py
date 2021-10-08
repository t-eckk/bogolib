#%%
import numpy as np
import pandas as pd 


from typing import List

from pandas.core.frame import DataFrame

class Backtester:

    def __init__(self, prices: DataFrame, signals: DataFrame, transaction_costs = 0.0006) -> None:
        self.prices = prices
        self.signals = signals
        self.transaction_costs = transaction_costs
        self.backtest = pd.DataFrame(index = prices.index, columns = prices.columns)
    
    def _initiate_backtest(self):
        arrays = [

        ]
# %%

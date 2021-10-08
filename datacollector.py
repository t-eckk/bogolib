#%%
import ccxt

import datetime as dt
import pandas as pd 
from collections import deque

from typing import Any, List, Deque
from pandas.core.frame import DataFrame 
from ccxt.base.exchange import Exchange


class DataCollector:

    def __init__(self, exchange: str) -> None:
        self.exchange = self._select_exchange(exchange=exchange)

    def switch_exchange(self, exchange: str) -> None:
        self.exchange = self._select_exchange(exchange=exchange)

    def _select_exchange(self, exchange: str) -> Exchange:
        exchange = exchange.upper()
        if   exchange == 'FTX':
            ccxt_exchange = ccxt.ftx()
        elif exchange == 'BINANCE':
            ccxt_exchange = ccxt.binance()
        elif exchange == 'BINANCEUSDM':
            ccxt_exchange = ccxt.binanceusdm()
        else:
            pass    #TODO: add exchanges
        return ccxt_exchange

    def get_markets(self, futures_only: bool = False, perps_only: bool = False) -> DataFrame:
        """Request available markets from exchange.

        Args:
            futures_only (bool, optional): Defaults to False.
            perps_only (bool, optional): Defaults to False.

        Returns:
            DataFrame: DataFrame including main markets data
        """

        markets = self.exchange.fetch_markets()
        timestamp = self.exchange.seconds()

        out = [{}] * len(markets)
        for i, market in enumerate(markets):         
            dict_ = {}
            try:
                dict_['timestamp'] = timestamp
                dict_['id'] = market['id']
                dict_['base'] = market['base']
                dict_['type'] = market['type']
                dict_['isPerp'] = True if 'PERP' in dict_['id'] and dict_['type'] == 'future' else False
                dict_['amount_precision'] = market['precision']['amount']
                dict_['price_precision'] = market['precision']['price']
                dict_['min_amount'] = market['limits']['amount']['min']
                dict_['bid'] = float(market['info']['bid'])
                dict_['ask'] = float(market['info']['ask'])
                dict_['price'] = float(market['info']['price'])
                dict_['change1h'] = float(market['info']['change1h'])
                dict_['change24h'] = float(market['info']['change24h'])
                dict_['changeBod'] = float(market['info']['changeBod'])
                dict_['volumeUsd24h'] = float(market['info']['volumeUsd24h'])
                out[i] = dict_
            except:
                print(f"Failed to import {market['id']}")

        df = pd.DataFrame(out)
        df.index = df['id']
        df.drop(['id'], axis=1, inplace=True)
        
        if perps_only == True:
            df = df[df['isPerp'] == True]
        elif futures_only == True:
            df = df[df['type'] == 'future']       
        return df

    def _time_to_timestamp(self, date: str) -> int:
        """Converts a date (string format) to timestamp.

        Args:
            date (str): YYYY-MM-DD

        Returns:
            int: Timestamp
        """
        return dt.datetime.fromisoformat(date).timestamp() * 10**3

    def _get_history(self, symbol: int, timeframe: str, start: str = None, request: Deque = deque(), convert_timestamp=False) -> DataFrame:
        """[summary]

        Args:
            symbol (int): [description]
            timeframe (str): [description]
            start (str, optional): [description]. Defaults to None.
            request (Deque, optional): [description]. Defaults to deque().

        Returns:
            DataFrame: [description]
        """
        if timeframe == '1d' or timeframe == '1wk':
            now = (dt.datetime.now().timestamp() - 3600 * 24) * 10**3
        else:
            now = (dt.datetime.now().timestamp() - 3600) * 10**3    # Offset by 3600 seconds 
        
        if start is None or start < now:
            ohlcv = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=start)
            ohlcv = ohlcv[::-1]
            request.append(ohlcv)
            start = ohlcv[0][0]
            return self._get_history(symbol, timeframe, start, request)
        else:
            out = []
            for subrequest in request:
                for element in subrequest:
                    out.append(element)

            # Pandas MultiIndex
            arrays = [
                [symbol] * 6,
                ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            ]
            tuples = list(zip(*arrays))
            index  = pd.MultiIndex.from_tuples(tuples, name=['symbol', 'OHLCV'])
            
            # Set up Pandas Dataframe
            out = pd.DataFrame(out, columns=index)
            out.index = pd.to_datetime(out[symbol]['timestamp'], unit='ms')
            out.drop((symbol, 'timestamp'), axis=1, inplace=True)
            out =  out[~out.index.duplicated(keep='first')]
            return out.sort_index()

    
    def get_history(self, symbols: List[str], timeframe: str, start: str = None) -> DataFrame:
        """Downloads OHLCV history from exchange.

        Args:
            symbols (str | List[str]): Symbol or list of symbols
            timeframe (str): Data frequency
            start (str, optional): Start date (YYYY-MM-DD). Defaults to None.

        Returns:
            DataFrame: Multi-level dataframe [Symbol;OHLCV]
        """
        start = self._time_to_timestamp(start) if start is not None else None
        if type(symbols) == str:
            out = self._get_history(symbol=symbols, timeframe=timeframe, start=start, request=deque())
        else:
            
            for symbol in symbols:
                try:
                    out = out.join(
                            self._get_history(symbol=symbol, timeframe=timeframe, start=start, request=deque())
                        )
                except UnboundLocalError:
                    out =   self._get_history(symbol=symbol, timeframe=timeframe, start=start, request=deque())
                    
        return out

    def get_topvolumes(self, n: int, futures_only: bool = False, perps_only: bool = False) -> List:
        markets = self.get_markets(futures_only=futures_only, perps_only=perps_only)

        return list(markets.sort_values('volumeUsd24h', ascending=False).index[:n])
# %%

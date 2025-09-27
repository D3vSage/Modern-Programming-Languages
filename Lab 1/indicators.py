import pandas as pd

class TechnicalIndicators:
    def __init__(self, df):
        self.df = df
    
    def calculate_sma(self, periods=[5, 10, 15, 30]):
        """Расчет простых скользящих средних"""
        for period in periods:
            self.df[f'SMA_{period}'] = self.df['Close'].rolling(period).mean().shift()
        return self.df
    
    def calculate_ema(self, periods=[9, 22]):
        """Расчет экспоненциальных скользящих средних"""
        for period in periods:
            self.df[f'EMA_{period}'] = self.df['Close'].ewm(period).mean().shift()
        return self.df
    
    def calculate_rsi(self, n=14):
        """Расчет индекса относительной силы"""
        close = self.df['Close']
        delta = close.diff()
        delta = delta[1:]
        pricesUp = delta.copy()
        pricesDown = delta.copy()
        pricesUp[pricesUp < 0] = 0
        pricesDown[pricesDown > 0] = 0
        rollUp = pricesUp.rolling(n).mean()
        rollDown = pricesDown.abs().rolling(n).mean()
        rs = rollUp / rollDown
        rsi = 100.0 - (100.0 / (1.0 + rs))
        self.df['RSI'] = rsi.fillna(0)
        return self.df
    
    def calculate_macd(self):
        """Расчет MACD и сигнальной линии"""
        EMA_12 = self.df['Close'].ewm(span=12, min_periods=12).mean()
        EMA_26 = self.df['Close'].ewm(span=26, min_periods=26).mean()
        self.df['MACD'] = EMA_12 - EMA_26
        self.df['MACD_signal'] = self.df['MACD'].ewm(span=9, min_periods=9).mean()
        return self.df
    
    def calculate_all_indicators(self):
        """Расчет всех индикаторов"""
        self.calculate_sma()
        self.calculate_ema()
        self.calculate_rsi()
        self.calculate_macd()
        return self.df
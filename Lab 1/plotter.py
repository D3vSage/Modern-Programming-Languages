import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

class Plotter:
    def __init__(self, df):
        self.df = df
    
    def time_series(self, column='Close', title='Временной ряд'):
        """Построение временного ряда с ползунком"""
        fig = px.line(self.df, x='Date', y=column, title=title)
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        return fig
    
    def volume_chart(self):
        """Столбчатая диаграмма объемов торгов"""
        fig = go.Figure()
        fig.add_trace(go.Bar(x=self.df['Date'], y=self.df['Volume'], marker_color='blue'))
        fig.update_xaxes(rangeslider_visible=True)
        fig.update_layout(title='Объемы торгов TSLA')
        return fig
    
    def candlestick(self, increasing_color='blue', decreasing_color='orange'):
        """График свечей"""
        fig = go.Figure(data=[go.Candlestick(
            x=self.df['Date'],
            open=self.df['Open'],
            high=self.df['High'],
            low=self.df['Low'],
            close=self.df['Close'],
            increasing_line_color=increasing_color,
            decreasing_line_color=decreasing_color
        )])
        fig.update_layout(title='График свечей TSLA')
        return fig
    
    def ohlc_with_volume(self):
        """График OHLC с объемами"""
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                           subplot_titles=('Цены', 'Объемы'),
                           vertical_spacing=0.1)
        
        fig.add_trace(go.Ohlc(x=self.df['Date'], open=self.df['Open'],
                             high=self.df['High'], low=self.df['Low'],
                             close=self.df['Close'], name='Цена'),
                     row=1, col=1)
        
        fig.add_trace(go.Bar(x=self.df['Date'], y=self.df['Volume'],
                            name='Объем', marker_color='blue'),
                     row=2, col=1)
        
        fig.update(layout_xaxis_rangeslider_visible=False)
        fig.update_layout(title='График OHLC с объемами торгов')
        return fig
    
    def moving_averages(self):
        """График скользящих средних"""
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['SMA_5'], name='SMA 5'))
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['SMA_10'], name='SMA 10'))
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['SMA_15'], name='SMA 15'))
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['SMA_30'], name='SMA 30'))
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['Close'], name='Close', opacity=0.3))
        fig.update_layout(title='Скользящие средние')
        fig.update_xaxes(rangeslider_visible=True)
        return fig
    
    def rsi_analysis(self):
        """Анализ RSI с ценой и объемами"""
        fig = make_subplots(rows=3, cols=1, 
                           subplot_titles=('Цена закрытия', 'RSI', 'Объемы'),
                           vertical_spacing=0.05)
        
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['Close'], 
                               name='Цена'), row=1, col=1)
        fig.add_trace(go.Scatter(x=self.df['Date'], y=self.df['RSI'], 
                               name='RSI'), row=2, col=1)
        fig.add_trace(go.Bar(x=self.df['Date'], y=self.df['Volume'], 
                           name='Объем'), row=3, col=1)
        
        # Добавляем горизонтальные линии для RSI
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        
        fig.update_layout(title='Анализ RSI', height=800)
        return fig
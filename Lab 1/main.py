from data_loader import DataLoader
from indicators import TechnicalIndicators
from plotter import Plotter

def main():
    # Загрузка данных
    print("Загрузка данных...")
    data_loader = DataLoader()
    df = data_loader.load_data()
    
    # Расчет индикаторов
    print("Расчет технических индикаторов...")
    indicators = TechnicalIndicators(df)
    df_with_indicators = indicators.calculate_all_indicators()
    
    # Создание графиков
    print("Создание графиков...")
    plotter = Plotter(df_with_indicators)
    
    # Построение различных графиков
    figures = [
        plotter.time_series('Close', 'Цена закрытия TSLA'),
        plotter.volume_chart(),
        plotter.candlestick(),
        plotter.ohlc_with_volume(),
        plotter.moving_averages(),
        plotter.rsi_analysis()
    ]
    
    # Отображение графиков
    for i, fig in enumerate(figures, 1):
        print(f"Отображение графика {i} из {len(figures)}")
        fig.show()
    
    print("Анализ завершен!")

if __name__ == "__main__":
    main()
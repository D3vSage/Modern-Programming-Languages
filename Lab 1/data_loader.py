import pandas as pd

class DataLoader:
    def __init__(self, file_path="./TSLA.csv"):
        self.file_path = file_path
        self.df = None
    
    def load_data(self):
        """Загрузка и предобработка данных"""
        self.df = pd.read_csv(self.file_path)
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        self.df.index = range(len(self.df))
        return self.df
    
    def get_data(self):
        """Получение загруженных данных"""
        return self.df
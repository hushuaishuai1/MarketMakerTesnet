import unittest
from ML.data_processing import prepare_dataset, label_data
from ML.model_train import train_model
import pandas as pd
import numpy as np

class TestMLStrategy(unittest.TestCase):

    def test_prepare_dataset(self):
        df = prepare_dataset('BTCUSDT', '5m', 500)
        self.assertFalse(df.empty)
        self.assertIn('ema_12', df.columns)
        self.assertIn('macd', df.columns)

    def test_label_data(self):
        df = prepare_dataset('BTCUSDT', '5m', 500)
        df = label_data(df)
        self.assertIn('signal', df.columns)
        self.assertFalse(df['signal'].isnull().any())

    def test_train_model(self):
        df = prepare_dataset('BTCUSDT', '5m', 500)
        df = label_data(df)
        model = train_model('BTCUSDT', '5m', 500)
        self.assertIsNotNone(model)
        self.assertTrue(hasattr(model, 'predict'))

if __name__ == '__main__':
    unittest.main()


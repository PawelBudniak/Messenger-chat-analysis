import unittest
import sys
sys.path.append('../')
from who_starts_conversations import count_conversation_starters
import pandas as pd
import datetime
from datetime import timedelta

class TestConversationCounter(unittest.TestCase):

    def setUp(self):
        self.newest = datetime.datetime(2020, 1, 1, 0, 0, 0)

    def test_count_conversations(self):
        # Define the test data
        data = {'sender_name': ['Sender1', 'Sender2', 'Sender2'],
                'timestamp_ms': [
                    self.newest,
                    self.newest - timedelta(minutes=1),
                    self.newest - timedelta(minutes=2),
                ]}
        df = pd.DataFrame(data)
        df['timestamp_ms'] = df['timestamp_ms'].apply(lambda x: x.timestamp() * 1000)
        sender_counts = count_conversation_starters(df, mins_between_convos=60)
        expected_output = {'Sender1': 0, 'Sender2': 1}

        # self.assertEqual(sender_counts['Sender1'], 0)
        # self.assertEqual(sender_counts['Sender2'], 1)
        self.assertDictEqual(sender_counts, expected_output)

    def test_edge_case_empty_df(self):
        df = pd.DataFrame()
        expected_output = {}
        sender_counts = count_conversation_starters(df)
        self.assertDictEqual(sender_counts, expected_output)

    def test_edge_case_max_time_gap(self):
        data = {'sender_name': ['Sender1', 'Sender1', 'Sender2'],
                'timestamp_ms': [
                    self.newest,
                    self.newest - timedelta(minutes=61),
                    self.newest - timedelta(minutes=62),
                ]}
        df = pd.DataFrame(data)
        df['timestamp_ms'] = df['timestamp_ms'].apply(lambda x: x.timestamp() * 1000)
        expected_output = {'Sender1': 1, 'Sender2': 1}
        sender_counts = count_conversation_starters(df, mins_between_convos=60)
        self.assertDictEqual(sender_counts, expected_output)


if __name__ == '__main__':
    unittest.main()

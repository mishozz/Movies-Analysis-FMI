import unittest
from spark_manager import SparkSessionManager

class TestSparkSessionManager(unittest.TestCase):

    def test_start_stop_session(self):
        self.assertIsNone(SparkSessionManager._instance)

        SparkSessionManager.get_session()
        self.assertIsNotNone(SparkSessionManager._instance)

        SparkSessionManager.stop_session()
        self.assertIsNone(SparkSessionManager._instance)

if __name__ == '__main__':
    unittest.main()

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from unittest.mock import patch, MagicMock
from dags.airflow_tasks.tasks_manager import TasksManager

class TestTasks(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls._mock_df_repo = MagicMock()
        cls._mock_spark = MagicMock()
        cls._mock_data_utils = MagicMock()
        cls._task_manager = TasksManager(cls._mock_df_repo, cls._mock_spark, cls._mock_data_utils)

    def test_load_data(self):
        self._mock_data_utils.load_and_clean_data.return_value = (MagicMock(), MagicMock(), MagicMock())

        result = self._task_manager.load_data()

        self._mock_data_utils.load_and_clean_data.assert_called_once_with(self._mock_spark)
        self.assertEqual(self._mock_df_repo.save_dataframe.call_count, 3)
        self.assertEqual(result, "Core data loaded successfully")


    def test_transform_data(self):
        self._mock_df_repo.load_dataframe.side_effect = [MagicMock(), MagicMock(), MagicMock()]
        self._mock_data_utils.join_data.return_value = MagicMock()
        self._mock_data_utils.fetch_title_types.return_value = MagicMock()

        result = self._task_manager.transform_data()

        self.assertEqual(self._mock_df_repo.load_dataframe.call_count, 3)
        self._mock_data_utils.join_data.assert_called_once()
        self._mock_data_utils.fetch_title_types.assert_called_once()
        self.assertEqual(self._mock_df_repo.save_dataframe.call_count, 1)
        self.assertEqual(self._mock_df_repo.save_data.call_count, 1)
        self.assertEqual(result, "Data transformation completed successfully")


    def test_analyze_trends(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_production_trends.return_value = MagicMock()

        result = self._task_manager.analyze_trends()

        self._mock_df_repo.load_dataframe.assert_called_once_with('movies_df')
        self._mock_data_utils.analyze_production_trends.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Production trends analyzed")


    def test_analyze_genres(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_genre_ratings.return_value = MagicMock()

        result = self._task_manager.analyze_genres()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_data_utils.analyze_genre_ratings.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Genre analysis completed")


    def test_analyze_titles(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_df_repo.load_data.return_value = MagicMock()
        self._mock_data_utils.analyze_top_titles.return_value = [MagicMock(), MagicMock()]

        result = self._task_manager.analyze_titles()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_df_repo.load_data.assert_called_once_with('title_types')
        self._mock_data_utils.analyze_top_titles.assert_called_once()
        self.assertEqual(self._mock_df_repo.save_figure.call_count, 2)
        self._mock_df_repo.save_data.assert_called_once_with('title_fig_count', 2)
        self.assertEqual(result, "Title analysis completed")


    def test_analyze_actors(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_actors_with_highest_ratings.return_value = MagicMock()
        
        result = self._task_manager.analyze_actors()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_data_utils.analyze_actors_with_highest_ratings.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Actors analysis completed")


    def test_analyze_genres_by_count(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_genres_by_title_count.return_value = MagicMock()
        
        result = self._task_manager.analyze_genres_by_count()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_data_utils.analyze_genres_by_title_count.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once_with('genres_by_title_count', self._mock_data_utils.analyze_genres_by_title_count.return_value)
        self.assertEqual(result, "Genre count analysis completed")

        
    def test_analyze_titles_count_by_type(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_titles_count_by_type.return_value = MagicMock()
        
        result = self._task_manager.analyze_titles_count_by_type()

        self._mock_df_repo.load_dataframe.assert_called_once_with('movies_df')
        self._mock_data_utils.analyze_titles_count_by_type.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once_with('titles_count_by_type', self._mock_data_utils.analyze_titles_count_by_type.return_value)
        self.assertEqual(result, "Titles count by type analysis completed")

    def test_analyze_most_productive_actors(self):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_data_utils.analyze_most_productive_actors.return_value = MagicMock()
        
        result = self._task_manager.analyze_most_productive_actors()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_data_utils.analyze_most_productive_actors.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once_with('most_productive_actors', self._mock_data_utils.analyze_most_productive_actors.return_value)
        self.assertEqual(result, "Most productive actors analysis completed")

    def test_cleanup(self):
        result = self._task_manager.cleanup()

        self._mock_spark.stop_session.assert_called_once()
        self.assertEqual(result, "Cleanup completed")

if __name__ == '__main__':
    unittest.main()

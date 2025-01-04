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
        cls._task_manager = TasksManager(cls._mock_df_repo, cls._mock_spark)

    @patch('dags.airflow_tasks.tasks_manager.load_and_clean_data')
    def test_load_data(self, mock_load_and_clean_data):
        mock_load_and_clean_data.return_value = (MagicMock(), MagicMock(), MagicMock())

        result = self._task_manager.load_data()

        mock_load_and_clean_data.assert_called_once_with(self._mock_spark)
        self.assertEqual(self._mock_df_repo.save_dataframe.call_count, 3)
        self.assertEqual(result, "Core data loaded successfully")


    @patch('dags.airflow_tasks.tasks_manager.join_data')
    @patch('dags.airflow_tasks.tasks_manager.fetch_title_types')
    def test_transform_data(self, mock_fetch_title_types, mock_join_data):
        self._mock_df_repo.load_dataframe.side_effect = [MagicMock(), MagicMock(), MagicMock()]
        mock_join_data.return_value = MagicMock()
        mock_fetch_title_types.return_value = MagicMock()

        result = self._task_manager.transform_data()

        self.assertEqual(self._mock_df_repo.load_dataframe.call_count, 3)
        mock_join_data.assert_called_once()
        mock_fetch_title_types.assert_called_once()
        self.assertEqual(self._mock_df_repo.save_dataframe.call_count, 1)
        self.assertEqual(self._mock_df_repo.save_data.call_count, 1)
        self.assertEqual(result, "Data transformation completed successfully")


    @patch('dags.airflow_tasks.tasks_manager.analyze_production_trends')
    def test_analyze_trends(self, mock_analyze_production_trends):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_production_trends.return_value = MagicMock()

        result = self._task_manager.analyze_trends()

        self._mock_df_repo.load_dataframe.assert_called_once_with('movies_df')
        mock_analyze_production_trends.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Production trends analyzed")


    @patch('dags.airflow_tasks.tasks_manager.analyze_genre_ratings')
    def test_analyze_genres(self, mock_analyze_genre_ratings):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_genre_ratings.return_value = MagicMock()

        result = self._task_manager.analyze_genres()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        mock_analyze_genre_ratings.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Genre analysis completed")


    @patch('dags.airflow_tasks.tasks_manager.analyze_top_titles')
    def test_analyze_titles(self, mock_analyze_top_titles):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        self._mock_df_repo.load_data.return_value = MagicMock()
        mock_analyze_top_titles.return_value = [MagicMock(), MagicMock()]

        result = self._task_manager.analyze_titles()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        self._mock_df_repo.load_data.assert_called_once_with('title_types')
        mock_analyze_top_titles.assert_called_once()
        self.assertEqual(self._mock_df_repo.save_figure.call_count, 2)
        self._mock_df_repo.save_data.assert_called_once_with('title_fig_count', 2)
        self.assertEqual(result, "Title analysis completed")


    @patch('dags.airflow_tasks.tasks_manager.save_plots_to_pdf')
    def test_save_report(self, mock_save_plots_to_pdf):
        self._mock_df_repo.load_figure.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        self._mock_df_repo.load_data.return_value = 2

        result = self._task_manager.save_report()

        self.assertEqual(self._mock_df_repo.load_figure.call_count, 6)
        self._mock_df_repo.load_data.assert_called_once_with('title_fig_count')
        mock_save_plots_to_pdf.assert_called_once()
        self.assertEqual(result, "Report saved successfully")


    @patch('dags.airflow_tasks.tasks_manager.analyze_actors_with_highest_ratings')
    def test_analyze_actors(self, mock_analyze_actors_with_highest_ratings):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_actors_with_highest_ratings.return_value = MagicMock()
        
        result = self._task_manager.analyze_actors()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        mock_analyze_actors_with_highest_ratings.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Actors analysis completed")


    @patch('dags.airflow_tasks.tasks_manager.analyze_genres_by_title_count')
    def test_analyze_genres_by_count(self, mock_analyze_genres_by_title_count):
        self._mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_genres_by_title_count.return_value = MagicMock()
        
        result = self._task_manager.analyze_genres_by_count()

        self._mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        mock_analyze_genres_by_title_count.assert_called_once()
        self._mock_df_repo.save_figure.assert_called_once_with('genres_by_title_count', mock_analyze_genres_by_title_count.return_value)
        self.assertEqual(result, "Genre count analysis completed")

    def test_cleanup(self):
        result = self._task_manager.cleanup()

        self._mock_spark.stop_session.assert_called_once()
        self.assertEqual(result, "Cleanup completed")

if __name__ == '__main__':
    unittest.main()

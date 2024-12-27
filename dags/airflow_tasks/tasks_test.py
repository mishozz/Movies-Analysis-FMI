import unittest
from unittest.mock import patch, MagicMock
from tasks import load_data, transform_data, analyze_trends, analyze_genres, analyze_titles, save_report, cleanup

class TestTasks(unittest.TestCase):

    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.SparkSessionManager.get_session')
    @patch('tasks.load_and_clean_data')
    def test_load_data(self, mock_load_and_clean_data, mock_get_session, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_spark = MagicMock()
        mock_get_session.return_value = mock_spark
        mock_load_and_clean_data.return_value = (MagicMock(), MagicMock(), MagicMock())

        result = load_data()

        mock_get_repository_instance.assert_called_once()
        mock_get_session.assert_called_once()
        mock_load_and_clean_data.assert_called_once_with(mock_spark)
        self.assertEqual(mock_df_repo.save_dataframe.call_count, 3)
        self.assertEqual(result, "Core data loaded successfully")

    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.join_data')
    @patch('tasks.fetch_title_types')
    def test_transform_data(self, mock_fetch_title_types, mock_join_data, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_df_repo.load_dataframe.side_effect = [MagicMock(), MagicMock(), MagicMock()]
        mock_join_data.return_value = MagicMock()
        mock_fetch_title_types.return_value = MagicMock()

        result = transform_data()

        mock_get_repository_instance.assert_called_once()
        self.assertEqual(mock_df_repo.load_dataframe.call_count, 3)
        mock_join_data.assert_called_once()
        mock_fetch_title_types.assert_called_once()
        self.assertEqual(mock_df_repo.save_dataframe.call_count, 1)
        self.assertEqual(mock_df_repo.save_data.call_count, 1)
        self.assertEqual(result, "Data transformation completed successfully")

    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.analyze_production_trends')
    def test_analyze_trends(self, mock_analyze_production_trends, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_production_trends.return_value = MagicMock()

        result = analyze_trends()

        mock_get_repository_instance.assert_called_once()
        mock_df_repo.load_dataframe.assert_called_once_with('movies_df')
        mock_analyze_production_trends.assert_called_once()
        mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Production trends analyzed")

    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.analyze_genre_ratings')
    def test_analyze_genres(self, mock_analyze_genre_ratings, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_analyze_genre_ratings.return_value = MagicMock()

        result = analyze_genres()

        mock_get_repository_instance.assert_called_once()
        mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        mock_analyze_genre_ratings.assert_called_once()
        mock_df_repo.save_figure.assert_called_once()
        self.assertEqual(result, "Genre analysis completed")

    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.analyze_top_titles')
    def test_analyze_titles(self, mock_analyze_top_titles, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_df_repo.load_dataframe.return_value = MagicMock()
        mock_df_repo.load_data.return_value = MagicMock()
        mock_analyze_top_titles.return_value = [MagicMock(), MagicMock()]

        result = analyze_titles()

        mock_get_repository_instance.assert_called_once()
        mock_df_repo.load_dataframe.assert_called_once_with('joined_df')
        mock_df_repo.load_data.assert_called_once_with('title_types')
        mock_analyze_top_titles.assert_called_once()
        self.assertEqual(mock_df_repo.save_figure.call_count, 2)
        mock_df_repo.save_data.assert_called_once_with('title_fig_count', 2)
        self.assertEqual(result, "Title analysis completed")
        
    @patch('tasks.RepositoryConfig.get_repository_instance')
    @patch('tasks.save_plots_to_pdf')
    def test_save_report(self, mock_save_plots_to_pdf, mock_get_repository_instance):
        mock_df_repo = MagicMock()
        mock_get_repository_instance.return_value = mock_df_repo
        mock_df_repo.load_figure.side_effect = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        mock_df_repo.load_data.return_value = 2

        result = save_report()

        mock_get_repository_instance.assert_called_once()
        self.assertEqual(mock_df_repo.load_figure.call_count, 4)
        mock_df_repo.load_data.assert_called_once_with('title_fig_count')
        mock_save_plots_to_pdf.assert_called_once()
        self.assertEqual(result, "Report saved successfully")

    @patch('tasks.SparkSessionManager.stop_session')
    def test_cleanup(self, mock_stop_session):
        result = cleanup()

        mock_stop_session.assert_called_once()
        self.assertEqual(result, "Cleanup completed")

if __name__ == '__main__':
    unittest.main()

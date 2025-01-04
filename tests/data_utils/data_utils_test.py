import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from dags.data_utils.data_utils import DataUtils
from pyspark.sql.functions import col, split

class TestAnalyzingFunctions(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
        self.data_utils = DataUtils()

    def tearDown(self):
        self.spark.stop()

    def test_load_and_clean_data(self):
        titles_df = MagicMock()
        ratings_df = MagicMock()
        actors_df = MagicMock()
        
        titles_df.filter.return_value = titles_df
        titles_df.withColumn.return_value = titles_df
        
        mock_spark = MagicMock()
        mock_spark.read.csv.side_effect = [titles_df, ratings_df, actors_df]

        cleaned_titles_df, _, _ = self.data_utils.load_and_clean_data(mock_spark)
        
        self.assertTrue(cleaned_titles_df.filter.called)
        self.assertTrue(cleaned_titles_df.withColumn.called)
        
    def test_join_data(self):
        movies_path = "test_data_samples/titles.tsv"
        ratings_path = "test_data_samples/ratings.tsv"
        actors_path = "test_data_samples/actors.tsv"

        movies_df = self.spark.read.csv(movies_path, sep='\t', header=True, inferSchema=True)
        ratings_df = self.spark.read.csv(ratings_path, sep='\t', header=True, inferSchema=True)
        actors_df = self.spark.read.csv(actors_path, sep='\t', header=True, inferSchema=True)

        result_df = self.data_utils.join_data(movies_df, ratings_df, actors_df)
        self.assertEqual(result_df.count(), 2)
        
    @patch('dags.data_utils.data_utils.create_barplot')
    def test_analyze_production_trends(self, mock_create_barplot):
        movies_path = "test_data_samples/titles.tsv"
        movies_df = self.spark.read.csv(movies_path, sep='\t', header=True, inferSchema=True)
        mock_create_barplot.return_value = "mocked_figure"

        result_fig = self.data_utils.analyze_production_trends(movies_df)

        mock_create_barplot.assert_called_once()
        _, kwargs = mock_create_barplot.call_args
        self.assertIn('x', kwargs)
        self.assertIn('y', kwargs)
        self.assertIn('x_label', kwargs)
        self.assertIn('y_label', kwargs)
        self.assertIn('barplot_title', kwargs)
        self.assertEqual(result_fig, "mocked_figure")
        
    @patch('dags.data_utils.data_utils.create_barplot')
    def test_analyze_genre_ratings(self, mock_create_barplot):
        data = [
            ("tt0000001", "Action,Comedy", 7.0, 1000),
            ("tt0000002", "Action,Drama", 8.0, 1500),
            ("tt0000003", "Comedy,Drama", 6.5, 2000),
            ("tt0000004", "Action", 9.0, 2500),
            ("tt0000005", "Drama", 7.5, 3000)
        ]
        schema = ["tconst", "genres_array", "averageRating", "numVotes"]
        titles_actors_ratings_joined = self.spark.createDataFrame(data, schema)
        titles_actors_ratings_joined = titles_actors_ratings_joined.withColumn("genres_array", split(col("genres_array"), ","))

        mock_create_barplot.return_value = "mocked_figure"
        result_fig = self.data_utils.analyze_genre_ratings(titles_actors_ratings_joined, topN=3)

        mock_create_barplot.assert_called_once()
        _, kwargs = mock_create_barplot.call_args
        self.assertIn('x', kwargs)
        self.assertIn('y', kwargs)
        self.assertIn('x_label', kwargs)
        self.assertIn('y_label', kwargs)
        self.assertIn('barplot_title', kwargs)
        self.assertEqual(result_fig, "mocked_figure")
    
    @patch('dags.data_utils.data_utils.create_barplot')
    def test_analyze_top_titles(self, mock_create_barplot):
        data = [
            ("tt0000001", "Action", "Movie", "Title1", 7.0, 11000),
            ("tt0000002", "Action", "Movie", "Title2", 8.0, 11500),
            ("tt0000003", "Comedy", "Movie", "Title3", 6.5, 12000),
            ("tt0000004", "Action", "Movie", "Title4", 9.0, 10500),
            ("tt0000005", "Drama", "Movie", "Title5", 7.5,  10000)
        ]
        schema = ["tconst", "genres_array", "titleType", "primaryTitle", "averageRating", "numVotes"]
        titles_actors_ratings_joined = self.spark.createDataFrame(data, schema)
        
        mock_create_barplot.return_value = "mocked_figure"

        titleTypes = ["Movie"]
        result_figures = self.data_utils.analyze_top_titles(titles_actors_ratings_joined, titleTypes, topN=3)
        mock_create_barplot.assert_called()
        for call in mock_create_barplot.call_args_list:
            _, kwargs = call
            self.assertIn('x', kwargs)
            self.assertIn('y', kwargs)
            self.assertIn('x_label', kwargs)
            self.assertIn('y_label', kwargs)
            self.assertIn('barplot_title', kwargs)
        self.assertEqual(result_figures, ["mocked_figure"])

    @patch('dags.data_utils.data_utils.create_barplot')
    def test_analyze_actors_with_highest_ratings(self, mock_create_barplot):
        data = [
            ("movie", "Actor A", 8.5),
            ("movie", "Actor B", 9.0),
            ("movie", "Actor C", 7.5),
            ("movie", "Actor D", 8.0),
            ("movie", "Actor E", 9.5),
            ("movie", "Actor F", 6.5),
            ("movie", "Actor G", 7.0),
            ("movie", "Actor H", 8.2),
            ("movie", "Actor I", 8.8),
            ("movie", "Actor J", 9.1)
        ]
        schema = ["titleType", "actorName", "averageRating"]
        joined_df = self.spark.createDataFrame(data, schema)

        mock_create_barplot.return_value = "mocked_figure"
        result_fig = self.data_utils.analyze_actors_with_highest_ratings(joined_df)

        mock_create_barplot.assert_called_once()
        _, kwargs = mock_create_barplot.call_args
        self.assertIn('x', kwargs)
        self.assertIn('y', kwargs)
        self.assertIn('x_label', kwargs)
        self.assertIn('y_label', kwargs)
        self.assertIn('barplot_title', kwargs)
        self.assertEqual(result_fig, "mocked_figure")

    @patch('dags.data_utils.data_utils.create_piechart')
    def test_analyze_genres_by_title_count(self, mock_create_piechart):
        data = [
            ("movie", ["Action", "Comedy"]),
            ("movie", ["Action"]),
            ("movie", ["Drama"]),
            ("movie", ["Action", "Drama"]),
            ("movie", ["Comedy"]),
            ("movie", ["Drama", "Comedy"]),
        ]
        schema = ["titleType", "genres_array"]
        joined_df = self.spark.createDataFrame(data, schema)
        mock_create_piechart.return_value = "mocked_piechart"

        result_fig = self.data_utils.analyze_genres_by_title_count(joined_df, 1)

        mock_create_piechart.assert_called_once()
        _, kwargs = mock_create_piechart.call_args
        self.assertIn('labels', kwargs)
        self.assertIn('values', kwargs)
        self.assertIn('title', kwargs)
        self.assertEqual(result_fig, "mocked_piechart")

    @patch('dags.data_utils.data_utils.create_barplot')
    def test_analyze_most_productive_actors(self, mock_create_barplot):
        data = [
            ("ttconst1","movie", "Actor A", 1000),
            ("ttconst2","movie", "Actor B", 1500),
            ("ttconst3","movie", "Actor C", 2000),
            ("ttconst4","movie", "Actor D", 2500),
            ("ttconst5","movie", "Actor E", 3000),
            ("ttconst6","movie", "Actor F", 3500),
            ("ttconst7","movie", "Actor G", 4000),
            ("ttconst8","movie", "Actor H", 4500),
            ("ttconst9","movie", "Actor I", 5000),
            ("ttconst10","movie", "Actor J", 5500)
        ]
        schema = ["tconst","titleType", "actorName", "numVotes"]
        joined_df = self.spark.createDataFrame(data, schema)

        mock_create_barplot.return_value = "mocked_figure"
        result_fig = self.data_utils.analyze_most_productive_actors(joined_df)

        mock_create_barplot.assert_called_once()
        _, kwargs = mock_create_barplot.call_args
        self.assertIn('x', kwargs)
        self.assertIn('y', kwargs)
        self.assertIn('x_label', kwargs)
        self.assertIn('y_label', kwargs)
        self.assertIn('barplot_title', kwargs)
        self.assertEqual(result_fig, "mocked_figure")

    

if __name__ == '__main__':
    unittest.main()

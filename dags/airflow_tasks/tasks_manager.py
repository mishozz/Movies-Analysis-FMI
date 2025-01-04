from dags.data_utils.analyzing_functions import (
    load_and_clean_data, join_data, fetch_title_types, analyze_production_trends,
    analyze_genre_ratings, analyze_top_titles, analyze_actors_with_highest_ratings,
    analyze_genres_by_title_count
)
from dags.data_utils.plotlib_utils import save_plots_to_pdf

class TasksManager:
    def __init__(self, df_repo, spark):
        self.df_repo =df_repo
        self.spark = spark

    def load_data(self, **context):
        """Load and clean data, save core datasets to the database"""
        movies_df, ratings_df, actors_df = load_and_clean_data(self.spark)
        
        self.df_repo.save_dataframe('movies_df', movies_df)
        self.df_repo.save_dataframe('ratings_df', ratings_df)
        self.df_repo.save_dataframe('actors_df', actors_df)
        
        return "Core data loaded successfully"

    def transform_data(self, **context):
        """Join datasets and parse title types, save transformed data"""
        movies_df = self.df_repo.load_dataframe('movies_df')
        ratings_df = self.df_repo.load_dataframe('ratings_df')
        actors_df = self.df_repo.load_dataframe('actors_df')
        
        titles_actors_ratings_joined = join_data(movies_df, ratings_df, actors_df)
        title_types = fetch_title_types(movies_df)
        
        self.df_repo.save_dataframe('joined_df', titles_actors_ratings_joined)
        self.df_repo.save_data('title_types', title_types)
        
        return "Data transformation completed successfully"

    def analyze_trends(self, **context):
        """Analyze production trends"""
        movies_df = self.df_repo.load_dataframe('movies_df')
        
        fig = analyze_production_trends(movies_df)
        self.df_repo.save_figure('trends', fig)
        
        return "Production trends analyzed"

    def analyze_genres(self, **context):
        """Analyze genre ratings"""
        joined_df = self.df_repo.load_dataframe('joined_df')
        
        fig = analyze_genre_ratings(joined_df)
        self.df_repo.save_figure('genres', fig)
        
        return "Genre analysis completed"

    def analyze_titles(self, **context):
        """Analyze top titles"""
        joined_df = self.df_repo.load_dataframe('joined_df')
        title_types = self.df_repo.load_data('title_types')
        
        figs = analyze_top_titles(joined_df, title_types)
        
        for i, fig in enumerate(figs):
            self.df_repo.save_figure(f'titles_{i}', fig)
        
        self.df_repo.save_data('title_fig_count', len(figs))
        
        return "Title analysis completed"

    def analyze_actors(self, **context):
        """Analyzing actors with the highest average ratings"""
        joined_df = self.df_repo.load_dataframe('joined_df')
        
        fig = analyze_actors_with_highest_ratings(joined_df)
        self.df_repo.save_figure('actors', fig)

        return "Actors analysis completed"

    def analyze_genres_by_count(self, **context):
        """Analyze genres by title count"""
        joined_df = self.df_repo.load_dataframe('joined_df')
        
        fig = analyze_genres_by_title_count(joined_df)
        self.df_repo.save_figure('genres_by_title_count', fig)

        return "Genre count analysis completed"

    def save_report(self, **context):
        """Create PDF report from saved figures"""
        figures = [
            self.df_repo.load_figure('trends'),
            self.df_repo.load_figure('genres'),
            self.df_repo.load_figure('actors'),
            self.df_repo.load_figure('genres_by_title_count')
        ]

        title_fig_count = self.df_repo.load_data('title_fig_count')
        for i in range(title_fig_count):
            figures.append(self.df_repo.load_figure(f'titles_{i}'))
        
        save_plots_to_pdf(figures)

        return "Report saved successfully"

    def cleanup(self, **context):
        """Cleanup Spark session"""
        self.spark.stop_session()
        return "Cleanup completed"
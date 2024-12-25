from dags.persistence.db_config import get_dataframe_repository
from data.analyzer import load_and_clean_data, join_data, fetch_title_types, analyze_production_trends, analyze_genre_ratings, analyze_top_titles, save_plots_to_pdf
from spark.sparkManager import SparkSessionManager

def load_data(**context):
    """Load and clean data, save core datasets to the database"""
    df_repo =  get_dataframe_repository()
    spark = SparkSessionManager.get_session()
    movies_df, ratings_df, actors_df = load_and_clean_data(spark)
    
    # Save the cleaned datasets to the database
    df_repo.save_dataframe('movies_df', movies_df)
    df_repo.save_dataframe('ratings_df', ratings_df)
    df_repo.save_dataframe('actors_df', actors_df)
    
    return "Core data loaded successfully"

def transform_data(**context):
    """Join datasets and parse title types, save transformed data"""
    df_repo =  get_dataframe_repository()
    movies_df = df_repo.load_dataframe('movies_df')
    ratings_df = df_repo.load_dataframe('ratings_df')
    actors_df = df_repo.load_dataframe('actors_df')
    
    titles_actors_ratings_joined = join_data(movies_df, ratings_df, actors_df)
    title_types = fetch_title_types(movies_df)
    
    df_repo.save_dataframe('joined_df', titles_actors_ratings_joined)
    df_repo.save_data('title_types', title_types)
    
    return "Data transformation completed successfully"

def analyze_trends(**context):
    """Analyze production trends"""
    df_repo =  get_dataframe_repository()
    movies_df = df_repo.load_dataframe('movies_df')
    
    fig = analyze_production_trends(movies_df)
    df_repo.save_figure('trends', fig)
    
    return "Production trends analyzed"

def analyze_genres(**context):
    """Analyze genre ratings"""
    df_repo =  get_dataframe_repository()
    joined_df = df_repo.load_dataframe('joined_df')
    
    fig = analyze_genre_ratings(joined_df)
    df_repo.save_figure('genres', fig)
    
    return "Genre analysis completed"

def analyze_titles(**context):
    """Analyze top titles"""
    df_repo =  get_dataframe_repository()
    joined_df = df_repo.load_dataframe('joined_df')
    title_types = df_repo.load_data('title_types')
    
    figs = analyze_top_titles(joined_df, title_types)
    
    for i, fig in enumerate(figs):
        df_repo.save_figure(f'titles_{i}', fig)
    
    df_repo.save_data('title_fig_count', len(figs))
    
    return "Title analysis completed"

def save_report(**context):
    """Create PDF report from saved figures"""
    df_repo =  get_dataframe_repository()
    
    figures = []
    figures.append(df_repo.load_figure('trends'))
    figures.append(df_repo.load_figure('genres'))
    
    title_fig_count = df_repo.load_data('title_fig_count')
    for i in range(title_fig_count):
        figures.append(df_repo.load_figure(f'titles_{i}'))
    
    save_plots_to_pdf(figures)
    
    return "Report saved successfully"

def cleanup(**context):
    """Cleanup Spark session"""
    SparkSessionManager.stop_session()
    return "Cleanup completed"

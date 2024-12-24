from dags.persistence.db_config import get_database
from data.analyzer import load_and_clean_data, join_data, fetch_title_types, analyze_production_trends, analyze_genre_ratings, analyze_top_titles, save_plots_to_pdf
from spark.sparkManager import SparkSessionManager

def load_data(**context):
    """Load and clean data, save to database"""
    db = get_database()
    spark = SparkSessionManager.get_session()
    
    movies_df, ratings_df, actors_df = load_and_clean_data(spark)
    titles_actors_ratings_joined = join_data(movies_df, ratings_df, actors_df)
    title_types = fetch_title_types(movies_df)
    
    # Save to database
    db.save_dataframe('movies_df', movies_df)
    db.save_dataframe('joined_df', titles_actors_ratings_joined)
    db.save_data('title_types', title_types)
    
    return "Data loaded successfully"

def analyze_trends(**context):
    """Analyze production trends"""
    db = get_database()
    movies_df = db.load_dataframe('movies_df')
    print(movies_df.show(n=3))
    
    fig = analyze_production_trends(movies_df)
    db.save_figure('trends', fig)
    
    return "Production trends analyzed"

def analyze_genres(**context):
    """Analyze genre ratings"""
    db = get_database()
    joined_df = db.load_dataframe('joined_df')
    
    fig = analyze_genre_ratings(joined_df)
    db.save_figure('genres', fig)
    
    return "Genre analysis completed"

def analyze_titles(**context):
    """Analyze top titles"""
    db = get_database()
    joined_df = db.load_dataframe('joined_df')
    title_types = db.load_data('title_types')
    
    figs = analyze_top_titles(joined_df, title_types)
    
    for i, fig in enumerate(figs):
        db.save_figure(f'titles_{i}', fig)
    
    db.save_data('title_fig_count', len(figs))
    
    return "Title analysis completed"

def save_report(**context):
    """Create PDF report from saved figures"""
    db = get_database()
    
    figures = []
    figures.append(db.load_figure('trends'))
    figures.append(db.load_figure('genres'))
    
    title_fig_count = db.load_data('title_fig_count')
    for i in range(title_fig_count):
        figures.append(db.load_figure(f'titles_{i}'))
    
    save_plots_to_pdf(figures)
    
    return "Report saved successfully"

def cleanup(**context):
    """Cleanup Spark session"""
    SparkSessionManager.stop_session()
    return "Cleanup completed"
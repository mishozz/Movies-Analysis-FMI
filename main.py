from spark_utils import create_spark_session
import time
from analyzer import load_and_clean_data, join_data, fetch_title_types, analyze_production_trends, analyze_genre_ratings, analyze_top_titles, save_plots_to_pdf

def main():
    spark = create_spark_session("IMDb Analysis")
    try:
        print("Loading data...")
        titles_df, ratings_df, actors_df = load_and_clean_data(spark)
        
        titles_actors_ratings_joined = join_data(titles_df, ratings_df, actors_df)
        titleTypes = fetch_title_types(titles_df)
        
        production_trends_plot = analyze_production_trends(titles_df)
        genres_ratings_plot = analyze_genre_ratings(titles_actors_ratings_joined)
        top_titles_plots = analyze_top_titles(titles_actors_ratings_joined, titleTypes)
        combined_plots = [production_trends_plot, genres_ratings_plot] + top_titles_plots
        save_plots_to_pdf(combined_plots) 
    finally:
        spark.stop()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time} seconds")
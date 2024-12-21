from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, explode, split, desc, round
from pyspark.sql.types import IntegerType
import plotly.express as px
from plotly.subplots import make_subplots
import time
import plotly.io as pio
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from PIL import Image
import io

MIN_NUM_OF_VOTES = 5000


def create_spark_session():
    return SparkSession.builder \
        .appName("IMDb Analysis") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_and_clean_data(spark):
    movies_df = spark.read.csv("moves_and_tv_series.tsv", sep="\t", header=True)
    ratings_df = spark.read.csv("ratings.tsv", sep="\t", header=True)
    actors_df = spark.read.csv("actors.tsv", sep="\t", header=True)

    movies_df = movies_df \
        .filter(col("startYear") != "\\N") \
        .withColumn("startYear", col("startYear").cast(IntegerType())) \
        .withColumn("genres_array", split(col("genres"), ","))

    return movies_df, ratings_df, actors_df

def join_data(movies_df, ratings_df, actors_df):
    return actors_df \
        .withColumn("movie_id", explode(split(col("knownForTitles"), ","))) \
        .join(ratings_df, col("movie_id") == ratings_df.tconst) \
        .join(movies_df, col("movie_id") == movies_df.tconst) \
        .withColumnRenamed("primaryName", "actorName") \
        .cache()
        
def fetch_title_types(movies_df):
    return movies_df.select("titleType").distinct().rdd.flatMap(lambda x: x).collect()            
        
def analyze_production_trends(movies_df):
    print("Analyzing production trends...")
    decade_analysis = movies_df \
        .withColumn("decade", (col("startYear") / 10).cast(IntegerType()) * 10) \
        .groupBy("decade") \
        .agg(count("*").alias("movie_count")) \
        .orderBy("decade") \
        .collect()

    decades = [row['decade'] for row in decade_analysis]
    movie_counts = [row['movie_count'] for row in decade_analysis]

    return px.bar(x=decades, y=movie_counts, labels={'x': 'Decade', 'y': 'Number of Movies'}, title='Number of Movies Produced per Decade')
   
def analyze_genre_ratings(titles_actors_ratings_joined, topN=10):
    print("Analyzing genres by average rating...")
    genre_analysis = titles_actors_ratings_joined \
        .filter(col("numVotes") >= MIN_NUM_OF_VOTES) \
        .withColumn("genre", explode(col("genres_array"))) \
        .groupBy("genre") \
        .agg(
            round(avg("averageRating"), 2).alias("avg_rating"),
        ) \
        .orderBy(desc("avg_rating")) \
        .limit(topN) \
        .collect()
        
    genres = [row['genre'] for row in genre_analysis]
    avg_ratings = [row['avg_rating'] for row in genre_analysis]
    
    return px.bar(x=genres, y=avg_ratings, labels={'x': 'Genre', 'y': 'Average Rating'}, title='Average Rating by Genre')

def save_plots_to_pdf(figures):
    # n = len(figures)
    # subplots = make_subplots(rows=n, cols=1)
    # for i, fig in enumerate(figures):
    #     for trace in fig['data']:
    #         subplots.add_trace(trace, row=i+1, col=1)

    # subplots.update_layout(height=800, width=600, title_text="IMDd data Analysis")
    # subplots.write_image("combined_plots.pdf", format="pdf")
     with PdfPages('combined_plots.pdf') as pdf:
        for fig in figures:
            # Convert Plotly figure to a static image
            img_bytes = pio.to_image(fig, format='png')
            
            # Read the image from the byte array
            img = Image.open(io.BytesIO(img_bytes))
            
            # Create a new matplotlib figure
            plt.figure(figsize=(8, 6))
            plt.imshow(img, aspect='auto')
            plt.axis('off')
            
            # Save the current figure to the PDF
            pdf.savefig()
            plt.close()


def analyze_top_titles(titles_actors_ratings_joined, titleTypes, topN=5):    
    print("Analyzing top titles for each type by average movie rating...")
    figures = []
    for titleType in titleTypes:
        print(f"\nTop titles in {titleType}")
        temp_df = titles_actors_ratings_joined \
            .filter(col("numVotes") >= MIN_NUM_OF_VOTES) \
            .filter(col("titleType") == titleType) \
            .groupBy("primaryTitle") \
            .agg(
                round(avg("averageRating"), 2).alias("avg_rating"),
            ) \
            .orderBy(desc("avg_rating")) \
            .limit(topN)
            
        temp_df_data = temp_df.collect()
        
        # Extract data for plotting
        titles = [row['primaryTitle'] for row in temp_df_data]
        avg_ratings = [row['avg_rating'] for row in temp_df_data]
        print(f"Titles: {titles}")
        print(f"Average Ratings: {avg_ratings}")
        # Create a plotly bar chart
        if len(titles) != 0 and len(avg_ratings) != 0:
            fig = px.bar(x=titles, y=avg_ratings, labels={'x': 'Title', 'y': 'Average Rating'}, title=f'Top {topN} Titles in {titleType}')
            figures.append(fig)
        
    return figures

def main():
    spark = create_spark_session()
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

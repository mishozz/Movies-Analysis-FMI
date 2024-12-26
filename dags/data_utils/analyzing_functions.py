from pyspark.sql.functions import col, avg, count, explode, split, desc, round
from pyspark.sql.types import IntegerType
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

MIN_NUM_OF_VOTES = 5000

def load_and_clean_data(spark):
    movies_df = spark.read.csv("titles.tsv", sep="\t", header=True)
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
        .drop(movies_df.tconst)

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
    
    fig = create_barplot(x=decades, y=movie_counts, x_label='Decade', y_label='Number of Movies (Millions)', barplot_title='Number of titles produced per decade')
    return fig
   
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
    
    fig = create_barplot(x=genres, y=avg_ratings, x_label='Genre', y_label='Average Rating', barplot_title=f'Top {topN} Genres by Average Rating')
    return fig

def save_plots_to_pdf(figures):
     with PdfPages('report.pdf') as pdf:
        for fig in figures:
            pdf.savefig(fig)
            plt.close(fig)


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
        
        titles = [row['primaryTitle'] for row in temp_df_data]
        avg_ratings = [row['avg_rating'] for row in temp_df_data]

        if len(titles) != 0 and len(avg_ratings) != 0:
            fig = create_barplot(x=titles, y=avg_ratings, x_label = 'Titles', y_label="Avg Rating", barplot_title=f'Top {topN} Titles in {titleType}')
            figures.append(fig)
        
    return figures

def create_barplot(x, y, x_label, y_label, barplot_title):
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.bar(x, y)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(barplot_title)
    plt.xticks(rotation=70, ha='left')
    plt.tight_layout()

    return fig

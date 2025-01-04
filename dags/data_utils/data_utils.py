from pyspark.sql.functions import col, avg, count, explode, split, desc, round
from pyspark.sql.types import IntegerType
from dags.data_utils.plotlib_utils import create_barplot, create_piechart

MIN_NUM_OF_VOTES = 5000
MIN_TITLE_COUNT = 20000

class DataUtils:

    def load_and_clean_data(self, spark):
        movies_df = spark.read.csv("titles.tsv", sep="\t", header=True)
        ratings_df = spark.read.csv("ratings.tsv", sep="\t", header=True)
        actors_df = spark.read.csv("actors.tsv", sep="\t", header=True)

        movies_df = movies_df \
            .filter((col("startYear") != "\\N") & (col("genres") != "\\N")) \
            .withColumn("genres_array", split(col("genres"), ","))

        return movies_df, ratings_df, actors_df

    def join_data(self, movies_df, ratings_df, actors_df):
        return actors_df \
            .withColumn("movie_id", explode(split(col("knownForTitles"), ","))) \
            .join(ratings_df, col("movie_id") == ratings_df.tconst) \
            .join(movies_df, col("movie_id") == movies_df.tconst) \
            .withColumnRenamed("primaryName", "actorName") \
            .drop(movies_df.tconst)

    def fetch_title_types(self, movies_df):
        return movies_df.select("titleType").distinct().rdd.flatMap(lambda x: x).collect()            
            
    def analyze_production_trends(self, movies_df):
        print("Analyzing production trends...")
        decade_analysis = movies_df \
            .withColumn("decade", (col("startYear") / 10).cast(IntegerType()) * 10) \
            .groupBy("decade") \
            .agg(count("*").alias("movie_count")) \
            .orderBy("decade") \
            .collect()

        decades = [row['decade'] for row in decade_analysis]
        movie_counts = [row['movie_count'] for row in decade_analysis]
        
        fig = create_barplot(x=decades, y=movie_counts, x_label='Decade', y_label='Number of Titles (Millions)', barplot_title='Number of titles produced per decade')
        return fig

    def analyze_genre_ratings(self, titles_actors_ratings_joined, topN=10):
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

    def analyze_top_titles(self, titles_actors_ratings_joined, titleTypes, topN=5):    
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

    def analyze_actors_with_highest_ratings(self, joined_df):
        print("Analyzing actors with the highest average ratings...")
        top_actors = joined_df \
            .filter(col("titleType") == "movie") \
            .groupBy("actorName") \
            .agg(avg("averageRating").alias("avg_rating")) \
            .orderBy(col("avg_rating").desc()) \
            .limit(10) \
            .collect()

        actors = [row['actorName'] for row in top_actors]
        avg_ratings = [row['avg_rating'] for row in top_actors]

        fig = create_barplot(x=actors, y=avg_ratings, x_label='Actors', y_label='Average Rating', barplot_title='Movie Actors with Highest Average Ratings')

        return fig

    def analyze_genres_by_title_count(self, joined_df, min_title_count=MIN_TITLE_COUNT):
        print("Analyzing genres by title count...")
        
        genre_counts = joined_df \
            .withColumn("genre", explode(col("genres_array"))) \
            .groupBy("genre") \
            .agg(count("*").alias("title_count")) \
            .filter(col("title_count") > min_title_count) \
            .orderBy(col("title_count").desc()) \
            .collect()

        genres = [row['genre'] for row in genre_counts]
        title_counts = [row['title_count'] for row in genre_counts]

        return create_piechart(labels=genres, values=title_counts, title=f'Genres by Title Count with more than {MIN_TITLE_COUNT} titles', legend_title_for_count="Genres count")

    def analyze_titles_count_by_type(self, movies_df):
        print("Analyzing titles count by type...")
        title_counts = movies_df \
            .groupBy("titleType") \
            .agg(count("*").alias("title_count")) \
            .collect()

        title_types = [row['titleType'] for row in title_counts]
        title_counts = [row['title_count'] for row in title_counts]

        return create_piechart(labels=title_types, values=title_counts, title='Titles count by type', legend_title_for_count="Titles count")
    
    def analyze_most_productive_actors(self, joined_df):
        print("Analyzing most productive actors...")
        productive_actors = joined_df \
            .groupBy("actorName") \
            .agg(count("tconst").alias("movie_count")) \
            .orderBy(col("movie_count").desc()) \
            .limit(10) \
            .collect()

        actors = [row['actorName'] for row in productive_actors]
        movie_counts = [row['movie_count'] for row in productive_actors]
        
        fig = create_barplot(x=actors, y=movie_counts, x_label='Actors', y_label='Number of Movies', barplot_title='Most Productive Actors')
        return fig

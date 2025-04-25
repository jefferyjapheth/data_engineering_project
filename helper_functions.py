from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,col, split, explode, year, mean, collect_list, array, lit, avg, round, concat_ws, when, lower, trim, regexp_replace, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType,LongType, IntegerType, FloatType, ArrayType, MapType, BooleanType
import os
import requests
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to initialize Spark session
def initialize_spark():
    # Creates and returns a new Spark session with the application name "TMDB Movie Data Processing"
    # The getOrCreate() method returns an existing SparkSession if one exists, otherwise creates a new one
    spark = SparkSession.builder \
        .appName("TMDB Movie Data Processing") \
        .getOrCreate()
    return spark


# Function to define schema for DataFrame to ensure proper data typing when loading from the TMDB API
def get_schema():
    # Returns a structured schema definition that maps JSON fields from the TMDB API to specific Spark data types
    # This explicit schema definition helps with data validation, optimization, and query performance
    return StructType([
        # Movie base fields - these represent the top-level properties of a movie object
        StructField("adult", BooleanType(), True),  # Flag indicating if movie is adult content (True allows null values)
        StructField("backdrop_path", StringType(), True),  # Path to backdrop image
        StructField("belongs_to_collection", MapType(StringType(), StringType()), True),  # Collection data as key-value pairs
        StructField("budget", LongType(), True),  # Movie budget as a long integer
        StructField("genres", ArrayType(MapType(StringType(), StringType())), True),  # Array of genre objects
        StructField("homepage", StringType(), True),  # Official homepage URL
        StructField("id", IntegerType(), True),  # TMDB movie ID
        StructField("imdb_id", StringType(), True),  # IMDB reference ID
        StructField("original_language", StringType(), True),  # Original language code
        StructField("original_title", StringType(), True),  # Title in original language
        StructField("overview", StringType(), True),  # Plot summary
        StructField("popularity", FloatType(), True),  # Popularity score
        StructField("poster_path", StringType(), True),  # Path to poster image
        StructField("production_companies", ArrayType(MapType(StringType(), StringType())), True),  # Companies involved
        StructField("production_countries", ArrayType(MapType(StringType(), StringType())), True),  # Countries involved
        StructField("release_date", StringType(), True),  # Release date as string
        StructField("revenue", LongType(), True),  # Box office revenue
        StructField("runtime", IntegerType(), True),  # Runtime in minutes
        StructField("spoken_languages", ArrayType(MapType(StringType(), StringType())), True),  # Languages in the film
        StructField("status", StringType(), True),  # Release status (e.g., "Released")
        StructField("tagline", StringType(), True),  # Movie tagline/slogan
        StructField("title", StringType(), True),  # Movie title
        StructField("video", BooleanType(), True),  # Video availability flag
        StructField("vote_average", FloatType(), True),  # Average rating
        StructField("vote_count", IntegerType(), True),  # Number of votes

        # Nested structure for credits information - contains cast and crew data
        StructField("credits", StructType([
            StructField("id", IntegerType(), True),  # Credits section ID (matches movie ID)
            # Cast is an array of structured objects containing actor information
            StructField("cast", ArrayType(StructType([
                StructField("adult", BooleanType(), True),  # Adult actor flag
                StructField("gender", IntegerType(), True),  # Gender code
                StructField("id", IntegerType(), True),  # Actor ID
                StructField("known_for_department", StringType(), True),  # Primary department
                StructField("name", StringType(), True),  # Actor name
                StructField("original_name", StringType(), True),  # Name in original language
                StructField("popularity", FloatType(), True),  # Actor popularity score
                StructField("profile_path", StringType(), True),  # Path to profile image
                StructField("cast_id", IntegerType(), True),  # ID within this cast list
                StructField("character", StringType(), True),  # Character name played
                StructField("credit_id", StringType(), True),  # Unique credit ID
                StructField("order", IntegerType(), True)  # Billing order
            ]), True)),
            # Crew is an array of structured objects containing crew member information
            StructField("crew", ArrayType(StructType([
                StructField("adult", BooleanType(), True),  # Adult flag
                StructField("gender", IntegerType(), True),  # Gender code
                StructField("id", IntegerType(), True),  # Crew member ID
                StructField("known_for_department", StringType(), True),  # Primary department
                StructField("name", StringType(), True),  # Crew member name
                StructField("original_name", StringType(), True),  # Name in original language
                StructField("popularity", FloatType(), True),  # Popularity score
                StructField("profile_path", StringType(), True),  # Path to profile image
                StructField("credit_id", StringType(), True),  # Unique credit ID
                StructField("department", StringType(), True),  # Department for this movie
                StructField("job", StringType(), True)  # Specific job title
            ]), True))
        ]), True),

        # Nested structure for reviews information
        StructField("reviews", StructType([
            StructField("id", IntegerType(), True),  # Reviews section ID (matches movie ID)
            StructField("page", IntegerType(), True),  # Current page of results
            # Reviews results as an array of structured objects
            StructField("results", ArrayType(StructType([
                StructField("author", StringType(), True),  # Review author name
                # Nested author details
                StructField("author_details", StructType([
                    StructField("name", StringType(), True),  # Author full name
                    StructField("username", StringType(), True),  # Author username
                    StructField("avatar_path", StringType(), True),  # Path to avatar image
                    StructField("rating", FloatType(), True)  # Author's rating
                ]), True),
                StructField("content", StringType(), True),  # Review text content
                StructField("created_at", StringType(), True),  # Creation timestamp
                StructField("id", StringType(), True),  # Review ID
                StructField("updated_at", StringType(), True),  # Last update timestamp
                StructField("url", StringType(), True)  # URL to the review
            ]), True)),
            StructField("total_pages", IntegerType(), True),  # Total pages of reviews
            StructField("total_results", IntegerType(), True)  # Total number of reviews
        ]), True)
    ])

# Function to fetch data for a single movie from the TMDB API
def fetch_single_movie(movie_id, headers):
    # Constructs API URL with movie_id and appends reviews and credits to the response
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=reviews,credits"
    try:
        # Makes HTTP GET request with authorization headers and a 10-second timeout
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # Logs success message and returns JSON data if request is successful
            logging.info(f"Successfully fetched movie ID: {movie_id}")
            return response.json()
        else:
            # Logs warning if request was unsuccessful (e.g., 404 not found)
            logging.warning(f"Failed to fetch movie ID: {movie_id}, Status code: {response.status_code}")
            return None
    except Exception as e:
        # Catches and logs any exceptions that occur during the request (network errors, timeouts, etc.)
        logging.error(f"Error fetching movie ID {movie_id}: {e}")
        return None

# Function to fetch data for multiple movies in parallel using a thread pool
def fetch_movie_data(movie_ids, api_token, max_workers=10):
    # Sets up authorization headers with the API token
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    movies_data = []  # List to store successfully fetched movie data

    # Creates a thread pool with specified number of workers (default 10)
    # This enables parallel API requests to improve throughput
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Maps each movie_id to a future that will execute fetch_single_movie
        future_to_id = {executor.submit(fetch_single_movie, movie_id, headers): movie_id for movie_id in movie_ids}
        
        # Processes completed futures as they finish (doesn't wait for all to complete)
        for future in as_completed(future_to_id):
            movie_data = future.result()  # Gets the result from the future
            if movie_data:  # If data was successfully fetched
                movies_data.append(movie_data)  # Add to the results list

    return movies_data  # Returns list of all successfully fetched movie data


# Function for loading the saved parquet data 
def load_from_parquet(file_path, spark):
    # Check if the specified parquet file exists
    if os.path.exists(file_path):
        # Load the parquet file into a Spark DataFrame
        df = spark.read.parquet(file_path)
        print(f"Loaded data from {file_path}")
        return df
    else:
        # Return None if file doesn't exist
        print(f"File not found: {file_path}")
        return None


# Function to save DataFrame to Parquet format
def save_to_parquet(df, file_path):
    # Write DataFrame to parquet with overwrite mode (replaces existing file)
    df.write.mode("overwrite").parquet(file_path)
    print(f"Data saved to {file_path}")



#DEFINING FUNCTIONS TO CLEAN AND TRANSFORM DATA

# Main process to fetch and save movie data
def process_movie_data(spark, movie_ids=None):
    # Configure logging to track process execution
    logging.basicConfig(level=logging.INFO)

    # Define path for raw movie data storage
    parquet_file_path = "./dataset/raw/raw_tmdb_movies.parquet"
    # Get schema for movie data structure
    schema = get_schema()

    # Try to load existing data first to avoid re-fetching
    raw_movies_df = load_from_parquet(parquet_file_path, spark)

    # If data already exists, return it immediately
    if raw_movies_df is not None:
        logging.info("Parquet file loaded successfully.")
        return raw_movies_df

    # If no data exists, get API key from environment variables
    api_token = os.getenv("TMDB_API_KEY")
    if not api_token:
        # Raise error if API key is missing
        raise EnvironmentError("TMDB_API_KEY not found in environment variables.")

    # If no movie IDs provided, use this default list of popular movies
    if movie_ids is None:
        movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
                     420818, 24428, 168259, 99861, 284054, 12445,
                     181808, 330457, 351286, 109445, 321612, 260513]

    # Fetch movie data from API using parallel processing
    movies_data = fetch_movie_data(movie_ids, api_token)

    # Verify we have data to process
    if not movies_data:
        raise ValueError("No movie data retrieved. Cannot create DataFrame.")

    try:
        # Create DataFrame with the retrieved data using the defined schema
        raw_movies_df = spark.createDataFrame(movies_data, schema)
        # Save data to avoid future API calls
        save_to_parquet(raw_movies_df, parquet_file_path)
        logging.info("DataFrame created and saved to Parquet.")
    except Exception as e:
        # Log any errors during DataFrame creation/saving
        logging.error(f"Failed to create/save DataFrame: {e}")
        raise

    # Return the raw DataFrame for further processing
    return raw_movies_df




# Function to extract movie columns from nested data
def get_movie_columns(df):
    """Extract additional columns from nested credits and reviews data"""
    
    logging.info("Starting to extract movie columns.")

    try:
        # Extract cast_size - count number of cast members
        df = df.withColumn("cast_size", F.size(F.col("credits.cast")))
        
        # Extract crew_size - count number of crew members
        df = df.withColumn("crew_size", F.size(F.col("credits.crew")))

        # Extract directors - filter crew members with 'Director' job and get their names
        df = df.withColumn("directors", F.expr("transform(filter(credits.crew, c -> c.job = 'Director'), d -> d.name)"))

        # Extract cast names - transform cast objects to just their names
        df = df.withColumn("cast", F.expr("transform(credits.cast, c -> c.name)"))

        # Calculate average rating from reviews - complex aggregation:
        # 1. Filter reviews with non-null ratings
        # 2. Sum up all ratings
        # 3. Divide by count of non-null ratings
        # 4. Round to 2 decimal places
        df = df.withColumn("rating", 
                           F.when(F.size(F.expr("filter(reviews.results, r -> r.author_details.rating is not null)")) > 0,
                                  F.round(F.expr(""" 
                                      aggregate(
                                          filter(reviews.results, r -> r.author_details.rating is not null), 
                                          cast(0 as double), 
                                          (acc, r) -> acc + cast(r.author_details.rating as double), 
                                          sum -> sum / size(filter(reviews.results, r -> r.author_details.rating is not null))
                                      )
                                  """), 2)
                           ).otherwise(None))

        logging.info("Movie columns extraction completed successfully.")
        return df

    except Exception as e:
        # Log any errors during column extraction
        logging.error(f"Error during movie columns extraction: {e}")
        raise

# Function to clean movie data
def clean_movie_data(df, pipe_columns=None, collection_col=None, columns_to_drop=None):
    """Clean movie data with essential transformations"""
    
    logging.info("Starting data cleaning process.")

    try:
        # Process pipe-separated columns - converts arrays of objects to pipe-delimited strings
        # Example: [{name:"Action"}, {name:"Adventure"}] becomes "Action|Adventure"
        if pipe_columns:
            for col_name in pipe_columns:
                if col_name in df.columns:
                    df = df.withColumn(col_name, 
                                      F.concat_ws("|", F.expr(f"transform({col_name}, x -> x.name)")))

        # Process collection name - extracts name field from collection object
        # Example: {id:12345, name:"Star Wars Collection"} becomes "Star Wars Collection"
        if collection_col and collection_col in df.columns:
            df = df.withColumn(collection_col, 
                               F.col(f"{collection_col}.name"))
        
        # Drop columns that aren't needed for analysis
        if columns_to_drop:
            df = df.drop(*columns_to_drop)

        logging.info("Data cleaning completed successfully.")
        return df

    except Exception as e:
        # Log any errors during data cleaning
        logging.error(f"Error during data cleaning: {e}")
        raise


# FUNCTION TO HANDLE INCORRECT AND MISSING DATA
def handle_incorrect_data(extracted_df, numeric_columns=None, money_columns=None, placeholders=None):
    """Handle incorrect or missing data in movie dataset"""
    
    logging.info("Starting data correction process.")

    try:
        # Replace zeros with null for numeric columns (zeros often indicate missing data)
        for col_name in numeric_columns:
            extracted_df = extracted_df.withColumn(
                col_name,
                F.when((F.col(col_name).isNotNull()) & (F.col(col_name) != 0), F.col(col_name)).otherwise(None)
            )

        # Convert money columns from dollars to millions for easier reading/analysis
        for col_name in money_columns:
            if col_name in extracted_df.columns:
                extracted_df = extracted_df.withColumn(col_name, F.col(col_name) / 1000000)

        # Rename money columns to include _musd suffix (millions USD)
        for old_name in money_columns:
            new_name = f"{old_name}_musd"
            extracted_df = extracted_df.withColumnRenamed(old_name, new_name)

        # Process string columns to replace placeholders with NaN
        # First get all columns that are string type
        string_cols = [f.name for f in extracted_df.schema.fields if isinstance(f.dataType, StringType)]

        # Replace placeholder values (like "N/A", "", etc.) with null
        for col_name in string_cols:
            extracted_df = extracted_df.withColumn(
                col_name,
                F.when(~F.col(col_name).isin(placeholders) & F.col(col_name).isNotNull(),
                      F.col(col_name)).otherwise(None)
            )
        
        # Save the final processed data for future use
        extracted_df.write.mode("overwrite").parquet("./dataset/processed/tmdb_movies_processed.parquet")
        logging.info("Processed movie data saved to tmdb_movies_processed.parquet")

        return extracted_df

    except Exception as e:
        # Log any errors during data correction
        logging.error(f"Error during data correction: {e}")
        raise


#FUNCTION TO REORDER COLUMNS
def reorder_columns(df, desired_order):
    """Reorders columns in a DataFrame according to a desired order and caches the result."""
    
    logging.info("Starting to reorder columns.")

    try:
        # Get set of columns that actually exist in the DataFrame
        existing_columns = set(df.columns)
        
        # Create final column list:
        # 1. First include columns from desired_order that exist in DataFrame
        # 2. Then add any remaining columns not in desired_order
        final_columns = [col for col in desired_order if col in existing_columns] + \
                        [col for col in df.columns if col not in set(desired_order)]

        # Select columns in the new order
        df = df.select(*final_columns)
        # Cache DataFrame in memory for faster repeated access
        df.cache()

        logging.info("Column reordering completed successfully.")
        return df

    except Exception as e:
        # Log any errors during column reordering
        logging.error(f"Error during column reordering: {e}")
        raise



# FUNCTION TO GET MOVIE RANKINGS
def get_movie_rankings(df, spark):
    """Generate rankings for movies based on various metrics"""
    
    logging.info("Starting movie rankings generation.")

    try:
        # Register DataFrame as a temporary SQL view
        df.createOrReplaceTempView("movies")
        
        # Dictionary of ranking names and their corresponding SQL queries
        queries = {
            "Highest Revenue": "SELECT * FROM movies ORDER BY revenue_musd DESC",
            "Highest Budget": "SELECT * FROM movies ORDER BY budget_musd DESC",
            "Highest Profit": "SELECT * FROM movies ORDER BY profit DESC",
            "Lowest Profit": "SELECT * FROM movies ORDER BY profit ASC",
            "Highest ROI (Budget ≥ 10M)": "SELECT * FROM movies WHERE budget_musd >= 10 ORDER BY roi DESC",
            "Lowest ROI (Budget ≥ 10M)": "SELECT * FROM movies WHERE budget_musd >= 10 ORDER BY roi ASC",
            "Most Voted Movies": "SELECT * FROM movies ORDER BY vote_count DESC",
            "Highest Rated Movies (≥10 votes)": "SELECT * FROM movies WHERE vote_count >= 10 ORDER BY rating DESC",
            "Lowest Rated Movies (≥10 votes)": "SELECT * FROM movies WHERE vote_count >= 10 ORDER BY rating ASC",
            "Most Popular Movies": "SELECT * FROM movies ORDER BY popularity DESC",
            "Shortest Runtime Movies": "SELECT * FROM movies ORDER BY runtime ASC"
        }

        # Execute each query and store results in a dictionary
        rankings = {}
        for name, query in queries.items():
            rankings[name] = spark.sql(query)

        logging.info("Movie rankings generated successfully.")
        return rankings

    except Exception as e:
        # Log any errors during rankings generation
        logging.error(f"Error during rankings generation: {e}")
        raise

# THIS FUNCTION RETURNS THE PERFORMANCE OF FRANCHISE MOVIES BY THE METRICS/STATS SPECIFIED AS PARAMETERS
"""
    if no parameter is specified it filters based on budget,revenue and rating
"""
def franchise_stats(spark, *, budget=None, revenue=None, rating=None, limit=10):
    logging.info("Running franchise stats...")

    # If no specific metrics are requested, use all three default metrics
    if all(v is None for v in [budget, revenue, rating]):
        budget = revenue = rating = True

    # Start with base fields for all queries
    fields = ["belongs_to_collection", "COUNT(*) AS movie_count"]

    # Add budget-related fields if requested
    if budget:
        fields += [
            "SUM(budget_musd) AS total_budget",
            "AVG(budget_musd) AS avg_budget"
        ]
    # Add revenue-related fields if requested
    if revenue:
        fields += [
            "SUM(revenue_musd) AS total_revenue",
            "AVG(revenue_musd) AS avg_revenue"
        ]
    # Add rating field if requested
    if rating:
        fields.append("AVG(vote_average) AS avg_rating")

    # Join all fields with commas for SQL query
    select_clause = ",\n    ".join(fields)
    
    # Determine sort order - default to total_revenue if available, otherwise movie_count
    order_by = "total_revenue" if revenue else "movie_count"

    # Construct and execute final SQL query
    query = f"""
        SELECT 
            {select_clause}
        FROM movies
        WHERE belongs_to_collection IS NOT NULL
        GROUP BY belongs_to_collection
        ORDER BY {order_by} DESC
        LIMIT {limit}
    """
    return spark.sql(query)

# THIS FUNCTION RETURNS THE PERFORMANCE OF DIRECTORS BY THE METRICS/STATS SPECIFIED AS PARAMETERS
"""
    if no parameter is specified it filters based on budget,revenue and rating
"""
def director_stats(spark, *, revenue=None, rating=None, limit=10):
    logging.info("Running director stats...")

    # If no specific metrics are requested, use both default metrics
    if all(v is None for v in [revenue, rating]):
        revenue = rating = True

    # Start with base fields for all queries
    fields = ["directors", "COUNT(*) AS movie_count"]

    # Add revenue-related field if requested
    if revenue:
        fields.append("SUM(revenue_musd) AS total_revenue")
    # Add rating field if requested
    if rating:
        fields.append("AVG(vote_average) AS avg_rating")

    # Join all fields with commas for SQL query
    select_clause = ",\n    ".join(fields)
    
    # Determine sort order - default to total_revenue if available, otherwise movie_count
    order_by = "total_revenue" if revenue else "movie_count"

    # Construct and execute final SQL query
    query = f"""
        SELECT 
            {select_clause}
        FROM movies
        WHERE directors IS NOT NULL
        GROUP BY directors
        ORDER BY {order_by} DESC
        LIMIT {limit}
    """
    return spark.sql(query)
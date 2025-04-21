## TMDB_MOVIE ANALYSIS USING PANDAS AND MATPLOTLIB
This project challenges you to build a movie data analysis pipeline using Python and Pandas.
You will fetch movie-related data from an API, clean and transform the dataset, and implement
key performance indicators (KPIs).


### Table of content
+ Step 1: Fetch Movie Data from API
+ Step 2: Data Cleaning and Preprocessing 
    - handling Missing & Incorrect Data
+ Step 3: KPI Implementation & Analysis 
    -  Identify the Best/Worst Performing Movies
+ Step 4: Data Visualization
    - Revenue vs. Budget Trends using scatter plot
    - ROI Distribution by Genre using bar graph(exploded genre column for accurate analysis)
    - Popularity vs. Rating using scatter plot
    - Yearly Trends in Box Office Performance using line chart (average revenue as performance metric)
    - Comparison of Franchise vs. Standalone Success using bar chart 


### Key Findings
+ While fetching data append_to_json helped retrieve corresponding credits and review data for movie details
+ retrieved credits and review to help extract other key columns missing in the movie details data
    -  ratings,cast,cast_size were some of the columns needed from the credits and review data

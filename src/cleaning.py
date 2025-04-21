
import pandas as pd
from typing import Dict, List, Optional


#custom function to drop columns from dataframe for specified list of columns as parameter 
def drop_irrelevant_columns(df, columns_to_drop):
    return df.drop(columns=columns_to_drop, errors='ignore')



def get_movie_columns( df: pd.DataFrame) -> pd.DataFrame:
    
    """
    this function is used to generate extract the cast_size, crew_size, directors, cast and rating from appended data(credits and reviews)
    """
    df = df.copy() #creates a copy of the df pararmeter to avoid errors
    
    # extracts ast_size,crew_size, directors,cast and ratings 
    df['cast_size'] = df['credits'].apply(lambda x: len(x.get('cast', [])))
    df['crew_size'] = df['credits'].apply(lambda x: len(x.get('crew', [])))
    df['directors'] = df['credits'].apply(
                                          lambda x: [p['name'] for p in x.get('crew', []) 
                                          if p.get('job') == 'Director'] 
    )
    # Extract names of cast members into a new column
    df['cast'] = df['credits'].apply(
                                     lambda credit: [person['name'] for person in credit.get('cast', []) if 'name' in person]
    )

    
    # Rating calculation with 2 decimal places formatting
    def avg_rating(reviews: Dict) -> Optional[float]:
        """Calculate and format average rating to 2 decimalplaces"""
        if not isinstance(reviews, dict) or 'results' not in reviews:
            return None
        
        ratings = [
            r['author_details'].get('rating') #extracts and adds to itself the rating key from the author_column key of the reviews data since it's a list
            for r in reviews['results']
            if r['author_details'].get('rating') is not None
        ]
        
        if not ratings:
            return None
            
        return round(sum(ratings)/len(ratings),2)
        
    
    df['rating'] = df['reviews'].apply(avg_rating)

    
    return df



"""This combines the drop_irrelevant_columns, get_movie_columns functions 
   extracts the desired values from the json-like columns of the dataframe while separating the values of specified ones with pipes

 """ 
def clean_movie_data(df, pipe_columns=None, collection_col=None, columns_to_drop=None):
    """Cleans movie data with essential transformations"""
    """
    
    :param df: Input DataFrame
    :param pipe_columns: List of columns to convert to pipe-separated strings
    :param collection_col: Column containing collection dict (extracts 'name')
    :return: Cleaned DataFrame
    """
    df = drop_irrelevant_columns(df, columns_to_drop) #calls the drop_irrelevant_columns() function
    df = df.copy()
    
  
    # Process pipe-separated columns
    if pipe_columns:
        for col in pipe_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: '|'.join(str(i.get('name', '')) for i in x) 
                    if isinstance(x, list) else ''
                )
    
    # Process collection name
    if collection_col and collection_col in df.columns:
        df[collection_col] = df[collection_col].apply(
            lambda x: x.get('name') if isinstance(x, dict) else None
        )
    print("Data cleaning completed.")
    return df 
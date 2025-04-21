
"""
this function returns a the ranked versions of the specified columns of the dataframe
df is the dataframe
asc = False sets the sorting to descending order
min_votes and min_budget are parameters help filter ranked df based on minimum number of votes or budget
"""
def rank_movies(df, by, asc=False, min_votes=0, min_budget=0):
    df = df.copy()
    if min_votes: df = df[df.vote_count >= min_votes]
    if min_budget: df = df[df.budget_musd >= min_budget]
    
    # a dictionaryof columns to be ranked 
    sort_col = {
        'revenue': 'revenue_musd',
        'budget': 'budget_musd', 
        'profit': 'profit',
        'roi': 'roi',
        'votes': 'vote_count',
        'rating': 'rating',
        'popularity': 'popularity',
        'runtime': 'runtime'
    }[by]
    
    return df.sort_values(sort_col, ascending=asc) 



#User-defined function that returns a list of dataframes by passing the dictionary of parameters to the ranked_movies function in an iterative manner
def get_movie_rankings(df):
    """Generate all requested movie rankings."""
    
    #dictionary of parameters
    ranking_scenarios = [
        {'name': 'Highest Revenue', 'by': 'revenue', 'asc': False},
        {'name': 'Highest Budget', 'by': 'budget', 'asc': False},
        {'name': 'Highest Profit', 'by': 'profit', 'asc': False},
        {'name': 'Lowest Profit', 'by': 'profit', 'asc': True},
        {'name': 'Highest ROI (Budget ≥ 10M)', 'by': 'roi', 'asc': False, 'min_budget': 10_000_000},
        {'name': 'Lowest ROI (Budget ≥ 10M)', 'by': 'roi', 'asc': True, 'min_budget': 10_000_000},
        {'name': 'Most Voted Movies', 'by': 'votes', 'asc': False},
        {'name': 'Highest Rated Movies (≥10 votes)', 'by': 'rating', 'asc': False, 'min_votes': 10},
        {'name': 'Lowest Rated Movies (≥10 votes)', 'by': 'rating', 'asc': True, 'min_votes': 10},
        {'name': 'Most Popular Movies', 'by': 'popularity', 'asc': False},
        {'name': 'Shortest Runtime Movies', 'by': 'runtime', 'asc': True}
    ]
    
    #list to store newly ranked dataframe after every iteration
    rankings = {}
    
    #passing parameters to ranked_movies() in iteration
    for scenario in ranking_scenarios:
        params = {
            'by': scenario['by'],
            'asc': scenario.get('asc', False),
            'min_votes': scenario.get('min_votes', 0),
            'min_budget': scenario.get('min_budget', 0)
        }
        ranked_df = rank_movies(df, **params)
        rankings[scenario['name']] = ranked_df
        
    return rankings
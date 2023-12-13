import pandas as pd
import pdcast as pdc   # Firstly you should run 'pip install pandas-downcast'


def df_optimize(df) :
        """Function to down cast pandas entries types into smaller versions in order to optimize space in the used dataframes
    returns the resulting dataframe
    """
        
        df = pdc.downcast(df)
        return df 

def df_optimize_path(df_path :str) :

    """Function to down cast pandas entries types into smaller versions in order to optimize space in the used dataframes
    returns the resulting dataframe
    """
 
    df = pd.read_csv(df_path)
    a =  df.memory_usage().sum()
    df = pdc.downcast(df)
    b = df.memory_usage().sum()
    print("Memory saved by : " ,((1-b/a) * 100),"%")
    
    return df

def is_unique_key(df : pd.DataFrame ,columnName : str) :

    """Function to check if column is a primary key in the given dataframe    
    returns true if the entry is primary key, false otherwise
    """

    x = df.shape[0]
    l = df[columnName].drop_duplicates().shape[0]

    return x == l  

def df_aggreg(df ,on ,aggreg_dict) : 
     """Function to do an aggregation on Dataframe columns on a specific function given in the aggreg_dict"""
     df = df.groupby(on).agg(aggreg_dict).reset_index()
     df.columns = [f'{col[0]}_{col[1].lower()}' if isinstance(col, tuple) else col.lower() for col in df.columns]
     df.rename(columns = {"sk_id_prev_": "sk_id_prev", "sk_id_curr_first" : "sk_id_curr","sk_id_curr_" : "sk_id_curr","sk_id_bureau_":"sk_id_bureau"},inplace=True)
     return df
     

def df_aggreg2(df : pd.DataFrame ,df_rename) : 

    """Function to perform an aggregation function on the DataFrame in order to have unique ids"""
    
    df_count = df[['sk_id_curr', 'sk_id_prev']].groupby('sk_id_curr').count()


    df['sk_id_prev'] = df['sk_id_curr'].map(df_count['sk_id_prev'])

    df_avg = df.groupby('sk_id_curr').mean()

    #Renaming columns

    df_avg.columns = [df_rename +'_' + col for col in df_avg.columns]


    return df_avg

def previous_credits_aggreg(df : pd.DataFrame) :

    """Function to perform an aggregation function on the DataFrame in order to have unique ids"""

    #group by id and perform an aggregation function to have unique ids
    df_avg = df.groupby('sk_id_curr').mean()
    df_avg['p_count'] = df[['sk_id_bureau','sk_id_curr']].groupby('sk_id_curr').count()['sk_id_bureau']
    #Rename columns
    df_avg.columns = ['previous_credits_' + col for col in df_avg.columns]
    return df_avg   

def previous_credits_aggreg_with_curr(df : pd.DataFrame) :

    """Function to perform an aggregation function on the DataFrame in order to have unique ids"""

    #group by id and perform an aggregation function to have unique ids
    df_avg = df.groupby('sk_id_curr').mean()
    df_avg['p_count'] = df[['sk_id_bureau','sk_id_curr']].groupby('sk_id_curr').count()['sk_id_bureau']
    #Rename columns
    df_avg.columns = ['previous_credits_' + col for col in df_avg.columns]
    df_avg['sk_id_curr'] = df_avg.index
    return df_avg 

def credit_bureau_balance_aggreg(df : pd.DataFrame) :

    """Function to perform an aggregation function on the DataFrame in order to have unique ids"""

    #group by id and perform an aggregation function to have unique ids
    df_count = df['sk_id_bureau'].groupby('sk_id_bureau').count()


    df['sk_id_bureau'] = df['sk_id_bureau'].map(df_count['sk_id_bureau'])

    df_avg = df.groupby('sk_id_bureau').mean()

    #Renaming columns

    df_avg.columns = ['credit_bureau_balance_' + col for col in df_avg.columns]
    return df_avg
      
def last(x) : 
    """Function to get the last in the DataFrame"""

    return x.iloc[-1] 


def getSection1() :
    """Funtion to generate Section 1 as used in the merge previously"""

    section1 = df_optimize_path("Dataset/loan_applications_train.csv")
    section1 = pd.get_dummies(section1, columns=section1.select_dtypes(include=['category']).columns)
    print("Section 1 Loaded")
    
    return section1


def getSection2() :
    """Funtion to generate Section 2 as used in the merge previously"""

    # Loading data
    previous_pos_cash_loans=df_optimize_path("Dataset/previous_POS_cash_loans.csv")
    previous_credit_cards=df_optimize_path("Dataset/previous_credit_cards.csv")
    previous_loan_applications=df_optimize_path("Dataset/previous_loan_applications.csv")
    repayment_history=df_optimize_path("Dataset/repayment_history.csv")


    #Converting categorical variable into dummy variables.
    previous_pos_cash_loans = pd.get_dummies(previous_pos_cash_loans, columns=previous_pos_cash_loans.select_dtypes(include=['category']).columns)
    previous_credit_cards = pd.get_dummies(previous_credit_cards, columns=previous_credit_cards.select_dtypes(include=['category']).columns)
    previous_loan_applications = pd.get_dummies(previous_loan_applications, columns=previous_loan_applications.select_dtypes(include=['category']).columns)
    repayment_history = pd.get_dummies(repayment_history, columns=repayment_history.select_dtypes(include=['category']).columns)

    #Groupby and Aggregation
    previous_credit_cards = df_aggreg2(previous_credit_cards,"previous_credit_cards")
    previous_pos_cash_loans = df_aggreg2(previous_pos_cash_loans,"previous_pos_cash_loans")
    previous_loan_applications = df_aggreg2(previous_loan_applications,"previous_pos_cash_loans")
    repayment_history = df_aggreg2(repayment_history,"repayment_history")



    #Inplace merging
    section2 = pd.merge( previous_loan_applications,previous_credit_cards ,on = "sk_id_curr",how = "left")
    section2 = pd.merge(section2,previous_pos_cash_loans ,on="sk_id_curr",how="left")
    section2 = pd.merge(section2,repayment_history,on="sk_id_curr",how='left')

    print("Section 2 Loaded")


    return section2



def getSection3() :
    """Funtion to generate Section 3 as used in the merge previously"""


    # Loading data
    previous_credits = df_optimize_path("Dataset/previous_credits.csv")
    credit_bureau_balance=df_optimize_path("Dataset/credit_bureau_balance.csv")

    previous_credits = pd.get_dummies(previous_credits, columns=previous_credits.select_dtypes(include=['category']).columns)
    credit_bureau_balance = pd.get_dummies(credit_bureau_balance, columns=credit_bureau_balance.select_dtypes(include=['category']).columns)

    #Converting categorical variable into dummy variables.
    aggregations_credit_bureau_balance = {
        'sk_id_bureau' : 'first',
        'months_balance': ['min', 'max'],
        'status_0': 'last',
        'status_1': 'last',
        'status_2': 'last',
        'status_3': 'last',
        'status_4': 'last',
        'status_5': 'last',
        'status_C': 'last',
        'status_X': 'last'
    }
    credit_bureau_balance = df_aggreg(credit_bureau_balance,'sk_id_bureau',aggregations_credit_bureau_balance)
    previous_credits = previous_credits_aggreg_with_curr(previous_credits)

    #Inplace merging
    
    previous_credits=previous_credits.rename(columns={"previous_credits_sk_id_bureau" : "sk_id_bureau"})
    section3 = pd.merge(previous_credits,credit_bureau_balance,on="sk_id_bureau",how="left")


    print("Section 3 Loaded")


    return section3
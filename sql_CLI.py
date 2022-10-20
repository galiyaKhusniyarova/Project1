import fire
import questionary
import sqlalchemy
import pandas as pd
from pathlib import Path

# Create a temporary sqlite database
database_connection_string = 'sqlite:///esg.db'

# Create an engine to interact with the database
engine = sqlalchemy.create_engine(database_connection_string)

"""READ

The READ operation will read the entire table from the database into a new DataFrame.
Then it will print the DataFrame.
"""


def read_table(engine, table_name):
    results_dataframe = pd.read_sql_table(table_name, con=engine)
    print(f"{table_name} Data:")
    print(results_dataframe)
    
    

def run():
    # Create a dataframe
    esg_df = pd.read_csv('./resources/esg_data.csv')
    sp_500_df = pd.read_csv('./resources/SP500_stocks.csv')

    #Create tables
    esg_df.to_sql("esg", engine, index=False, if_exists='replace')
    sp_500_df.to_sql("sp_500", engine, index = False, if_exists='replace')

    table_name = questionary.text("Which table would you like to read?").ask()
    read_table(engine, table_name)

if __name__ == "__main__":
    fire.Fire(run)

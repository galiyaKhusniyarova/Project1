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
    
"""CREATE

The CREATE operation creates a new table in the database using the given DataFrame.
The table is replaced by the new data if it already exists.
"""


def create_table(engine, table_name, table_data_df):
    table_data_df.to_sql(table_name, engine, index=True, if_exists='replace')
    
"""SELECT"""


def select_data(engine, table_name):
    select_sql = """
    SELECT {table_name} from dcf_all
    """
    

def run():
    
    res_df = pd.read_csv('./FAANG_DATA/res.csv')
    res_df.set_index('symbol', inplace = True)
    res_df.to_csv('res2.csv')
    
    #Create original table
    create_table(engine, 'dcf_all', res_df)
    
    
    sql_cli_running = True
    
    while sql_cli_running:
        choice = questionary.select(
            "What do you want to do?",
            choices=["Read", "Merge", "Save", "Quit"]
        ).ask()
        
        if choice == "Read":
            table_name = questionary.select(
                "Which table would you like to read?",
                choices = ["META", "AAPL", "AMZN", "GOOG"]
            ).ask()
            if table_name == "META":
                select_data(engine, table_name)
                read_table(engine, table_name)
        elif choice == "Quit":
            sql_cli_running = False
            print("Goodbye")

if __name__ == "__main__":
    fire.Fire(run)
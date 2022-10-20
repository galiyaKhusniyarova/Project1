import fire
import questionary
import sqlalchemy
import pandas as pd
from pathlib import Path
import os

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
    return results_dataframe
    
    
"""CREATE

The CREATE operation creates a new table in the database using the given DataFrame.
The table is replaced by the new data if it already exists.
"""


def create_table(engine, table_name, table_data_df):
    table_data_df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"The table {table_name} was created")
    
    
"""SELECT

The SELECT operation creates a new table out of existing one.
"""


def select_data(engine, table_name):
    
    select_sql = f"""
    SELECT Indicators, {table_name} from dcf_all
    """
    table_name_df = pd.read_sql_query(select_sql, con=engine)
    create_table(engine, table_name, table_name_df)


"""MERGE

The MERGE operation merges 2 tables into one. 
Then it will print the DataFrame.
"""

def merge_tables(engine, table_name_1, table_name_2):
    
    table_name_1_df = pd.read_sql_table(table_name_1, con=engine)
    table_name_2_df = pd.read_sql_table(table_name_2, con=engine)
    res_df = table_name_1_df.merge(table_name_2_df, how='inner', on='Indicators')
    table_name = f"{table_name_1}&{table_name_2}"
    create_table(engine, table_name, res_df)
    
    read_table(engine, table_name)


"""SAVE

The SAVE operation saves converts the table into the dataFrame and saves it as a csv file.
"""    

def save_to_csv(table_name, csv_path):
    res_df = pd.read_sql_table(table_name, con=engine)
    res_df.to_csv(f'{csv_path}', index = False)
    
    print(f"CSV file successfully created and uploaded to {csv_path}!")

    
def run():
    # Dataframe view options
    pd.set_option('display.float_format', '{:.2f}'.format)
    
    # Read the csv file
    choice = questionary.select(
        "Which CSV file do you want to read?",
        choices = ["default", "my file"]
    ).ask()
    if choice == "default":
        csv_path = "./FAANG_DATA/res.csv"
        res_df = pd.read_csv(f'{csv_path}')
    
        res_df = res_df.rename(columns={"Unnamed: 0":"Indicators"})
    
        # Print the original DataFrame
        print(res_df.head())
    
        #Create original(default) tables
        create_table(engine, 'dcf_all', res_df)
        select_data(engine, "AAPL")
        select_data(engine, "META")
        select_data(engine, "AMZN")
        select_data(engine, "GOOG")
    else:
        csv_path = questionary.text(
            "Enter the path to csv.file:"
        ).ask()
    
        res_df = pd.read_csv(f'{csv_path}')
        # Print the original DataFrame
        print(res_df.head())
    
 
    
    sql_cli_running = True
    
    while sql_cli_running:
        choice = questionary.select(
            "What do you want to do?",
            choices=["Read", "Merge", "Save", "Quit"]
        ).ask()
        
        if choice == "Read":
            table_name = questionary.select(
                "Which table would you like to read?",
                choices = engine.table_names()
            ).ask()
            select_data(engine, table_name)
            read_table(engine, table_name)
            
        elif choice == "Merge":
            table_name_1 = questionary.select(
                "Choose table 1: ",
                choices = engine.table_names()
            ).ask()
            table_name_2 = questionary.select(
                "Choose table 2: ",
                choices = engine.table_names()
            ).ask()
            merge_tables(engine, table_name_1, table_name_2)
            
        elif choice == "Save":
            table_name = questionary.select(
                "Which table do you want to save?",
                choices = engine.table_names()
            ).ask()
            csv_path = questionary.text(
                "Enter the path for new csv.file: ").ask()
            save_to_csv(table_name, csv_path)
            
        elif choice == "Quit":
            questionary.text("Created tables will be deleted.").ask()
            sql_cli_running = False
            print("Goodbye")

            
    # Delete the esg.db file
    path = 'esg.db'
    os.remove(path)
    print("all data cleaned")
    
if __name__ == "__main__":
    fire.Fire(run)
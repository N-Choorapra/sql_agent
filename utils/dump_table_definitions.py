from ruamel.yaml import YAML
import pandas, os
import pyodbc
from argparse import ArgumentParser

SUCCESS_MESSAGE = "---Your {database}.{table} table has been dumper to the table_definitions.yaml file successfully---"
FAILURE_MESSAGE = "Please define all the arguments arguments. For help\n python dump_table_definitions.py -h"

parser = ArgumentParser()

parser.add_argument("-db", "--database", type=str, help="name of the database")
parser.add_argument("-t", "--table", type=str, help="name of the table")
parser.add_argument("-d", "--description", type=str, help="The detailed description of the table. Make it understandable to the AI so that the result produced would be accurate. Mention the behaviour of the fields on each record")


class DumpDefs:
    def __init__(self) -> None:
        self._yaml = YAML()
        with open(os.path.join("..", "config", "db_config.yaml"), "r") as f:
            self._db_config = self._yaml.load(f)["db_config"]
            
        username = self._db_config["user"]
        password = self._db_config["password"]
        server = self._db_config["host"]
        database = self._db_config["database"]

        conn_str = (
                r'DRIVER={ODBC Driver 17 for SQL Server};'
                r'SERVER=' + server + ';'
                r'DATABASE=' + database + ';'
                r'UID=' + username + ';'
                r'PWD=' + password + ';'
            )
        self.connection = pyodbc.connect(conn_str)

        with open(os.path.join("..", "config", "table_definitions.yaml"), "r") as f:
            self.existing_table = self._yaml.load(f)
    
    def dump_tables(self, 
                    database: str, 
                    table: str,
                    description: str
                    ) -> None:
        
        """
        dumps the column:datatype structure of the given table to the table_definitions.yaml file in the config folder
        ---------------------------------------------------------------------------------------
        Args:
            database: Name of the database
            table: name of the table
        Returns: None
        """

        query = f"""SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.columns
                    WHERE TABLE_CATALOG = '{database}'
                        AND TABLE_NAME = '{table}'"""
        
        result = pandas.read_sql_query(query, self.connection)
        result = result.set_index("COLUMN_NAME")["DATA_TYPE"].to_dict()
        self.existing_table["tables"].update({table:{
                                                    "description": description,
                                                    "fields":result}}) # Matching to the structure of table definitions
        with open(os.path.join("..", "config", "table_definitions.yaml"), "w") as f:
            self._yaml.dump(self.existing_table, f)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.database and args.table and args.description: #Checks if the arguments were defined properly
        dumper = DumpDefs()
        dumper.dump_tables(database=args.database, table=args.table, description = args.description)
        print(SUCCESS_MESSAGE.format(database = args.database, table = args.table))
    else:
        print(FAILURE_MESSAGE)
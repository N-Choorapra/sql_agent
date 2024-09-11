from typing import Optional, Dict
from openai import AsyncOpenAI, OpenAI
import os, json, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ruamel.yaml import YAML
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
# from sqlalchemy.exc import ProgrammingError
import pyodbc
import pandas

ENV_PATH = os.path.join("config", ".env")
load_dotenv(ENV_PATH)


class AsyncSQLAdapter(AsyncOpenAI):
    def __init__(
            self
            ,*args,
            **kwargs
            ) -> None:
        super().__init__(*args, **kwargs)

        _yaml = YAML()
        _table_definitions_path = os.path.join("..", "config", "table_definitions.yaml")
        _db_config_path = os.path.join("..", "config", "db_config.yaml")

        
        with open(_table_definitions_path, "r") as f:
            self.table_schema = _yaml.load(f)
        
        with open(_db_config_path, "r") as f:
            self.db_config = _yaml.load(f)

    def table_name(
            self,
            intent: str
            ) -> Optional[str]:
        
        """Identified the table name from the intent : table mapping
        ------------------------------------------------------------
        Args:
            intent: the intent recognized
        Returns:
            table name"""
        
        try:
            return self.table_schema["mappers"][intent.lower()]
        except KeyError:
            return None
        
    def table_def(
            self,
            table: str
            ) -> Optional[Dict]:
        
        """Loads the table definition with the table schema and descriptions
        --------------------------------------------------------------------
        Args:
            table: table name
        Returns:
            table definition
        """

        if table:
            return self.table_schema["tables"][table]
        else:
            return None
        
    async def draft_query(
            self,
            user_query: str,
            table_schema: Dict,
            table: str,
            user_id: str, 
            ) -> Optional[str]:
        
        """Drafts SQL query as per the user requirement
        -------------------------------------------
        Args:
            user_query:
            table_schema: table schema loaded
            table: table name
            user_id: user ID
        Returns:
            SQL query
        """
        
        if table_schema:
            gen_query = await self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": f"You are an AI assistant who can make MySQL queries to fetch data from the following table '{table}'. The table stores the {table_schema['description']}. The fields are {table_schema['fields']} Just produce the SQL query alone. Do not add any strings like ```sql```"},
                        {"role":"user", "content": f"user query: {user_query}, user_id: {user_id}"}],
                seed=1,
                temperature=1
                )
        else:
            gen_query = None

        return gen_query.choices[0].message.content
    
    def fetch_value(
            self,
            sql_query: str
            ) -> Optional[str]:
        
        """Fetches the value from the respective table using the drafted query
        ----------------------------------------------------------------------
        Args:
            sql_query: Generated query
        Returns:
            fetched values from the table
        """

        username = self.db_config["db_config"]["user"]
        password = self.db_config["db_config"]["password"]
        server = self.db_config["db_config"]["host"]
        database = self.db_config["db_config"]["database"]

        conn_str = (
                r'DRIVER={ODBC Driver 17 for SQL Server};'
                r'SERVER=' + server + ';'
                r'DATABASE=' + database + ';'
                r'UID=' + username + ';'
                r'PWD=' + password + ';'
            )
        # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        # engine = create_engine(connection_url)
        connection = pyodbc.connect(conn_str)
        print(connection)


        try:
            value = pandas.read_sql_query(sql_query, con=connection)
        except:# ProgrammingError:
            value = None
        return value
    
    def __call__(
            self,
            intent: str,
            user_query: str,
            user_id: str
            ) -> Optional[str]:



        table = self.table_name(intent=intent)
        table_definition = self.table_def(table)
        print(table_definition)
        sql_generated = self.draft_query(user_query= user_query, table_schema= table_definition, table=table, user_id=user_id)
        print(sql_generated)
        value_from_source = self.fetch_value(sql_query= sql_generated)

        return value_from_source
    
    async def correct_query(
            self,
            query: str
    ) -> str:
        if "LIMIT" in query:
            gen_query = await self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": "You are a helpful assistant who can identify the the errors in the given SQL query and convert it into a fully functional SQLServer query (MSSQL query). The response should be a MSSQL query that should not contain any strings like ```sql``` or any system messages or instructions. The output text should be directly executable."},
                        {"role":"user", "content": f"user query: {query}"}],
                seed=1,
                temperature=0.2
                )
            return gen_query.choices[0].message.content
        else:
            return query
        
    async def draft_query_special(
            self,
            user_query: str,
            table_schema: Dict,
            ) -> Optional[str]:
        
        """Drafts SQL query as per the user requirement
        -------------------------------------------
        Args:
            user_query:
            table_schema: table schema loaded
            table: table name
            user_id: user ID
        Returns:
            SQL query
        """
        system_content = """You are an AI assistant designed to identify the appropriate table from a given set of table schemas and generate SQLServer (MSSQL) queries to fetch data. Your focus should be on:

                            - Understanding the intent from the 'table_descriptions' within the schema.
                            - Aggregating data only over quarters, never over a complete year.
                            - Restricting any aggregate calculations to the last quarter available in that year, unless the user specifies a particular quarter.
                            - Should be a single query which I can directly execute from the output without making any changes
                            - If the user is looking for AUM, always show the sum of Values for the period of interest. Do not show the Values of Total Categories separately.
                            An example query and result is given here with to understand the complexity of the query expected.
                            query: What is the worst and best deals of mandate 37007 in 2024
                            answer: SELECT * FROM (SELECT TOP 1 scp.MandateId, scp.MarketValue, dpu.DealId, di.DealName, DealStatus='Best'
                                    FROM ShareClassPerformance scp
                                    JOIN DealPriceUpdate dpu ON dpu.ISIN = scp.ISIN
                                    JOIN DealInformation di ON di.DealId = dpu.DealId
                                    WHERE scp.MandateId = 37007 AND Year = 2024 AND Quarter = (
                                                                                                SELECT MAX(Quarter)
                                                                                                FROM Holdings
                                                                                                WHERE MandateId = 37007 AND Year = 2023
                                                                                            )
                                    ORDER BY MarketValue DESC) a
                                    UNION
                                    SELECT * FROM (SELECT TOP 1 scp.MandateId, scp.MarketValue, dpu.DealId, di.DealName, DealStatus='Worst'
                                    FROM ShareClassPerformance scp
                                    JOIN DealPriceUpdate dpu ON dpu.ISIN = scp.ISIN
                                    JOIN DealInformation di ON di.DealId = dpu.DealId
                                    WHERE scp.MandateId = 37007 AND Year = 2024 AND Quarter = (
                                                                                                SELECT MAX(Quarter)
                                                                                                FROM Holdings
                                                                                                WHERE MandateId = 37007 AND Year = 2023
                                                                                            )
                                    ORDER BY MarketValue ASC) b
                            Generate only SQLServer query based on the provided table_schema: {table_schema}. The result should not contain any strings like ```sql``` or any system messages or instructions. The query should be directly executable in SQLServer."""
        system_content_old = """You are an AI assistant who can identify the table of intent from a given structure of tables and make only SQLServer (MSSQL) queries to fetch data from that table.
                                                            Look at the key 'table_descriptions' in each table schema to understand what to look for in each table. The data is tracked across various fields as mentioned in the table_descriptions. Never aggregate on a complete year; only aggregates over quarters are allowed.
                                                            Always ensure that when calculating any aggregate value for a year, it must be restricted to the last quarter available in that year unless the quarter is specified by user.
                                                            table_schema: {table_schema}
                                                            Just produce the SQL query alone. The result should not contain any strings like ```sql```. The query should be directly executable."""
        if table_schema:
            gen_query = await self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": system_content.format(table_schema = table_schema)},
                        {"role":"user", "content": f"user query: {user_query}"}],
                seed=1,
                temperature=0.2
                )
        else:
            gen_query = None

        return gen_query.choices[0].message.content
    

    async def special_agent(
            self,
            user_query:str,
            ) -> Optional[str]:
        table_definition = self.table_schema["tables"]
        sql_generated = await self.correct_query(await self.draft_query_special(user_query= user_query, table_schema= table_definition))
        print(sql_generated)
        value_from_source = self.fetch_value(sql_query= sql_generated)
        if not value_from_source.empty:
            with open("../logs/qandquery.jsonl", "a") as f:
                f.write(json.dumps({user_query:sql_generated}) + "\n")

        return value_from_source
    





class SQLAdapter(OpenAI):
    def __init__(
            self
            ,*args,
            **kwargs
            ) -> None:
        super().__init__(*args, **kwargs)

        _yaml = YAML()
        _table_definitions_path = os.path.join("..", "config", "table_definitions.yaml")
        _db_config_path = os.path.join("..", "config", "db_config.yaml")
        # _table_definitions_path = os.path.join("config", "table_definitions.yaml")
        # _db_config_path = os.path.join("config", "db_config.yaml")

        
        with open(_table_definitions_path, "r") as f:
            self.table_schema = _yaml.load(f)
        
        with open(_db_config_path, "r") as f:
            self.db_config = _yaml.load(f)

    def table_name(
            self,
            intent: str
            ) -> Optional[str]:
        
        """Identified the table name from the intent : table mapping
        ------------------------------------------------------------
        Args:
            intent: the intent recognized
        Returns:
            table name"""
        
        try:
            return self.table_schema["mappers"][intent.lower()]
        except KeyError:
            return None
        
    def table_def(
            self,
            table: str
            ) -> Optional[Dict]:
        
        """Loads the table definition with the table schema and descriptions
        --------------------------------------------------------------------
        Args:
            table: table name
        Returns:
            table definition
        """

        if table:
            return self.table_schema["tables"][table]
        else:
            return None
        
    def draft_query(
            self,
            user_query: str,
            table_schema: Dict,
            table: str,
            user_id: str, 
            ) -> Optional[str]:
        
        """Drafts SQL query as per the user requirement
        -------------------------------------------
        Args:
            user_query:
            table_schema: table schema loaded
            table: table name
            user_id: user ID
        Returns:
            SQL query
        """
        
        if table_schema:
            gen_query = self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": f"You are an AI assistant who can make MySQL queries to fetch data from the following table '{table}'. The table stores the {table_schema['description']}. The fields are {table_schema['fields']} Just produce the SQL query alone. Do not add any strings like ```sql```"},
                        {"role":"user", "content": f"user query: {user_query}, user_id: {user_id}"}],
                seed=1,
                temperature=1
                )
        else:
            gen_query = None

        return gen_query.choices[0].message.content
    
    def fetch_value(
            self,
            sql_query: str
            ) -> Optional[str]:
        
        """Fetches the value from the respective table using the drafted query
        ----------------------------------------------------------------------
        Args:
            sql_query: Generated query
        Returns:
            fetched values from the table
        """

        username = self.db_config["db_config"]["user"]
        password = self.db_config["db_config"]["password"]
        server = self.db_config["db_config"]["host"]
        database = self.db_config["db_config"]["database"]

        conn_str = (
                r'DRIVER={ODBC Driver 17 for SQL Server};'
                r'SERVER=' + server + ';'
                r'DATABASE=' + database + ';'
                r'UID=' + username + ';'
                r'PWD=' + password + ';'
            )
        # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        # engine = create_engine(connection_url)
        connection = pyodbc.connect(conn_str)
        print(connection)


        try:
            value = pandas.read_sql_query(sql_query, con=connection)
        except:# ProgrammingError:
            value = None
        return value
    
    def __call__(
            self,
            intent: str,
            user_query: str,
            user_id: str
            ) -> Optional[str]:



        table = self.table_name(intent=intent)
        table_definition = self.table_def(table)
        print(table_definition)
        sql_generated = self.draft_query(user_query= user_query, table_schema= table_definition, table=table, user_id=user_id)
        print(sql_generated)
        value_from_source = self.fetch_value(sql_query= sql_generated)

        return value_from_source
    
    def correct_query(
            self,
            query: str
    ) -> str:
        if "LIMIT" in query:
            gen_query = self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": "You are a helpful assistant who can identify the the errors in the given SQL query and convert it into a fully functional SQLServer query (MSSQL query). The response should be a MSSQL query that should not contain any strings like ```sql``` or any system messages or instructions. The output text should be directly executable."},
                        {"role":"user", "content": f"user query: {query}"}],
                seed=1,
                temperature=0.2
                )
            return gen_query.choices[0].message.content
        else:
            return query
        
    def draft_query_special(
            self,
            user_query: str,
            table_schema: Dict,
            ) -> Optional[str]:
        
        """Drafts SQL query as per the user requirement
        -------------------------------------------
        Args:
            user_query:
            table_schema: table schema loaded
            table: table name
            user_id: user ID
        Returns:
            SQL query
        """
        system_content = """You are an AI assistant designed to identify the appropriate table from a given set of table schemas and generate SQLServer (MSSQL) queries to fetch data. Your focus should be on:

                            - Understanding the intent from the 'table_descriptions' within the schema.
                            - Aggregating data only over quarters, never over a complete year.
                            - Restricting any aggregate calculations to the last quarter available in that year, unless the user specifies a particular quarter.
                            - Should be a single query which I can directly execute from the output without making any changes
                            - If the user is looking for AUM, always show the sum of Values for the period of interest. Do not show the Values of Total Categories separately.
                            An example query and result is given here with to understand the complexity of the query expected.
                            query: What is the worst and best deals of mandate 37007 in 2024
                            answer: SELECT * FROM (SELECT TOP 1 scp.MandateId, scp.MarketValue, dpu.DealId, di.DealName, DealStatus='Best'
                                    FROM ShareClassPerformance scp
                                    JOIN DealPriceUpdate dpu ON dpu.ISIN = scp.ISIN
                                    JOIN DealInformation di ON di.DealId = dpu.DealId
                                    WHERE scp.MandateId = 37007 AND Year = 2024 AND Quarter = (
                                                                                                SELECT MAX(Quarter)
                                                                                                FROM Holdings
                                                                                                WHERE MandateId = 37007 AND Year = 2023
                                                                                            )
                                    ORDER BY MarketValue DESC) a
                                    UNION
                                    SELECT * FROM (SELECT TOP 1 scp.MandateId, scp.MarketValue, dpu.DealId, di.DealName, DealStatus='Worst'
                                    FROM ShareClassPerformance scp
                                    JOIN DealPriceUpdate dpu ON dpu.ISIN = scp.ISIN
                                    JOIN DealInformation di ON di.DealId = dpu.DealId
                                    WHERE scp.MandateId = 37007 AND Year = 2024 AND Quarter = (
                                                                                                SELECT MAX(Quarter)
                                                                                                FROM Holdings
                                                                                                WHERE MandateId = 37007 AND Year = 2023
                                                                                            )
                                    ORDER BY MarketValue ASC) b
                            Generate only SQLServer query based on the provided table_schema: {table_schema}. The result should not contain any strings like ```sql``` or any system messages or instructions. The query should be directly executable in SQLServer."""
        system_content_old = """You are an AI assistant who can identify the table of intent from a given structure of tables and make only SQLServer (MSSQL) queries to fetch data from that table.
                                                            Look at the key 'table_descriptions' in each table schema to understand what to look for in each table. The data is tracked across various fields as mentioned in the table_descriptions. Never aggregate on a complete year; only aggregates over quarters are allowed.
                                                            Always ensure that when calculating any aggregate value for a year, it must be restricted to the last quarter available in that year unless the quarter is specified by user.
                                                            table_schema: {table_schema}
                                                            Just produce the SQL query alone. The result should not contain any strings like ```sql```. The query should be directly executable."""
        if table_schema:
            gen_query = self.chat.completions.create(
                model = "gpt-4o",
                messages=[{"role":"system", "content": system_content.format(table_schema = table_schema)},
                        {"role":"user", "content": f"user query: {user_query}"}],
                seed=1,
                temperature=0.2
                )
        else:
            gen_query = None

        return gen_query.choices[0].message.content
    

    def special_agent(
            self,
            user_query:str,
            ) -> Optional[str]:
        table_definition = self.table_schema["tables"]
        sql_generated = self.correct_query(self.draft_query_special(user_query= user_query, table_schema= table_definition))
        print(sql_generated)
        value_from_source = self.fetch_value(sql_query= sql_generated)
        if not value_from_source.empty:
            with open("../logs/qandquery.jsonl", "a") as f:
            # with open("./logs/qandquery.jsonl", "a") as f:
                f.write(json.dumps({user_query:sql_generated}) + "\n")

        return value_from_source
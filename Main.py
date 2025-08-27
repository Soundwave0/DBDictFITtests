import oracledb

import credentials

oracledb.defaults.fetch_lobs = False
class DBJsonFunctions:
    @staticmethod
    def connect_to_db():
        """Connect to Oracle database"""
        try:
            connection = oracledb.connect(
                user=credentials.user,
                password=credentials.password,
                dsn = credentials.dsn
            )
            return connection
        except Exception as e:
            print(f"Connection error: {e}")
            return None


def execute_query_with_results(connection, query, params=None):
    """Execute a query and return results"""
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # For SELECT queries, fetch results
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return results, columns
        else:
            # For INSERT/UPDATE/DELETE, commit changes
           # connection.commit()
            return cursor.rowcount, None

    except Exception as e:
        connection.rollback()
        print(f"Query execution error: {e}")
        return None, None
    finally:
        cursor.close()


def execute_query_no_results(connection, query, params=None):
    """Execute a query that doesn't return results (INSERT/UPDATE/DELETE)"""
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        #connection.commit()
        return cursor.rowcount
    except Exception as e:
        connection.rollback()
        print(f"Query execution error: {e}")
        return None
    finally:
        cursor.close()


import pandas as pd
from typing import List, Dict, Any, Optional, Tuple


def results_to_dict_list(results: List[Tuple], columns: List[str]) -> List[Dict[str, Any]]:

    if not results or not columns:
        return []

    return [dict(zip(columns, row)) for row in results]


def results_to_dataframe(results: List[Tuple], columns: List[str]) -> pd.DataFrame:

    if not results or not columns:
        return pd.DataFrame()

    return pd.DataFrame(results, columns=columns)


def execute_query_to_dict(connection, query: str, params=None) -> List[Dict[str, Any]]:
    """
    Execute query and return results as list of dictionaries.
    """
    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                return results_to_dict_list(results, columns)
            else:
                #connection.commit()
                return [{"rows_affected": cursor.rowcount}]

    except Exception as e:
        connection.rollback()
        print(f"Query execution error: {e}")
        return []


def execute_query_to_dataframe(connection, query: str, params=None) -> pd.DataFrame:
    """
    Execute query and return results as pandas DataFrame.
    """
    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                return results_to_dataframe(results, columns)
            else:
                #connection.commit()
                return pd.DataFrame({"rows_affected": [cursor.rowcount]})

    except Exception as e:
        connection.rollback()
        print(f"Query execution error: {e}")
        return pd.DataFrame()

def select_data_instance(connection,test_instance_id,trial)->dict:
    query_select_data_instance = f"SELECT DATA FROM DATA_INSTANCE WHERE test_instance_id = {test_instance_id} AND TRIAL = {trial}"
    result = execute_query_to_dict(DBJsonFunctions.connect_to_db(), query_select_data_instance)
    return result
def select_test_instance_with_data(test_instance_id):
    # Get test instance config - returns list of dicts
    query_select_test_instance_config = f"SELECT CONFIG FROM TEST_INSTANCE WHERE ID = {test_instance_id}"
    test_instance_config_list = execute_query_to_dict(DBJsonFunctions.connect_to_db(),
                                                      query_select_test_instance_config)

    # Get data instances - returns list of dicts
    data_instance_query = f"SELECT * FROM DATA_INSTANCE WHERE TEST_INSTANCE_ID = {test_instance_id}"
    data_instance_list = execute_query_to_dict(DBJsonFunctions.connect_to_db(), data_instance_query)

    params_query = f"SELECT TEST_PARAMETERS FROM TEST_TYPE WHERE ID = (SELECT TEST_TYPE_ID FROM TEST_INSTANCE WHERE id = {test_instance_id})"
    params_result = execute_query_to_dict(DBJsonFunctions.connect_to_db(), params_query)
    # Extract the first (and should be only) config dictionary
    if test_instance_config_list:
        config_dict = test_instance_config_list[0]  # Get first config dict
    else:
        config_dict = {}  # Empty dict if no config found

    # Merge with data list nested under "data" key
    merged_dict = {
        **config_dict,
        **params_result[0],# Unpack the config dictionary
        "data": data_instance_list  # Nest the entire data list
    }

    return merged_dict
def connect_execute(query: str):
    try:
        dbconnection = DBJsonFunctions.connect_to_db()

        if dbconnection:
            # Create a cursor
            cursor = dbconnection.cursor()

            # Execute your query
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            datadictionary = results_to_dict_list(results, columns)
            cursor.close()
            dbconnection.close()
            return datadictionary
        else:
            print("Unable to connect to database")

    except Exception as e:
        print(f"Error: {e}")
if __name__ == '__main__': #function to show examples

    print(select_test_instance_with_data(1))



    #



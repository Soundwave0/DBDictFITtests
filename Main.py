import oracledb

import credentials


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
            connection.commit()
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
        connection.commit()
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
                connection.commit()
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
                connection.commit()
                return pd.DataFrame({"rows_affected": [cursor.rowcount]})

    except Exception as e:
        connection.rollback()
        print(f"Query execution error: {e}")
        return pd.DataFrame()


if __name__ == '__main__':
    # Connect to the database
    try:
        dbconnection = DBJsonFunctions.connect_to_db()

        if dbconnection:
            # Create a cursor
            cursor = dbconnection.cursor()

            # Execute your query
            cursor.execute("SELECT * FROM device_instance")
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            dictionary = results_to_dict_list(results, columns)
            dataframe = results_to_dataframe(results, columns)
            print("--------------------------------")
            print(dictionary)
            print("--------------------------------")
            print(f"Columns: {columns}")
            for row in results:
                print(row)
            print("-------------------------------")
            print(dataframe)

            # Close cursor and connection
            cursor.close()
            dbconnection.close()

        else:
            print("Unable to connect to database")

    except Exception as e:
        print(f"Error: {e}")




    #



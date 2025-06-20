import logging
import operator
from typing import TypedDict, Annotated, Literal
import datetime

import psycopg2
from langchain_core.messages import ToolMessage, AnyMessage
from langchain_core.tools import tool
from pydantic.v1 import BaseModel, Field
import os

logging.basicConfig(level=logging.INFO)


ALLOWED_TABLES = [
    "core.t1_bookings_all",
    "core.t1_bi_bookings"
]

def get_redshift_connection():
    """Get a connection to the Redshift database."""
    try:
        conn = psycopg2.connect(
                    host=os.getenv("REDSHIFT_HOST"),
                    port=int(os.getenv("REDSHIFT_PORT", 5439)),
                    user=os.getenv("REDSHIFT_USER"),
                    password=os.getenv("REDSHIFT_PASSWORD"),
                    dbname=os.getenv("REDSHIFT_DBNAME")
                )
        logging.info("Connected to Redshift")
        return conn
    except Exception as e:
        logging.error(f"Connection error: {e}")
        raise


def fetch_columns_for_allowed_tables(conn, allowed_tables):
    schema_dict = {}
    for table in allowed_tables:
        # Assuming table names are in 'schema.table_name' format
        parts = table.split('.')
        if len(parts) < 2:
            logging.warning(f"Invalid table format: {table}. Skipping.")
            continue
        
        schema_name = parts[0]
        table_name = '.'.join(parts[1:]) # Rejoin in case table name has dots

        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query, (schema_name, table_name))
                columns = [row[0] for row in cur.fetchall()]
                schema_dict[table] = columns
        except Exception as e:
            logging.error(f"Error fetching schema for {table}: {e}")
            # Optionally, you could set an empty list or raise an error
            schema_dict[table] = [] 
    return schema_dict


def execute_redshift_query(query: str) -> dict:
    """Execute query and return results as a dictionary."""
    conn = get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                def serialize_value(val):
                    if isinstance(val, (datetime.datetime, datetime.date)):
                        return val.isoformat()
                    return val
                data = [
                    {col: serialize_value(val) for col, val in zip(columns, row)}
                    for row in rows
                ]
                return {"data": data}
            return {"data": []}  # Return empty data list for no-result queries
    except Exception as e:
        logging.error(f"Query execution error: {e}")
        return {"error": str(e)}
    finally:
        conn.close()


class SQLQuery(BaseModel):
    """Schema for SQL query execution."""
    query: str = Field(description="SQL query to execute")


@tool(args_schema=SQLQuery)
def execute_sql(query: str) -> dict:
    """Execute SQL query on Redshift and return results."""
    return execute_redshift_query(query)


class AgentState(TypedDict):
    """State for the SQL agent workflow."""
    messages: Annotated[list[AnyMessage], operator.add]
    next: Literal["continue", "end", "error", "retry"]
    attempt_count: int


def handle_error(self, state: AgentState) -> AgentState:
    """Handle errors in the agent workflow."""
    attempt_count = state.get("attempt_count", 0) + 1
    if attempt_count >= self.max_attempts:
        return {
            "messages": state["messages"],
            "next": "end",
            "attempt_count": attempt_count
        }
    return {
        "messages": state["messages"],
        "next": "retry",
        "attempt_count": attempt_count
    }


def should_continue(
    self, state: AgentState
) -> Literal["function", "end", "error"]:
    """Determine if the agent should continue processing."""
    last_msg = state['messages'][-1]
    if isinstance(last_msg, ToolMessage) and last_msg.name == "error":
        return "error"
    if hasattr(last_msg, 'tool_calls') and len(last_msg.tool_calls) > 0:
        return "function"
    return "end"

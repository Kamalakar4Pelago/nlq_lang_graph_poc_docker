import os
import sys
import logging

import streamlit as st

from lang_graph_poc.llm.openai import get_model
from lang_graph_poc.tools.redshift import (
    execute_sql,
    fetch_columns_for_allowed_tables,
    get_redshift_connection
)
from lang_graph_poc.agents.sql_agent import SQLAgent

# Give the tool the correct name for the agent to find
execute_sql.name = "redshift_query"

# Configure logging at the beginning of the script
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s'
)
logger = logging.getLogger(__name__)


# Adjust the path to import from the parent directory
sys.path.append(os.path.join(
    os.path.dirname(__file__), '..', '..'
))

# Set Streamlit page configuration as the very first Streamlit command
st.set_page_config(page_title="LangGraph SQL Agent Demo", layout="wide")


# --- Constants ---
# Define a curated list of allowed tables and views for schema fetching
ALLOWED_TABLES = {
    "core.t1_bookings_all": [],  # Columns will be fetched live
    "core.t1_bi_bookings": [],
    # Add other allowed tables/views here
}

SYSTEM_PROMPT_FILE = "lang_graph_poc/llm/system_prompt.txt"


# --- Session State Initialization and Data Loading ---
def load_examples_from_file(path):
    try:
        with open(path, "r") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Could not load examples from {path}: {e}")
        return ""

# When loading the system prompt:
SYSTEM_PROMPT_FILE = "lang_graph_poc/llm/system_prompt.txt"
SAMPLES_FILE = "lang_graph_poc/context_kb/t1_bookings_all_samples_nlq.sql"

def load_system_prompt():
    base_prompt = ""
    try:
        with open(SYSTEM_PROMPT_FILE, 'r') as f:
            base_prompt = f.read()
    except FileNotFoundError:
        st.error(f"System prompt file not found at {SYSTEM_PROMPT_FILE}")
    # Append examples
    examples = load_examples_from_file(SAMPLES_FILE)
    if examples:
        base_prompt += "\n\n# Additional Example Patterns:\n" + examples
    return base_prompt

@st.cache_resource
def get_redshift_schema():
    """Fetches the Redshift schema for allowed tables."""
    with st.spinner("Fetching Redshift schema..."):
        try:
            conn = get_redshift_connection()
            schema = fetch_columns_for_allowed_tables(conn, ALLOWED_TABLES)
            conn.close()  # Close connection after fetching schema
            logger.info(f"Fetched schema: {schema}")
            return schema
        except Exception as e:
            logger.error(f"Error fetching Redshift schema: {e}")
            st.error(f"Failed to fetch Redshift schema: {e}")
            return {}


# Initialize session state variables if not already present
if "messages" not in st.session_state:
    st.session_state.messages = []
if "system_prompt" not in st.session_state:
    st.session_state.system_prompt = load_system_prompt()
if "schema" not in st.session_state:
    st.session_state.schema = get_redshift_schema()

# Initialize LLM and Agent only once, and if schema is available
if "llm" not in st.session_state and st.session_state.schema:
    with st.spinner("Initializing LLM and Agent..."):
        try:
            st.session_state.llm = get_model()
            st.session_state.sql_agent = SQLAgent(
                model=st.session_state.llm,
                tools=[execute_sql],
                system_prompt=st.session_state.system_prompt,
                schema=st.session_state.schema
            )
            logger.info("LLM and SQL Agent initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing LLM or Agent: {e}")
            st.error(f"Failed to initialize LLM or Agent: {e}")


# --- Streamlit UI (after page config) ---
st.title("Ask your data 💬")

# Sidebar for system prompt editing
with st.sidebar:
    st.header("System Prompt")
    edited_prompt = st.text_area(
        "Edit the system prompt for the SQL agent:",
        st.session_state.system_prompt,
        height=400
    )
    if st.button("Apply Prompt Changes"):
        st.session_state.system_prompt = edited_prompt
        # Re-initialize agent with new prompt if it exists
        if "sql_agent" in st.session_state:
            try:
                st.session_state.sql_agent = SQLAgent(
                    model=st.session_state.llm,
                    tools=[execute_sql],
                    system_prompt=st.session_state.system_prompt,
                    schema=st.session_state.schema
                )
                st.success("System prompt updated and agent re-initialized!")
                logger.info("System prompt updated and agent re-initialized.")
            except Exception as e:
                logger.error(f"Error re-initializing agent with new prompt: {e}")
                st.error(f"Failed to re-initialize agent: {e}")
        else:
            st.warning("Agent not initialized yet. Prompt will apply on first agent init.")

    st.write("--- Jarvin V1.0 ---")

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("What would you like to know?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    if "sql_agent" not in st.session_state:
        st.warning("SQL Agent not initialized. "
                   "Please ensure schema fetching was successful.")
        st.stop()

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # Call the agent's ask method with the user query
                result = st.session_state.sql_agent.ask(prompt)
                print("\n\n ====>>> ", result)
                if result.get("usage"):
                    st.info(
                        f"Tokens used: {result['usage'].get('total_tokens', 0)} | "
                        f"Cost: ${result.get('cost', 0.0):.4f}"
                    )
                # Initialize response variables
                response_message = ""
                summary = ""
                final_content = None
                # Determine what to display based on the agent's result
                if result.get('action') == 'clarify':
                    response_message = result.get(
                        'error',  # Use error field for clarification messages
                        "I need more information to process your request."
                    )
                    st.markdown(response_message)
                elif result.get('success'):
                    summary = result.get('summary', "Query Generated successfully.")
                    st.markdown(summary)
                    if result.get('data') is not None and not result['data'].empty:
                        st.dataframe(result['data'])
                        # Display other relevant metadata if available
                        st.json({
                            "SQL Query": result.get('sql_query'),
                            "Reasoning": result.get('reasoning'),
                            "Assumptions": result.get('metadata', {}).get('assumptions'),
                            "Row Count": result.get('metadata', {}).get('row_count'),
                            "Column Count": result.get('metadata', {}).get('column_count'),
                            "Time Taken": (
                                f"{result.get('metadata', {}).get('execution_time')}"
                            )
                        })
                else:
                    error_message = result.get('error', "An unknown error occurred.")
                    st.error(f"Error: {error_message}")
                    logger.error(f"Agent returned an error: {error_message}")
                    response_message = error_message

                # Add the final assistant response to chat history
                # Prefer summary, then error, then reasoning, then fallback to a generic message
                if result.get('summary'):
                    final_content = result['summary']
                elif result.get('error'):
                    final_content = result['error']
                elif result.get('reasoning'):
                    final_content = result['reasoning']
                elif result.get('sql_query'):
                    final_content = f"Generated SQL: {result['sql_query']}"
                else:
                    final_content = "No response generated. Please try again or rephrase your query."

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": final_content
                })
            except Exception as e:
                error_message = f"An error occurred during agent execution: {str(e)}"
                st.error(error_message)
                logger.error(error_message)
                st.session_state.messages.append({"role": "assistant", "content": error_message})
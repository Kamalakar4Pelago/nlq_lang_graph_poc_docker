import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Dict, Any, TypedDict
from langchain_core.messages import HumanMessage, SystemMessage
from lang_graph_poc.agents.sql_agent import SQLAgent, QueryResult, AgentState
from lang_graph_poc.llm.openai import get_model, get_system_prompt
from lang_graph_poc.tools.redshift import execute_sql


def initialize_agent():
    """Initialize the SQL agent with model and tools."""
    model = get_model()
    main_system_prompt = get_system_prompt()
    # system_prompt_for_summarisation = """You are a SQL expert assistant. Generate clear, concise SQL queries and provide brief explanations.
    # Keep summaries under 200 words. Focus on key insights only."""
    
    return SQLAgent(
        model=model,
        tools=[execute_sql],
        system_prompt=main_system_prompt
    )


def display_query_results(query_result: QueryResult, show_summary: bool = False):
    """Display query results in a minimal format."""
    if not query_result:
        st.warning("No query results to display.")
        return
    
    # Create tabs for essential information
    tab1, tab2 = st.tabs(["Results", "Query Details"])
    
    # Results Tab
    with tab1:
        if query_result.get('data') is not None:
            df = query_result['data']
            if not df.empty:
                # Display data in a clean format
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Show summary only if requested
                if show_summary and query_result.get('summary'):
                    st.markdown("### Summary")
                    st.write(query_result['summary'])
            else:
                st.info("Query executed successfully but returned no data.")
        else:
            if query_result.get('error'):
                st.error(f"Error: {query_result['error']}")
            else:
                st.warning("No data available to display.")
    
    # Query Details Tab
    with tab2:
        if query_result.get('sql_query'):
            st.markdown("### SQL Query")
            st.code(query_result['sql_query'], language='sql')
            
            # Show execution details
            if query_result.get('metadata'):
                metadata = query_result['metadata']
                if 'timestamp' in metadata:
                    st.write(f"Generated at: {metadata['timestamp']}")
                if 'execution_time' in metadata:
                    st.write(f"Executed at: {metadata['execution_time']}")
                if 'error_time' in metadata:
                    st.write(f"Error occurred at: {metadata['error_time']}")


def main():
    """Main function to run the Streamlit app."""
    st.title("SQL Query Assistant")
    st.markdown("Ask questions about your data in natural language.")
    
    # Initialize session state for chat history and query state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_query" not in st.session_state:
        st.session_state.pending_query = None
    if "show_summary" not in st.session_state:
        st.session_state.show_summary = True
    
    # Initialize the agent
    if "agent" not in st.session_state:
        st.session_state.agent = initialize_agent()
    
    # Display chat history
    for message in st.session_state.messages:
        if message["role"] == "user":
            st.chat_message("user").write(message["content"])
        else:
            with st.chat_message("assistant"):
                if "query_result" in message:
                    display_query_results(
                        message["query_result"],
                        message.get("show_summary", False)
                    )
                else:
                    st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about the data"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.chat_message("user").write(prompt)
        
        # Get agent response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing your question and Prepare the response......"):
                # Initialize state for SQL generation
                initial_state = AgentState(
                    messages=[HumanMessage(content=prompt)],
                    next_step="generate_sql",
                    query_result=None,
                    attempt_count=0,
                    current_step="generate_sql"
                )
                
                # Call the agent's ask method to run the full LangGraph workflow
                final_response = st.session_state.agent.ask(prompt)
                
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "Here are the results:",
                    "query_result": final_response,
                    "show_summary": st.session_state.show_summary
                })
                display_query_results(final_response, st.session_state.show_summary)
                st.session_state.pending_query = None
    
    # Add a toggle for summary display in the sidebar
    with st.sidebar:
        st.header("Display Options")
        st.session_state.show_summary = st.toggle(
            "Show Summary",
            value=st.session_state.show_summary,
            help="Toggle to show/hide query result summaries"
        )
        
        # Add a button to regenerate summary for the last query
        if st.session_state.messages and any(
            "query_result" in msg for msg in st.session_state.messages
        ):
            if st.button("Regenerate Summary"):
                last_query_msg = next(
                    (msg for msg in reversed(st.session_state.messages)
                     if "query_result" in msg),
                    None
                )
                if last_query_msg:
                    with st.spinner("Regenerating summary..."):
                        # Get a fresh summary from the agent
                        new_summary = st.session_state.agent.summarize_results(
                            last_query_msg["query_result"]
                        )
                        last_query_msg["query_result"]["summary"] = new_summary
                        st.experimental_rerun()


if __name__ == "__main__":
    main()
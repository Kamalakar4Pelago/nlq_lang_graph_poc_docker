import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Dict, Any, TypedDict
from langchain_core.messages import HumanMessage
from lang_graph_poc.agents.sql_agent import SQLAgent, QueryResult
from lang_graph_poc.llm import get_model
from lang_graph_poc.tools.redshift import execute_sql


def initialize_agent():
    """Initialize the SQL agent with model and tools."""
    model = get_model()
    system_prompt = """You are a SQL expert assistant. Generate clear, concise SQL queries and provide brief explanations.
    Keep summaries under 200 words. Focus on key insights only."""
    
    return SQLAgent(
        model=model,
        tools=[execute_sql],
        system_prompt=system_prompt
    )


def display_query_results(query_result: QueryResult):
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
                
                # Show brief summary if available
                if query_result.get('summary'):
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
    
    # Initialize session state for chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
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
                    display_query_results(message["query_result"])
                else:
                    st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about the data"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.chat_message("user").write(prompt)
        
        # Get agent response
        with st.chat_message("assistant"):
            with st.spinner("Processing your query..."):
                response = st.session_state.agent.ask(prompt)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "Here are the results:",
                    "query_result": response
                })
                display_query_results(response)


if __name__ == "__main__":
    main()
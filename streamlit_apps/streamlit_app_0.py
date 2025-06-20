import streamlit as st
from langchain_core.messages import HumanMessage, ToolMessage, AIMessage
from lang_graph_poc.agents.sql_agent import SQLAgent
from lang_graph_poc.llm.openai import get_model
from lang_graph_poc.tools.redshift import execute_sql
from lang_graph_poc.config import Config
import pandas as pd
from typing import List, Dict, Any
import json

st.set_page_config(
    page_title="SQL Agent POC",
    page_icon="🔍",
    layout="wide"
)

# Custom CSS for better chat interface
st.markdown("""
<style>
.chat-message {
    padding: 1.5rem;
    border-radius: 0.5rem;
    margin-bottom: 1rem;
    display: flex;
    flex-direction: column;
}
.chat-message.user {
    background-color: #2b313e;
}
.chat-message.assistant {
    background-color: #475063;
}
.chat-message .avatar {
    width: 20%;
}
.chat-message .content {
    width: 80%;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 2rem;
}
.stTabs [data-baseweb="tab"] {
    height: 4rem;
    white-space: pre-wrap;
    background-color: #475063;
    border-radius: 4px 4px 0px 0px;
    gap: 1rem;
    padding-top: 10px;
    padding-bottom: 10px;
}
.stTabs [aria-selected="true"] {
    background-color: #2b313e;
}
</style>
""", unsafe_allow_html=True)

def initialize_agent():
    """Initialize the SQL agent with model and tools."""
    model = get_model()
    system_prompt = """You're a senior Redshift SQL expert. Use below domain knowledge to generate related SQL queries,
    Domain MetaData:
    1. For Booking related query prefer to refer and return from core.t1_bookings_all.
    2. For Product only related query prefer to refer and return based on core.t1_products_all
    3. For Customer related queries prefer and fetch result from core.t1_customers
    4. For Event Tracking Data related queries prefer to fetch and results from core.t1_bi_event_sessions, core.t2_bi_booking_sessions

    Follow these rules: 
    1. Always verify table/column names exist
    2. Use APPROXIMATE COUNT(DISTINCT) for large tables
    3. Never return raw data - always summarize, and give me the query used to answer in SQL format.
    4. Add LIMIT 10 if querying raw data"""
    
    return SQLAgent(
        model=model,
        tools=[execute_sql],
        system_prompt=system_prompt
    )

def format_message(message: Any) -> str:
    """Format different types of messages for display."""
    if isinstance(message, HumanMessage):
        return f"👤 You: {message.content}"
    elif isinstance(message, ToolMessage):
        return f"🔧 Tool ({message.name}): {message.content}"
    elif isinstance(message, AIMessage):
        return f"🤖 Assistant: {message.content}"
    return str(message)

def parse_sql_results(results: str) -> pd.DataFrame:
    """Convert SQL results string to pandas DataFrame."""
    try:
        # Split by newlines and commas
        rows = [row.split(',') for row in results.strip().split('\n')]
        if not rows:
            return pd.DataFrame()
        # First row as headers
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df
    except Exception as e:
        st.error(f"Error parsing results: {str(e)}")
        return pd.DataFrame()

def extract_agent_thoughts(messages: List[Any]) -> List[Dict[str, str]]:
    """Extract agent's thinking process from messages."""
    thoughts = []
    for msg in messages:
        if isinstance(msg, AIMessage):
            thoughts.append({
                "type": "🤖 Assistant",
                "content": msg.content
            })
        elif isinstance(msg, ToolMessage):
            thoughts.append({
                "type": "🔧 Tool",
                "content": f"Executed {msg.name}: {msg.content}"
            })
    return thoughts

def main():
    st.title("SQL Agent POC")
    
    # Initialize session state for chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent" not in st.session_state:
        st.session_state.agent = initialize_agent()
    
    # Chat input
    with st.container():
        user_query = st.chat_input("Ask your question about the data:")
        
        if user_query:
            # Add user message to chat
            st.session_state.messages.append({"role": "user", "content": user_query})
            
            # Get agent response
            with st.spinner("Thinking..."):
                result = st.session_state.agent.graph.invoke({
                    "messages": [HumanMessage(content=user_query)]
                })
                
                # Add assistant response to chat
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result['messages'][-1].content,
                    "details": {
                        "sql_query": next((msg.content for msg in result['messages'] 
                                        if isinstance(msg, ToolMessage) and msg.name == "execute_sql"), ""),
                        "thoughts": extract_agent_thoughts(result['messages'])
                    }
                })
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])
            
            # If this is an assistant message with details, show tabs
            if message["role"] == "assistant" and "details" in message:
                tabs = st.tabs(["Descriptive Answer", "Query & Results", "Agent Audit Trail"])
                
                with tabs[0]:
                    st.markdown(message["content"])
                
                with tabs[1]:
                    if message["details"]["sql_query"]:
                        st.code(message["details"]["sql_query"], language="sql")
                        # Try to parse and display results as a table
                        results = next((msg.content for msg in result['messages'] 
                                     if isinstance(msg, ToolMessage) and msg.name == "execute_sql"), "")
                        if results:
                            df = parse_sql_results(results)
                            if not df.empty:
                                st.dataframe(df, use_container_width=True)
                
                with tabs[2]:
                    for thought in message["details"]["thoughts"]:
                        st.markdown(f"**{thought['type']}**")
                        st.markdown(thought['content'])
                        st.markdown("---")

if __name__ == "__main__":
    main()
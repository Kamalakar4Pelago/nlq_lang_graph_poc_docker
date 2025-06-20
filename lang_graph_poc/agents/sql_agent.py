from typing import Dict, Any, Optional, TypedDict
from datetime import datetime
import pandas as pd
from langchain_core.messages import (
    HumanMessage, SystemMessage, ToolMessage, AIMessage
)
from langgraph.graph import StateGraph, END
import logging
import json
from lang_graph_poc.llm.openai import calculate_cost
import re


def safe_json_loads(content):
    # Remove triple backticks and optional 'json' language tag
    if content.strip().startswith("```"):
        content = re.sub(
            r"^```(?:json)?\s*", "", content.strip(), flags=re.IGNORECASE
        )
        content = re.sub(r"\s*```$", "", content.strip())
    try:
        return json.loads(content)
    except Exception as e:
        logging.error(
            f"Failed to parse LLM response as JSON: {content} | Error: {e}"
        )
        return None


class QueryResult(TypedDict):
    """Type definition for query results."""
    success: bool
    data: Optional[pd.DataFrame]
    error: Optional[str]
    raw_result: Optional[str]
    sql_query: Optional[str]
    reasoning: Optional[str]
    summary: Optional[str]
    metadata: Dict[str, Any]
    action: Optional[str]
    usage: Optional[str]
    missing_tables: Optional[list[str]]
    missing_columns: Optional[list[str]]


class AgentState(TypedDict):
    """Type definition for agent state."""
    messages: list[HumanMessage | SystemMessage | ToolMessage | AIMessage]
    next_step: str
    query_result: Optional[Dict[str, Any]]
    attempt_count: int
    current_step: str


def extract_token_usage(response):
    # LangChain's ChatOpenAI puts token usage here
    return getattr(response, "response_metadata", {}).get("token_usage", None)


class SQLAgent:

    def __init__(self, model, tools, system_prompt="", schema=None):
        """Initialize the SQL agent with model and tools."""
        self.system_prompt = system_prompt
        self.schema = schema

        print("\n\n===> system_prompt for chosen model is : ", system_prompt)
        print("<<<<<<====================>>>>")

        self.tools = {t.name: t for t in tools}
        self.model = model.bind_tools(
            tools,
            tool_choice="auto"
        )
        self.max_attempts = 3

        # Build graph with distinct steps
        graph = StateGraph(AgentState)

        # Add nodes for each step
        graph.add_node("understand_and_expand_user_query",
                       self.understand_and_expand_user_query)
        graph.add_node("generate_sql", self.generate_sql)
        graph.add_node("verify_sql", self.verify_sql)
        graph.add_node("execute_sql", self.execute_function)
        graph.add_node("process_results", self.process_results)
        graph.add_node("summarize", self.summarize_results)
        graph.add_node("seek_clarification_on_draft_sql",
                       self.seek_clarification_on_draft_sql)
        graph.add_node("handle_sql_error", self.handle_sql_error)
        graph.add_node("display_generated_sql",
                       self.display_generated_sql)  # New node

        # Define the workflow
        graph.set_entry_point("understand_and_expand_user_query")

        graph.add_conditional_edges(
            "understand_and_expand_user_query",
            self.check_understanding_status,
            {
                "clarify": "seek_clarification_on_draft_sql",
                "proceed": "generate_sql"
            }
        )

        graph.add_edge("generate_sql", "verify_sql")

        graph.add_conditional_edges(
            "verify_sql",
            self.check_sql_verification_status,
            {
                "proceed": "display_generated_sql",  # Show SQL to User
                "clarify": "seek_clarification_on_draft_sql"
            }
        )

        # User engagement options
        graph.add_conditional_edges(
            "display_generated_sql",
            self.check_user_feedback,
            {
                "execute": "execute_sql",  # User wants to execute
                "modify": "generate_sql",  # User wants to modify
                # User needs clarification
                "clarify": "seek_clarification_on_draft_sql"
            }
        )

        graph.add_conditional_edges(
            "execute_sql",
            self.check_execution_status,
            {
                "success": "process_results",
                "error": "handle_sql_error"
            }
        )

        graph.add_edge("process_results", "summarize")
        graph.add_edge("summarize", END)
        
        graph.add_conditional_edges(
            "handle_sql_error",
            self.check_error_resolution,
            {
                "clarify_user": "seek_clarification_on_draft_sql",
                "retry_sql": "generate_sql",
                "end_error": END
            }
        )
        
        # Clarification can lead to retry or end
        graph.add_conditional_edges(
            "seek_clarification_on_draft_sql",
            self.check_clarification_response,
            {
                "retry": "generate_sql",  # User provided clarification
                "end": END  # User wants to end
            }
        )
        
        self.graph = graph.compile()

    def understand_and_expand_user_query(self, state: AgentState) -> Dict[str, Any]:
        """LLM-driven query understanding and expansion."""
        messages = state.get('messages', [])
        user_query = messages[-1].content if isinstance(messages[-1], HumanMessage) else ''
        
        logging.info(f"\n\n===>> Entering ::  understand_and_expand_user_query. User query: {user_query}")
        
        # LLM prompt for query understanding and expansion
        understanding_prompt = f"""
        Original User Query: {user_query}
        Available Schema: {self.schema}
        
        Your task is to:
        1. Understand the user's intent
        2. Identify any ambiguous terms or missing context
        3. Expand the query if needed for clarity
        4. Identify potential schema mismatches
        
        IMPORTANT: Only ask for clarification if the query is truly ambiguous or references non-existent schema elements.
        For common queries like "sales from last month", use reasonable defaults and proceed.
        
        Output a JSON with:
        {{
            "expanded_query": "Clear, expanded version of the query",
            "identified_terms": ["term1", "term2"],
            "missing_context": ["context1", "context2"],
            "schema_concerns": ["concern1", "concern2"],
            "requires_clarification": true/false,
            "clarification_questions": ["question1", "question2"]
        }}
        """
        usage =0.0
        try:
            # In understand_and_expand_user_query
            print("\n[LLM PROMPT] understand_and_expand_user_query:\n", understanding_prompt)
            response = self.model.invoke([SystemMessage(content=understanding_prompt)])
            usage = extract_token_usage(response)
            print("\n[LLM RAW RESPONSE] understand_and_expand_user_query:\n", response.content)
            llm_analysis = safe_json_loads(response.content)
            print("\n[LLM PARSED JSON] understand_and_expand_user_query:\n", llm_analysis)
            
            # If clarification is needed, route to clarification
            if llm_analysis.get('requires_clarification', False):
                clarification_msg = (
                    "I need some clarification to better understand your request:\n" +
                    "\n".join([f"• {q}" for q in llm_analysis.get('clarification_questions', [])]) +
                    "\n\nPlease provide more details so I can assist you better."
                )
                
                return {
                    "messages": messages + [AIMessage(content=clarification_msg)],
                    "query_result": {
                        'success': False,
                        'error': clarification_msg,
                        'action': 'clarify',
                        'missing_columns': llm_analysis.get('identified_terms', []),
                        'usage': usage,
                        'metadata': {
                            'user_query': user_query, 
                            'attempt': 0, 
                            'action_taken': 'query_understanding_clarification'
                        }
                    },
                    "current_step": "understand_and_expand_user_query"
                }
            
            # If no clarification needed, proceed with expanded query
            expanded_query = llm_analysis.get('expanded_query', user_query)
            
            query_result = {
                'success': True,
                'data': None,
                'error': None,
                'raw_result': None,
                'sql_query': None,
                'reasoning': "Query understood and expanded.",
                'summary': None,
                'usage': usage,
                'metadata': {
                    'user_query': user_query,
                    'expanded_query': expanded_query,
                    'identified_terms': llm_analysis.get('identified_terms', []),
                    'attempt': 0,
                    'action_taken': 'query_understood_and_expanded'
                },
                'action': 'proceed',
                'missing_tables': [],
                'missing_columns': llm_analysis.get('identified_terms', [])
            }
            
            logging.info(f"\n\n===>> Exiting ::  understand_and_expand_user_query. Expanded query: {expanded_query}")
            return {
                "messages": messages + [AIMessage(content="Query understood and expanded.")],
                "query_result": query_result,
                "current_step": "understand_and_expand_user_query"
            }
            
        except Exception as e:
            error_msg = f"Error in query understanding: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    'success': False,
                    'error': error_msg,
                    'action': 'end_error',
                    'usage': usage,
                    'metadata': {
                        'user_query': user_query, 
                        'attempt': 0, 
                        'action_taken': 'understanding_failed'
                    }
                },
                "current_step": "understand_and_expand_user_query"
            }

    def generate_sql(self, state: AgentState) -> Dict[str, Any]:
        """Generate SQL query with reasoning."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        user_query = query_result.get('metadata', {}).get('user_query', '')
        expanded_query = query_result.get('metadata', {}).get('expanded_query', user_query)
        
        logging.info(f"\n\n===>> Entering ::  generate_sql. User query: {user_query}. Expanded query: {expanded_query}")

        # If coming from handle_sql_error with a corrected_sql, use that
        if query_result.get('action_taken') == 'retry_sql' and \
                query_result.get('sql_query'):
            sql_query = query_result['sql_query']
            reasoning = "Retrying with LLM-corrected SQL query."
            assumptions = "Based on previous error analysis and LLM correction."
            logging.info(f"Using LLM-corrected SQL for retry: {sql_query}")
            
            query_result = {
                'success': True,
                'data': None,
                'error': None,
                'raw_result': None,
                'sql_query': sql_query,
                'reasoning': reasoning,
                'summary': None,
                'metadata': {**query_result.get('metadata', {}),
                            'user_query': user_query,
                            'assumptions': assumptions,
                            'action_taken': 'sql_generation_retried'},
                'action': 'proceed',
                'missing_tables': [],
                'missing_columns': []
            }
            logging.info(f"\n\n===>> Exiting ::  generate_sql with retry result: {query_result}")
            return {
                "messages": messages + [AIMessage(content=f"Retrying SQL generation: {sql_query}")],
                "query_result": query_result,
                "current_step": "generate_sql"
            }

        # Enhanced SQL generation prompt with better schema awareness
        sql_generation_prompt = f"""
        Given the user query and the database schema, generate a SQL query.
        
        Database Schema: {self.schema}
        Original User Query: {user_query}
        Expanded Query: {expanded_query}
        
        IMPORTANT RULES:
        1. Use ONLY tables and columns that exist in the provided schema
        2. For "sales" queries, use booking_state IN ('CONFIRMED', 'PENDING', 'FULFILLED')
        3. For "last month" queries, use DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        4. For revenue, use gross_total_sgd, net_total_sgd, or booking_gross_total_sgd
        5. For date filtering, use booking_date as default unless specified otherwise
        6. Always include LIMIT 100 for exploratory queries
        
        Ensure the query is syntactically correct and uses only tables and columns 
        present in the schema. If a requested column or table is not present, 
        do NOT hallucinate; instead, note it as a missing element.
        
        Output a JSON object with the following structure:
        {{
            "sql_query": "YOUR_SQL_QUERY_HERE",
            "reasoning": "YOUR_REASONING_FOR_SQL_QUERY_HERE",
            "missing_tables": ["table1", "table2"], 
            "missing_columns": ["column1", "column2"]
        }}
        If no tables or columns are missing, provide empty lists.
        """
        try:
            print("\n[LLM PROMPT] generate_sql:\n", sql_generation_prompt)
            response = self.model.invoke([SystemMessage(content=sql_generation_prompt)])
            usage = extract_token_usage(response)
            print("\n[LLM RAW RESPONSE] generate_sql:\n", response.content)
            llm_response = safe_json_loads(response.content)
            print("\n[LLM PARSED JSON] generate_sql:\n", llm_response)
            if not llm_response:
                error_msg = "The LLM did not return a valid JSON response."
                logging.error(error_msg)
                return {
                    "messages": messages + [AIMessage(content=error_msg)],
                    "query_result": {
                        **query_result,
                        'success': False,
                        'error': error_msg,
                        'action': 'end_error',
                        'usage': usage,
                        'metadata': {**query_result.get('metadata', {}),
                                    'error_time': datetime.now().isoformat(),
                                    'action_taken': 'llm_response_invalid'}
                    },
                    "current_step": "generate_sql"
                }
            sql_query = llm_response.get('sql_query', '')
            # sql_query = llm_response.get('sql_query', '').replace("```sql", "").replace("```", "").strip()
            reasoning = llm_response.get('reasoning', 'No reasoning provided.')
            missing_tables = llm_response.get('missing_tables', [])
            missing_columns = llm_response.get('missing_columns', [])

            query_result = {
                'success': True,
                'data': None,
                'error': None,
                'raw_result': None,
                'sql_query': sql_query,
                'reasoning': reasoning,
                'summary': None,
                'usage': usage,
                'metadata': {**query_result.get('metadata', {}),
                            'user_query': user_query,
                            'expanded_query': expanded_query,
                            'action_taken': 'sql_generated'},
                'action': 'proceed',
                'missing_tables': missing_tables,
                'missing_columns': missing_columns
            }
            logging.info(f"\n\n===>> Exiting ::  generate_sql with generated query: {query_result}")
            return {
                "messages": messages + [AIMessage(content="SQL generated.")],
                "query_result": query_result,
                "current_step": "generate_sql"
            }
        except Exception as e:
            error_msg = f"Error during SQL generation: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'action': 'end_error',
                    'metadata': {**query_result.get('metadata', {}),
                                'error_time': datetime.now().isoformat(),
                                'action_taken': 'sql_generation_failed'}
                },
                "current_step": "generate_sql"
            }

    def verify_sql(self, state: AgentState) -> Dict[str, Any]:
        """LLM-driven SQL verification against schema and user intent."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        sql_query = query_result.get('sql_query', '')
        user_query = query_result.get('metadata', {}).get('user_query', '')
        expanded_query = query_result.get('metadata', {}).get('expanded_query', user_query)
        
        logging.info(f"\n\n===>> Entering ::  verify_sql. SQL: {sql_query}, User Query: {user_query}")
        
        # LLM prompt for comprehensive SQL verification
        verification_prompt = f"""
        Original User Query: {user_query}
        Expanded Query: {expanded_query}
        Generated SQL: {sql_query}
        Available Schema: {self.schema}
        
        Your task is to verify:
        1. Does the SQL accurately reflect the user's intent?
        2. Are all tables and columns used in the SQL present in the schema?
        3. Is the SQL syntactically correct?
        4. Are there any logical issues or missing conditions?
        
        Output a JSON with:
        {{
            "is_valid": true/false,
            "reasoning": "Detailed explanation of verification results",
            "missing_tables": ["table1", "table2"],
            "missing_columns": ["column1", "column2"],
            "syntax_issues": ["issue1", "issue2"],
            "logical_issues": ["issue1", "issue2"],
            "suggested_fixes": ["fix1", "fix2"],
            "requires_clarification": true/false,
            "clarification_reason": "Why clarification is needed"
        }}
        """
        
        try:
            print("\n[LLM PROMPT] verify_sql:\n", verification_prompt)
            response = self.model.invoke([SystemMessage(content=verification_prompt)])
            usage = extract_token_usage(response)
            print("\n[LLM RAW RESPONSE] verify_sql:\n", response.content)
            verification_result = safe_json_loads(response.content)
            print("\n[LLM PARSED JSON] verify_sql:\n", verification_result)
            
            is_valid = verification_result.get('is_valid', False)
            missing_tables = verification_result.get('missing_tables', [])
            missing_columns = verification_result.get('missing_columns', [])
            requires_clarification = verification_result.get('requires_clarification', False)
            
            if not is_valid or requires_clarification:
                # Construct comprehensive clarification message
                issues = []
                if missing_tables:
                    issues.append(f"Missing tables: {', '.join(missing_tables)}")
                if missing_columns:
                    issues.append(f"Missing columns: {', '.join(missing_columns)}")
                if verification_result.get('syntax_issues'):
                    issues.append(f"Syntax issues: {', '.join(verification_result['syntax_issues'])}")
                if verification_result.get('logical_issues'):
                    issues.append(f"Logical issues: {', '.join(verification_result['logical_issues'])}")
                
                clarification_message = (
                    "I couldn't generate a valid SQL query because:\n" +
                    "\n".join([f"• {issue}" for issue in issues]) + "\n\n" +
                    f"Reasoning: {verification_result.get('reasoning', 'No specific reason provided')}\n\n" +
                    "Please clarify your request or provide more specific details about what you're looking for."
                )
                
                logging.info(f"\n\n===>> Exiting ::  verify_sql with clarification. Issues: {issues}")
                return {
                    "messages": messages + [AIMessage(content=clarification_message)],
                    "query_result": {
                        **query_result,
                        'success': False,
                        'error': clarification_message,
                        'action': 'clarify',
                        'missing_tables': missing_tables,
                        'missing_columns': missing_columns,
                        'usage': usage,
                        'metadata': {
                            **query_result.get('metadata', {}),
                            'action_taken': 'sql_verification_failed',
                            'verification_reasoning': verification_result.get('reasoning'),
                            'suggested_fixes': verification_result.get('suggested_fixes', [])
                        }
                    },
                    "current_step": "verify_sql"
                }
            
            logging.info("\n\n===>> Exiting ::  verify_sql with proceed. SQL verified successfully.")
            return {
                "messages": messages + [AIMessage(content="SQL verified successfully.")],
                "query_result": {
                    **query_result,
                    'success': True,
                    'error': None,
                    'action': 'proceed',
                    'usage': usage,
                    'metadata': {
                        **query_result.get('metadata', {}),
                        'action_taken': 'sql_verified',
                        'verification_reasoning': verification_result.get('reasoning')
                    }
                },
                "current_step": "verify_sql"
            }
            
        except Exception as e:
            error_msg = f"Error during SQL verification: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'action': 'end_error',
                    'usage': usage,
                    'metadata': {
                        **query_result.get('metadata', {}),
                        'action_taken': 'verification_failed'
                    }
                },
                "current_step": "verify_sql"
            }

    def seek_clarification_on_draft_sql(self, state: AgentState) -> Dict[str, Any]:
        """Provides clarification to the user based on missing information."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        missing_columns = query_result.get('missing_columns', [])
        missing_tables = query_result.get('missing_tables', [])

        logging.info("\n\n===>> Entering ::  seek_clarification_on_draft_sql. " +
                     f"Missing columns: {missing_columns}, Missing tables: {missing_tables}")

        clarification_text = """
                I couldn't generate a valid SQL query because some terms in your request were " +
                "unclear or not found in the database schema.\nSpecifically:\n\n"""
        if missing_tables:
            clarification_text += f"- Missing Tables: {', '.join(missing_tables)}\n"
        if missing_columns:
            clarification_text += f"- Missing Columns: {', '.join(missing_columns)}\n"
        
        clarification_text += ("\nPlease provide more specific details or correct " +
                               "the terms so I can assist you better.")

        logging.info("\n\n===>> Exiting ::  seek_clarification_on_draft_sql. Clarification: " +
                     f"{clarification_text}")
        return {
            "messages": messages + [AIMessage(content=clarification_text)],
            "query_result": {
                **query_result,
                'success': False,
                'action': 'clarified',
                'metadata': {**query_result.get('metadata', {}),
                             'action_taken': 'clarification_provided'}
            },
            "current_step": "seek_clarification_on_draft_sql"
        }
        
    def display_generated_sql(self, state: AgentState) -> Dict[str, Any]:
        """Display the generated SQL to user and ask for feedback."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        sql_query = query_result.get('sql_query', '')
        reasoning = query_result.get('reasoning', '')
        
        display_message = f"""
    ✅ **SQL Generated Successfully!**

    **Your Query:** {query_result.get('metadata', {}).get('user_query', '')}

    **Generated SQL:**
    ```sql
    {sql_query}
    ```

    **Reasoning:** {reasoning}

    **What would you like to do?**
    1. **Execute** this SQL query (Phase 2 feature)
    2. **Modify** the query
    3. **Ask for clarification** about the results

    Please let me know your preference!
    """
        
        return {
            "messages": messages + [AIMessage(content=display_message)],
            "query_result": {
                **query_result,
                'success': True,
                'action': 'sql_ready_for_review',
                'summary': display_message,  # <-- Add this line
                'metadata': {
                    **query_result.get('metadata', {}),
                    'action_taken': 'sql_displayed_to_user'
                }
            },
            "current_step": "display_generated_sql"
        }

    def check_execution_status(self, state: AgentState) -> str:
        query_result = state.get('query_result', {})
        if query_result.get('success'):
            return "success"
        return "error"

    def check_error_resolution(self, state: AgentState) -> str:
        query_result = state.get('query_result', {})
        action = query_result.get('action')
        if action == 'clarify':
            return "clarify_user"
        elif action == 'retry_sql':
            return "retry_sql"
        return "end_error"

    def check_sql_verification_status(self, state: AgentState) -> str:
        """Check if generated SQL is valid and ready for user review."""
        query_result = state.get('query_result', {})
        
        # Check for missing elements
        missing_tables = query_result.get('missing_tables', [])
        missing_columns = query_result.get('missing_columns', [])
        
        # Check if SQL was generated
        sql_query = query_result.get('sql_query', '')
        
        if missing_tables or missing_columns or not sql_query.strip():
            logging.info(f"Verification failed: missing_tables={missing_tables}, missing_columns={missing_columns}")
            return "clarify"
        
        logging.info("SQL verification passed, ready for user review")
        return "proceed"
    
    def check_understanding_status(self, state: AgentState) -> str:
        """Check if the initial query understanding requires clarification."""
        action = state.get('query_result', {}).get('action')
        if action == 'clarify':
            logging.info("Routing to seek clarification based on query understanding.")
            return "clarify"
        logging.info("Proceeding to SQL generation.")
        return "proceed"

    def check_user_feedback(self, state: AgentState) -> str:
        """Check what the user wants to do with the generated SQL."""
        # This would be implemented based on user input in Streamlit
        # For now, we'll simulate user choice
        user_choice = state.get('query_result', {})\
            .get('metadata', {})\
            .get('user_choice', 'execute')
        
        if user_choice == 'execute':
            return "execute"
        elif user_choice == 'modify':
            return "modify"
        else:
            return "clarify"
  
    def check_clarification_response(self, state: AgentState) -> str:
        """Check if user provided clarification and wants to retry."""
        # This would check if user provided new information
        has_clarification = state.get('query_result', {})\
            .get('metadata', {})\
            .get('has_clarification', False)
        
        if has_clarification:
            return "retry"
        else:
            return "end"

    def handle_sql_error(self, state: AgentState) -> Dict[str, Any]:
        """Handles SQL execution errors, attempting to fix or asking for clarification."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        sql_query = query_result.get('sql_query', 'N/A')
        error_message = query_result.get('error', 'Unknown error')
        user_query = query_result['metadata'].get('user_query', '')
        attempt_count = query_result['metadata'].get('attempt', 0)

        logging.error(
            f"\n\n===>> Entering ::  handle_sql_error. "
            f"Attempt {attempt_count}/{self.max_attempts}. "
            f"Error: {error_message}"
        )

        # Increment attempt count for the current query cycle
        new_attempt_count = attempt_count + 1
        updated_metadata = {**query_result.get('metadata', {}),
                            'attempt': new_attempt_count}

        # Limit retries to prevent loops
        if new_attempt_count > self.max_attempts:
            final_error_msg = (
                "I'm sorry, I encountered an unresolvable error "
                f"after {new_attempt_count} attempts: "
                f"{error_message}. Please rephrase your question "
                "or try again later."
            )
            logging.error(
                f"Max attempts reached. Ending with error: {final_error_msg}")
            return {
                "messages": messages + [AIMessage(content=final_error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': final_error_msg,
                    'action': 'end_error',
                    'metadata': {**updated_metadata,
                                 'action_taken': 'max_retries_reached'}
                },
                "current_step": "handle_sql_error"
            }
        
        # Prompt LLM to analyze and potentially fix the SQL error or ask for
        # clarification
        error_analysis_prompt = f"""
Original user query: {user_query}
Generated SQL: {sql_query}
SQL Error: {error_message}
Schema: {self.schema}
Analyze this SQL error. Can you fix the SQL query based on the schema and the
error message? If you can fix it, provide the corrected SQL query.
If the error indicates ambiguity or a missing concept in the user's original
query that requires clarification, output a JSON with {{"action": "clarify",
"terms": ["term1", "term2"]}}. Otherwise, output a JSON with
{{"action": "retry_sql", "corrected_sql": "YOUR_CORRECTED_SQL_HERE"}}.
If it's an unresolvable error, just output a simple message that says the
query cannot be fixed, like "I cannot fix this query.".
"""
        
        try:
            response = self.model.invoke(
                [SystemMessage(content=error_analysis_prompt)])
            usage = extract_token_usage(response)
            llm_decision = safe_json_loads(response.content)

            if llm_decision.get('action') == 'clarify':
                logging.info(f"LLM decided to clarify: "
                             f"{llm_decision.get('terms', [])}")
                # Construct a user-friendly clarification message
                clarification_terms = llm_decision.get('terms', [])
                clarification_msg = (
                    "I need more information about: "
                    f"{', '.join(clarification_terms)}. Please clarify "
                    "these terms. Would you like to retry with more "
                    "details, or end this conversation?"
                )
                # Transition to clarification
                return {
                    "messages": messages + [AIMessage(content=clarification_msg)],
                    "query_result": {
                        **query_result,
                        'action': 'clarify',
                        'missing_columns': clarification_terms,
                        # Set error to the clarification message
                        'error': clarification_msg,
                        'metadata': {**updated_metadata,
                                     'action_taken': 'error_clarification'}
                    },
                    "current_step": "handle_sql_error"
                }
            elif (llm_decision.get('action') == 'retry_sql' and
                  llm_decision.get('corrected_sql')):
                # Retry with corrected SQL
                new_sql = llm_decision['corrected_sql'].replace(
                    "```sql", "").replace("```", "").strip()
                logging.info(f"LLM provided corrected SQL. Retrying: {new_sql}")

                # Update query_result with new SQL and increment attempt count
                updated_query_result = {
                    **query_result,
                    'sql_query': new_sql,
                    'error': None,  # Clear previous error
                    'metadata': updated_metadata  # Already incremented above
                }
                return {
                    "messages": [
                        *messages,
                        AIMessage(content=f"Attempting to fix SQL: {new_sql}")
                    ],
                    "query_result": updated_query_result,
                    "current_step": "handle_sql_error"
                }
            else:
                # Unresolvable error by LLM
                final_error_msg = (
                    "I'm sorry, I couldn't resolve the SQL error: "
                    f"{error_message}. Please rephrase your "
                    "question or try again later."
                )
                logging.error(
                    f"LLM could not resolve error. Ending: {final_error_msg}")
                return {
                    "messages": messages + [AIMessage(content=final_error_msg)],
                    "query_result": {
                        **query_result,
                        'success': False,
                        'error': final_error_msg,
                        'action': 'end_error',
                        'metadata': {**updated_metadata,
                                     'action_taken': 'unresolvable_error'}
                    },
                    "current_step": "handle_sql_error"
                }
        except Exception as e:
            error_msg = f"Error in SQL error handling: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'metadata': {**updated_metadata,
                                 'error_time': datetime.now().isoformat(),
                                 'action_taken': 'internal_error_in_handler'}
                },
                "current_step": "handle_sql_error"
            }

    def execute_function(self, state: AgentState) -> Dict[str, Any]:
        """Execute the SQL query or call a tool based on the agent's decision."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        sql_query = query_result.get('sql_query', '')
        user_query = query_result.get('metadata', {}).get('user_query', '')
        
        logging.info(
            f"\n\n===>> Entering ::  execute_function. SQL to execute: {sql_query}")

        tool_name = "redshift_query"
        tool_to_call = self.tools.get(tool_name)

        if not tool_to_call:
            error_msg = f"Tool {tool_name} not found."
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'action': 'end_error',
                    'metadata': {**query_result.get('metadata', {}),
                                 'action_taken': 'tool_not_found'}
                },
                "current_step": "execute_sql"
            }

        try:
            tool_output = tool_to_call.invoke({"query": sql_query})
            logging.info(f"Tool output: {tool_output}")

            if not tool_output or "data" not in tool_output:
                error_detail = "No data returned from tool."
                if tool_output and "error" in tool_output:
                    error_detail = tool_output['error']
                
                logging.error(f"SQL execution failed: {error_detail}")
                return {
                    "messages": messages + [AIMessage(content="SQL execution failed.")],
                    "query_result": {
                        **query_result,
                        'success': False,
                        'error': error_detail,
                        'raw_result': tool_output,
                        'action': 'error',
                        'metadata': {**query_result.get('metadata', {}),
                                     'action_taken': 'sql_execution_failed'}
                    },
                    "current_step": "execute_sql"
                }

            result_df = pd.DataFrame(tool_output['data'])
            raw_result = json.dumps(tool_output['data'])
            summary = f"Query executed successfully. Returned {len(result_df)} rows."
            logging.info(f"SQL execution successful. Summary: {summary}")

            return {
                "messages": messages + [AIMessage(content="SQL executed successfully.")],
                "query_result": {
                    **query_result,
                    'success': True,
                    'data': result_df,
                    'raw_result': raw_result,
                    'summary': summary,
                    'action': 'proceed',
                    'metadata': {**query_result.get('metadata', {}),
                                 'action_taken': 'sql_executed_successfully'}
                },
                "current_step": "execute_sql"
            }
        except Exception as e:
            error_msg = f"Error calling tool {tool_name}: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'action': 'end_error',
                    'metadata': {**query_result.get('metadata', {}),
                                 'error_time': datetime.now().isoformat(),
                                 'action_taken': 'tool_invocation_error'}
                },
                "current_step": "execute_sql"
            }

    def process_results(self, state: AgentState) -> Dict[str, Any]:
        """Process the results of the executed SQL query."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        
        logging.info(f"\n\n===>> Entering ::  process_results. Query result: {query_result}")

        # For now, simply passes the results along.
        # In the future, this node can perform data manipulation or further analysis.
        processed_result = query_result # No change for now

        logging.info(f"\n\n===>> Exiting ::  process_results. Processed result: {processed_result}")
        return {
            "messages": messages + [AIMessage(content="Results processed.")],
            "query_result": processed_result,
            "current_step": "process_results"
        }

    def summarize_results(self, state: AgentState) -> Dict[str, Any]:
        """Summarize the processed results for the user."""
        messages = state.get('messages', [])
        query_result = state.get('query_result', {})
        user_query = query_result.get('metadata', {}).get('user_query', '')
        data_summary = query_result.get('summary', 'No summary available.')
        raw_data = query_result.get('raw_result', '')
        
        logging.info(f"\n\n===>> Entering ::  summarize_results. User query: {user_query}, " +
                     f"Data summary: {data_summary}")

        # Prepare prompt for LLM to summarize the results
        summary_prompt = f"""
        Original User Query: {user_query}
        Data Summary: {data_summary}
        Raw Data (if available): {raw_data}
        
        Given the above, generate a concise and user-friendly summary for the user. 
        Focus on answering the original user query based on the data. If the data is 
        empty or an error occurred previously, explain that clearly. Keep it brief.
        """

        try:
            response = self.model.invoke([SystemMessage(content=summary_prompt)])
            usage = extract_token_usage(response) 
            final_summary = response.content
            logging.info(f"Generated final summary: {final_summary}")

            return {
                "messages": messages + [AIMessage(content=final_summary)],
                "query_result": {
                    **query_result,
                    'summary': final_summary,
                    'action': 'completed',
                    'metadata': {**query_result.get('metadata', {}),
                                 'action_taken': 'results_summarized'}
                },
                "current_step": "summarize"
            }
        except Exception as e:
            error_msg = f"Error during summarization: {str(e)}"
            logging.error(error_msg)
            return {
                "messages": messages + [AIMessage(content=error_msg)],
                "query_result": {
                    **query_result,
                    'success': False,
                    'error': error_msg,
                    'metadata': {**query_result.get('metadata', {}),
                                 'error_time': datetime.now().isoformat(),
                                 'action_taken': 'summarization_failed'}
                },
                "current_step": "summarize"
            }

    def ask(self, query: str) -> Dict[str, Any]:
        """Entry point for asking a question to the SQL Agent."""
        logging.info(f"Agent received a new query: {query}")
        initial_state = {
            "messages": [HumanMessage(content=query)],
            "next_step": "understand_and_expand_user_query",
            "query_result": {
                "user_query": query,
                "attempt_count": 0
            },
            "current_step": "start"
        }
        # Run the graph with the initial state
        final_state = self.graph.invoke(initial_state)
        result = final_state['query_result']
        usage = result.get("usage")
        cost = calculate_cost(usage, model="gpt-4o")  # or your model name
        result["cost"] = cost
        return result

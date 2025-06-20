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

agent = SQLAgent(
    model=model,
    tools=[execute_sql],
    system_prompt=system_prompt
)

# Test simple query
result = agent.graph.invoke({
    "messages": [HumanMessage(content="How many Bookings are from Singapore, Australia and Indonesia in last 3 months")]
})

print(result['messages'][-1].content)
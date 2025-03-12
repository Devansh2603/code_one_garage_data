# from langgraph.graph import StateGraph, END
# from sql_agent import retrieve_similar_queries, save_sql_example
# from sql_agent import query_ollama_together, get_database_schema
# from sqlalchemy import text, exc
# import logging
# import json
# from typing import List, Union, TypedDict

# # ✅ Define the Workflow State Schema
# class AgentState(TypedDict):
#     question: str
#     sql_query: str
#     query_result: Union[str, List[dict]]
#     sql_error: bool

# # ✅ Define workflow
# workflow = StateGraph(state_schema=AgentState)

# def execute_sql(state, config):
#     """Execute the SQL query and return results using SQLAlchemy."""
#     session = config.get("configurable", {}).get("session")
#     user_role = config.get("configurable", {}).get("role", "").lower()

#     if not session:
#         raise ValueError("Session is not available in config.")
    
#     query = state["sql_query"].strip()
#     logging.debug(f"⚡ Running query: {query}")

#     # ✅ Define allowed tables per role
    
#     ROLE_TABLE_ACCESS = {
#     "admin": [
#         "customer_vehicle_info",
#         "job_card_details",
#         "vehicle_service_details",
#         "vehicle_service_summary"
#     ],
#     "owner": [
#         "customer_vehicle_info",
#         "vehicle_service_details",
#         "vehicle_service_summary"
#     ],
#     "customer": [
#         "customer_vehicle_info",
#         "vehicle_service_summary"
#     ]
# }



#     # ✅ Extract tables used in the query
#     used_tables = [table for table in ROLE_TABLE_ACCESS["admin"] if table in query.lower()]

#     # ✅ Check if query is out of domain
#     allowed_tables = set(ROLE_TABLE_ACCESS.get(user_role, []))
#     if not set(used_tables).issubset(allowed_tables):
#         logging.error(f"❌ Query out of domain for role '{user_role}'. Query: {query}")
#         state["query_result"] = {"raw_answer": "", "human_readable": "Query out of domain"}
#         state["sql_error"] = True
#         return state

#     try:
#         # ✅ Ensure it's a valid SELECT query
#         if not query.lower().startswith("select"):
#             raise ValueError("Invalid SQL query. Only SELECT statements are allowed.")

#         # ✅ Execute the query
#         result = session.execute(text(query))
#         rows = result.fetchall()
#         keys = result.keys()

#         state["query_result"] = {"data": [dict(zip(keys, row)) for row in rows]}
#         state["sql_error"] = False

#     except exc.SQLAlchemyError as e:
#         logging.error(f"❌ SQLAlchemy Error: {str(e)}")
#         state["query_result"] = {"error": f"Database error: {str(e)}"}
#         state["sql_error"] = True

#     except Exception as e:
#         logging.error(f"❌ General Error: {str(e)}")
#         state["query_result"] = {"error": f"An error occurred: {str(e)}"}
#         state["sql_error"] = True

#     return generate_human_readable_response_with_llama(state)

# def clean_sql_query(query: str) -> str:
#     """Cleans the generated SQL query by removing unwanted formatting."""
#     if not query:
#         return ""

#     query = query.strip().replace("ILIKE", "LIKE")

#     # ✅ Remove unwanted markdown code block markers
#     if query.startswith("```sql"):
#         query = query[len("```sql"):].strip()
#     if query.endswith("```"):
#         query = query[:-3].strip()

#     # ✅ Remove AI response artifacts
#     if query.startswith("<s>"):
#         query = query[len("<s>"):].strip()

#     return query



# def convert_nl_to_sql(state, config):
#     """Convert a natural language query into an SQL query with AI assistance."""
#     session = config.get("configurable", {}).get("session")
#     if not session:
#         raise ValueError("Session is not available in config.")

#     question = state["question"]
#     user_role = config.get("configurable", {}).get("role", "").lower()
#     garage_ids = state.get("garage_ids", [])

#     # ✅ Get the correct database schema dynamically
#     try:
#         tables = session.execute(text("SHOW TABLES")).fetchall()
#         table_names = [row[0] for row in tables]

#         schema = {}
#         for table in table_names:
#             columns = session.execute(text(f"DESC {table}")).fetchall()
#             schema[table] = [row[0] for row in columns]

#         logging.debug(f"🟢 Retrieved Database Schema: {json.dumps(schema, indent=2)}")
#     except Exception as e:
#         logging.error(f"❌ Error fetching database schema: {str(e)}")
#         state["query_result"] = {"error": "Could not fetch database schema."}
#         state["sql_error"] = True
#         return state

#     # ✅ Ensure owners can only see their own customers' data
#     if garage_ids:
#         if len(garage_ids) == 1:
#             customer_filter = f"cv.customer_id = {garage_ids[0]}"
#         else:
#             customer_filter = f"cv.customer_id IN ({', '.join(map(str, garage_ids))})"
#     else:
#         customer_filter = "1=0"  # Prevents unauthorized access if no valid customers are found

#     # ✅ Construct the schema text for AI
#     schema_text = "\n".join(
#         [f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema.items()]
#     )

#     # ✅ Updated SQL generation prompt with stricter constraints
#     prompt = f"""
# ### Instructions:
# You are an SQL query generator for MySQL. Follow these rules strictly:
# - **Only output a valid `SELECT` statement, without explanations or comments.**
# - **DO NOT include markdown (` ```sql `) or any other format artifacts.**
# - **Use table aliases (Example: `FROM vehicle_service_details vsd`).**
# - **Ensure correct `JOIN ON` conditions for table relationships.**
# - **Use ONLY the following valid tables and columns:**
#   {schema_text}
# - **NEVER use non-existent tables like `garages` or `users`.**
# - **For owners, ensure filtering is applied using:** `{customer_filter}`  
# - **DO NOT generate queries requiring write permissions (`INSERT`, `UPDATE`, `DELETE`).**

# #### Example Revenue Query for Owners:
# ```sql
# SELECT cv.customer_name, SUM(vss.total_paid) AS revenue 
# FROM customer_vehicle_info cv 
# JOIN vehicle_service_summary vss ON cv.customer_id = vss.customer_id 
# WHERE {customer_filter} 
# GROUP BY cv.customer_name;

# **Database Schema:**
#      {schema}

     

#      **User's Question:**
#      "{question}"

#      **Correct SQL Query:**
     


# """
#     try:
#     # ✅ Generate SQL query from AI
#         sql_query = query_ollama_together(prompt, "Qwen/Qwen2.5-Coder-32B-Instruct").strip()
#         logging.debug(f"🟢 Raw AI Output: {sql_query}")

#     # ✅ Ensure the AI-generated query is valid
#         if not sql_query.lower().startswith("select"):
#          logging.error(f"❌ Invalid SQL Query Generated: {sql_query}")
#          state["sql_error"] = True
#          state["query_result"] = {"error": "Query could not be generated correctly."}
#          return state

#     # ✅ Remove `WHERE g.owner_id IN ()` if AI mistakenly generates it
#         if user_role == "owner" and "WHERE g.owner_id IN ()" in sql_query:
#          sql_query = sql_query.replace("WHERE g.owner_id IN ()", f"WHERE {customer_filter}").strip()

#     # ✅ Store the final valid SQL query
#         state["sql_query"] = sql_query

#     except Exception as exc:  # Assign a default name 'error' instead of 'e'
#      logging.error(f"❌ SQL Generation Error: {str(exc)}")
#     state["sql_query"] = ""
#     state["sql_error"] = True
#     state["query_result"] = {"error": f"SQL Generation Error: {str(exc)}"}

#     return state




# def generate_human_readable_response_with_llama(state):
#     """Generate both a raw SQL query result and a human-readable response."""
    
#     question = state["question"]
#     query_result = state["query_result"]
#     sql_query = state["sql_query"]

#     # ✅ If there was an SQL error, return the raw error message
#     if state["sql_error"]:
#         state["query_result"] = {
#             "raw_answer": query_result,  # Preserve raw query result
#             "human_readable": f"An error occurred while executing the query: {query_result}"
#         }
#         return state

#     # ✅ If query results are empty, provide a better response
#     if not query_result or "data" not in query_result or not query_result["data"]:
#         state["query_result"] = {
#             "raw_answer": query_result,
#             "human_readable": "No relevant data found for your query."
#         }
#         return state

#     results = query_result["data"]

#     # ✅ Convert query results into a structured format for AI processing
#     formatted_results = "\n".join(
#         [" | ".join(f"{key}: {value}" for key, value in row.items()) for row in results]
#     )

#     # ✅ Improved prompt for concise human-readable response
#     prompt = f"""
#     You are an AI assistant that explains SQL query results in a **concise and direct** way.

#     **User's Question:** "{question}"
#     **SQL Query:** {sql_query}

#     **Database Results:** 
#     {formatted_results}

#     **Instructions:**
#     - Provide a **one-sentence answer** summarizing the key information.
#     - Do NOT assume missing details or add extra information.
#     - If multiple records exist, summarize them concisely.
#     - If there is no relevant data, respond with: "No relevant data found."

#     **Final Answer:**
#     """

#     try:
#         # ✅ Query a local LLM (replace "deepseek-llm:7b-chat" with your preferred model)
#         response = query_ollama_together(prompt,"meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo-128K" )
#         response = response.strip() if response else "No human-readable answer available."

#         state["query_result"] = {
#             "raw_answer": results,  # ✅ Store raw SQL results
#             "human_readable": response  # ✅ AI-generated explanation
#         }

#     except Exception as e:
#         state["query_result"] = {
#             "raw_answer": results,
#             "human_readable": f"Error generating explanation: {str(e)}"
#         }

#     return state



# ✅ Define workflow nodes
# workflow.add_node("convert_nl_to_sql", convert_nl_to_sql)
# workflow.add_node("execute_sql", execute_sql)
# workflow.add_edge("convert_nl_to_sql", "execute_sql")
# workflow.add_edge("execute_sql", END)
# workflow.set_entry_point("convert_nl_to_sql")


 

from langgraph.graph import StateGraph, END
from sql_agent import retrieve_similar_queries, save_sql_example
from sql_agent import query_ollama_together, get_database_schema
from sqlalchemy import text, exc
import logging
import json
from typing import List, Union, TypedDict
from sql_agent import SessionLocal
import re

# ✅ Define the Workflow State Schema
class AgentState(TypedDict):
    question: str
    sql_query: str
    query_result: Union[str, List[dict]]
    sql_error: bool

# ✅ Define workflow
workflow = StateGraph(state_schema=AgentState)



def clean_sql_query(query: str) -> str:
    """Cleans the generated SQL query by removing unwanted formatting, comments, and artifacts."""
    if not query:
        return ""

    query = query.strip().replace("ILIKE", "LIKE")

    # ✅ Remove unwanted markdown code block markers
    if query.startswith("```sql"):
        query = query[len("```sql"):].strip()
    if query.endswith("```"):
        query = query[:-3].strip()

    # ✅ Remove AI response artifacts
    if query.startswith("<s>"):
        query = query[len("<s>"):].strip()

    # ✅ Remove comments, notes, and irrelevant text
    query = re.sub(r'(?i)note:.*', '', query).strip()  # Removes 'Note: ...'
    query = re.sub(r'--.*', '', query).strip()          # Removes inline comments
    query = re.sub(r'/\*[\s\S]*?\*/', '', query).strip() # Removes block comments

    return query

def execute_sql(state, config):
    """Execute the SQL query and return results using SQLAlchemy."""
    session = config.get("configurable", {}).get("session")
    user_role = config.get("configurable", {}).get("role", "").lower()

    if not session:
        raise ValueError("Session is not available in config.")
    
    query = clean_sql_query(state["sql_query"]).strip()
    logging.debug(f"🟢 Executing SQL Query: {query}")

    # ✅ Define allowed tables per role
    ROLE_TABLE_ACCESS = {
        "admin": ["customer_vehicle_info", "job_card_details", "vehicle_service_details", "vehicle_service_summary"],
        "owner": ["job_card_details", "vehicle_service_details", "vehicle_service_summary","customer_vehicle_info"],
        "customer": ["customer_vehicle_info", "vehicle_service_summary"]
    }

    # ✅ Extract tables used in the query
    used_tables = [table for table in ROLE_TABLE_ACCESS["admin"] if table in query.lower()]

    # ✅ Check if query is out of domain
    allowed_tables = set(ROLE_TABLE_ACCESS.get(user_role, []))
    if not set(used_tables).issubset(allowed_tables):
        logging.error(f"❌ Query out of domain for role '{user_role}'. Query: {query}")
        state["query_result"] = {"raw_answer": "", "human_readable": "Query out of domain"}
        state["sql_error"] = True
        return state

    try:
        # ✅ Ensure it's a valid SELECT query
        if not query.lower().startswith("select"):
            logging.error(f"❌ Invalid Query Generated: {query}")
            raise ValueError("Invalid SQL query. Only SELECT statements are allowed.")

        # ✅ Execute the query
        result = session.execute(text(query))
        rows = result.fetchall()
        keys = result.keys()

        state["query_result"] = {"data": [dict(zip(keys, row)) for row in rows]}
        logging.debug(f"✅ Query Execution Successful. Results: {state['query_result']}")
        state["sql_error"] = False

    except exc.SQLAlchemyError as e:
        logging.error(f"❌ SQLAlchemy Error: {str(e)}")
        state["query_result"] = {"error": f"Database error: {str(e)}"}
        state["sql_error"] = True

    except Exception as e:
        logging.error(f"❌ General Error: {str(e)}")
        state["query_result"] = {"error": f"An error occurred: {str(e)}"}
        state["sql_error"] = True

    return generate_human_readable_response_with_llama(state)



def convert_nl_to_sql(state, config):
    """Convert a natural language query into an SQL query with RAG-based retrieval."""
    session = config.get("configurable", {}).get("session")
    if not session:
        raise ValueError("Session is not available in config.")

    question = state["question"]
    schema = get_database_schema(session)
    retrieved_queries = retrieve_similar_queries(question)
    retrieved_examples = "\n".join(retrieved_queries) if retrieved_queries else "No relevant examples found."

    user_role = config.get("configurable", {}).get("role", "").lower()
    garage_ids = state.get("garage_ids", [])

    # ✅ Ensure garage owners only see their own garages
    if garage_ids:
        if len(garage_ids) == 1:
            customer_filter = f"vs.garage_id = {garage_ids[0]}"
        else:
            customer_filter = f"vs.garage_id IN ({', '.join(map(str, garage_ids))})"
    else:
        customer_filter = "True"

    # ✅ Improved Prompt Instructions
    prompt = f"""
### Instructions:
You are a MySQL SQL query generator. Follow these rules:
- Only output a valid `SELECT` statement.
- Use table aliases and define them before use.
- Correctly apply `JOIN ON` conditions.
- - For owners:
  - Only calculate total revenue using `SUM(vs.total_amt)`.
  - Do NOT include `garage_name` or JOIN the `garages` table.
  - Always include `WHERE {customer_filter}`.

- Ensure table aliases are correctly defined in `FROM` or `JOIN` before use.

#### Database Schema:
{schema}

#### Example Queries:
{retrieved_examples}

#### User's Question:
"{question}"

#### Correct SQL Query:
"""

    try:
        sql_query = query_ollama_together(prompt, "Qwen/Qwen2.5-Coder-32B-Instruct").strip()
        logging.debug(f"🟢 Generated SQL Query: {sql_query}")

        sql_query = clean_sql_query(sql_query)

        # ✅ Force the garage filter for owners
        

        if not sql_query.lower().startswith("select"):
            state["sql_query"] = "Query could not be generated."
            state["query_result"] = {"error": "Query could not be generated correctly."}
            return state

        state["sql_query"] = sql_query
        with SessionLocal() as session:
            config = {"configurable": {"session": session, "role": user_role}}
        result = execute_sql(state, config)

        if result:
            logging.info(f"✅ SQL Query Successful:\n{sql_query}")
            logging.info(f"✅ Query Result: {result}")
        else:
            logging.warning("⚠️ No results found.")

        state["query_result"] = result

    except Exception as e:
        logging.error(f"❌ Error Occurred: {str(e)}")
        state["sql_query"] = "Query could not be generated."
        state["query_result"] = {"error": str(e)}

    return state

      
        



def generate_human_readable_response_with_llama(state):
     """Generate both a raw SQL query result and a human-readable response."""
    
     question = state["question"]
     query_result = state["query_result"]
     sql_query = state["sql_query"]

     # ✅ If there was an SQL error, return the raw error message
     if state["sql_error"]:
         state["query_result"] = {
             "raw_answer": query_result,
             "human_readable": f"An error occurred while executing the query: {query_result}"
         }
         return state

     # ✅ If query results are empty, provide a better response
     if not query_result or "data" not in query_result or not query_result["data"]:
         state["query_result"] = {
             "raw_answer": query_result,
             "human_readable": "No relevant data found for your query."
         }
         return state

     results = query_result["data"]

     # ✅ Convert query results into a structured format for AI processing
     formatted_results = "\n".join(
         [" | ".join(f"{key}: {value}" for key, value in row.items()) for row in results]
     )

     prompt = f"""
     You are an AI assistant that explains SQL query results in a **concise and direct** way.

     **User's Question:** "{question}"
     **SQL Query:** {sql_query}

     **Database Results:** 
     {formatted_results}

     **Final Answer:**
     """

     try:
         response = query_ollama_together(prompt, "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo-128K")
         response = response.strip() if response else "No human-readable answer available."

         state["query_result"] = {"raw_answer": results, "human_readable": response}

     except Exception as e:
         state["query_result"] = {"raw_answer": results, "human_readable": f"Error generating explanation: {str(e)}"}

     return state

workflow.add_node("convert_nl_to_sql", convert_nl_to_sql)
workflow.add_node("execute_sql", execute_sql)
workflow.add_edge("convert_nl_to_sql", "execute_sql")
workflow.add_edge("execute_sql", END)
workflow.set_entry_point("convert_nl_to_sql")



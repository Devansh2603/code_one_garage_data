

import os
import json
import faiss
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import logging

DATABASE_URL = os.getenv("DATABASE_URL_1", f"mysql+pymysql://root:devanshjoshi@localhost/flag_data")
# print(f"🔍 Using database: {DATABASE_URL}")

# # ✅ Create database engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# # ✅ Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# ✅ Define SQL examples storage file
EXAMPLES_FILE = "sql_examples.json"

# ✅ Load SQL examples from file
def load_sql_examples():
    if os.path.exists(EXAMPLES_FILE):
        with open(EXAMPLES_FILE, "r") as f:
            return json.load(f)
    return []

# ✅ Save new SQL query example
def save_sql_example(question, sql_query):
    examples = load_sql_examples()
    examples.append({"question": question, "sql_query": sql_query})

    with open(EXAMPLES_FILE, "w") as f:
        json.dump(examples, f, indent=4)

# ✅ Use a Local Embedding Model
def get_local_embeddings():
    return HuggingFaceEmbeddings(model_name="BAAI/bge-base-en-v1.5")

# ✅ Build FAISS Vector Store using Local LLM
def build_vector_store():
    """Converts SQL examples into embeddings and stores them in FAISS."""
    examples = load_sql_examples()
    
    if not examples:
        print("⚠️ No SQL examples found! Please add data to `sql_examples.json`.")
        return None

    texts = [ex["question"] + " | " + ex["sql_query"] for ex in examples]
    embeddings = get_local_embeddings()

    vector_store = FAISS.from_texts(texts, embeddings)
    vector_store.save_local("faiss_sql_db")
    print("✅ FAISS Vector Store built successfully!")

# ✅ Retrieve Similar SQL Queries
def retrieve_similar_queries(user_question, top_k=3):
     """Finds the most relevant SQL queries from FAISS."""
     embeddings = get_local_embeddings()
     vector_store = FAISS.load_local("faiss_sql_db", embeddings, allow_dangerous_deserialization=True)
     retriever = vector_store.as_retriever(search_kwargs={"k": top_k})
    
     docs = retriever.get_relevant_documents(user_question)
     return [doc.page_content for doc in docs]

 # ✅ Build FAISS DB at Startup (Only if Missing)
if os.path.exists("faiss_sql_db"):
     print("✅ FAISS database found, loading it...")
else:
     print("⚠️ FAISS database not found, rebuilding it...")
     build_vector_store()

# def retrieve_similar_queries(user_question, top_k=3):
#     """Finds the most relevant SQL queries from FAISS."""
#     embeddings = get_local_embeddings()
    
#     try:
#         vector_store = FAISS.load_local("faiss_sql_db", embeddings, allow_dangerous_deserialization=True)
#         retriever = vector_store.as_retriever(search_kwargs={"k": top_k})
#         docs = retriever.invoke(user_question)  # ✅ Fix: Use `.invoke()` instead of deprecated `.get_relevant_documents()`
        
#         return [doc.page_content for doc in docs]
    
#     except Exception as e:
#         logging.error(f"❌ FAISS Retrieval Error: {str(e)}")
#         return []
# # ✅ Ensure FAISS vector store is properly built
# if os.path.exists("faiss_sql_db"):
#     print("✅ FAISS database found, loading it...")
# else:
#     print("⚠️ FAISS database not found, rebuilding it...")
#     build_vector_store()




def query_ollama_together(prompt: str, model: str) -> str:
    """Interact with the Together API for SQL generation."""
    logging.info(f"🚀 Querying Together API with model: {model}")
    try:
        url = "https://api.together.xyz/v1/chat/completions"
        headers = {
            "Authorization": "Bearer d2e6fb732211ac24c7bd473cabe27ae43aab7cbf89c989c8e8c8a9458c49d77c",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,        # ✅ Set realistic token limit
            "temperature": 0.1,       # ✅ Lower temp for precise structure
            "top_p": 0.7,
            "top_k": 50,
            "repetition_penalty": 1.0,
            "stop": [";"]             # ✅ Safer stop token to prevent cutoff
        }

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        response_data = response.json()
        logging.debug(f"🔍 Together API Response: {json.dumps(response_data, indent=2)}")

        # ✅ Properly handle missing or invalid API responses
        if "choices" not in response_data or not response_data["choices"]:
            logging.error("❌ Together API response is missing 'choices' or is empty.")
            return "SELECT 'Query could not be generated' AS error;"

        sql_query = response_data["choices"][0]["message"]["content"].strip()


        # ✅ Improved Markdown formatting cleanup
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

        # ✅ Enhanced validation for empty or invalid SQL queries
        if not sql_query.lower().startswith("select"):
            logging.error(f"❌ AI failed to generate a valid SQL query: {sql_query}")
            return "SELECT 'Query could not be generated' AS error;"

        return sql_query

    except requests.RequestException as e:
        logging.error(f"❌ API Request Error: {str(e)}")
        return "SELECT 'Query could not be generated' AS error;"
    except ValueError as e:
        logging.error(f"❌ Value Error: {str(e)}")
        return "SELECT 'Query could not be generated' AS error;"
    except json.JSONDecodeError as e:
        logging.error(f"❌ JSON Decode Error: {str(e)}")
        return "SELECT 'Query could not be generated' AS error;"



def get_database_schema(session):
    """Retrieve the database schema dynamically from MySQL."""
    schema = {}

    try:
        # ✅ Fetch all table names
        tables_result = session.execute(text("SHOW TABLES")).fetchall()
        table_names = [table[0] for table in tables_result]

        for table in table_names:
            columns_result = session.execute(text(f"DESC `{table}`")).fetchall()
            schema[table] = [column[0] for column in columns_result]

         
    
    except Exception as e:
        logging.error(f"❌ Error fetching database schema: {str(e)}")
        return {}

    return schema


__all__ = ["SessionLocal"]

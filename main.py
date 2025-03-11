

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sql_agent import SessionLocal
#from models import Garage
from workflow_engine import workflow
import logging
from models import CustomerVehicleInfo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import Depends

import time  # ✅ Import time module

# ✅ Configure Logging
logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

# ✅ Request & Response Models
class QueryRequest(BaseModel):
    user_id: int  # 🔹 User selects their ID manually in UI
    role: str
    question: str

class QueryResponse(BaseModel):
    query_result: dict
    sql_error: bool
    execution_time: float  # ✅ Include execution time

# ✅ Get All Garage IDs for Owners
def get_user_vehicles(session, user_id):
    return [v.customer_id for v in session.query(CustomerVehicleInfo.customer_id).filter(CustomerVehicleInfo.customer_id == user_id).all()]


selected_garage = None

class GarageSelection(BaseModel):
    garage_name: str

@app.post("/set_garage/")
def set_garage(garage: GarageSelection):
    """Set the selected garage name dynamically."""
    global selected_garage
    selected_garage = garage.garage_name  # Store garage name globally (for now)
    return {"message": f"Garage set to {selected_garage}"}

@app.get("/get_garage/")
def get_garage():
    """Get the currently selected garage."""
    if not selected_garage:
        raise HTTPException(status_code=400, detail="No garage selected yet.")
    
    return {"selected_garage": selected_garage}  # ✅ Fix key name


def get_database_url():
    """Dynamically fetches the database URL based on the selected garage."""
    if not selected_garage:
        raise HTTPException(status_code=400, detail="No garage selected")
    return f"mysql+pymysql://root:devanshjoshi@localhost/{selected_garage}"

def get_session():
    """Returns a database session based on the selected garage."""
    database_url = get_database_url()
    engine = create_engine(database_url, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def get_db():
    """FastAPI dependency to provide a database session."""
    db = get_session()
    try:
        yield db
    finally:
        db.close()

@app.post("/ask_question", response_model=QueryResponse)
def ask_question(request: QueryRequest, db: Session = Depends(get_db)):
    """Process user query with role-based access control."""
    start_time = time.time()  # ✅ Start Timer

    user_role = request.role.lower()  # ✅ Ensure role comparison is case-insensitive
    user_id = request.user_id

    try:
        # ✅ Get Garage IDs for the user (Owners only)
        garage_ids = get_user_vehicles(db, user_id) if user_role == "owner" else []
        garage_condition = f"cv.customer_id IN ({', '.join(map(str, garage_ids))})" if garage_ids else ""

        # ✅ Initial Query State
        state = {
            "question": request.question,
            "sql_query": "",
            "query_result": {"raw_answer": "No data", "human_readable": "No response generated."},
            "sql_error": False,
            "garage_ids": garage_ids  # ✅ Always pass a list
        }
        config = {"configurable": {"session": db, "role": user_role}}  # ✅ Pass role to workflow

        logging.debug(f"Received query: {request.question} from user {request.user_id} with role {request.role}")

        # ✅ Compile and invoke workflow
        app_workflow = workflow.compile()
        result = app_workflow.invoke(input=state, config=config)

        execution_time = round(time.time() - start_time, 3)

        return QueryResponse(
            query_result=result.get("query_result", {}),
            sql_error=result["sql_error"],
            execution_time=execution_time  # ✅ Include execution time in response
        )

    except Exception as e:
        logging.error(f"❌ ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

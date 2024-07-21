from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timezone
from typing import List

# Set the database URL to a folder ../database in the project
DATABASE_URL = "sqlite:///../database/elevator.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ElevatorDemand(Base):
    __tablename__ = 'elevator_demand'
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    floor = Column(Integer, nullable=False)
    source_ip = Column(String, nullable=False)

class ElevatorState(Base):
    __tablename__ = 'elevator_state'
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    floor = Column(Integer, nullable=False)
    vacant = Column(Boolean, nullable=False)
    source_ip = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

class DemandCreate(BaseModel):
    floor: int

class StateCreate(BaseModel):
    floor: int
    vacant: bool

class DemandResponse(BaseModel):
    id: int
    timestamp: datetime
    floor: int
    source_ip: str

    class Config:
        orm_mode = True

class StateResponse(BaseModel):
    id: int
    timestamp: datetime
    floor: int
    vacant: bool
    source_ip: str

    class Config:
        orm_mode = True

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/demand", response_model=dict)
def create_demand(demand: DemandCreate, request: Request, db: Session = Depends(get_db)):
    source_ip = request.client.host
    new_demand = ElevatorDemand(floor=demand.floor, source_ip=source_ip)
    db.add(new_demand)
    db.commit()
    db.refresh(new_demand)
    return {"message": "Demand created"}

@app.post("/state", response_model=dict)
def create_state(state: StateCreate, request: Request, db: Session = Depends(get_db)):
    source_ip = request.client.host
    new_state = ElevatorState(floor=state.floor, vacant=state.vacant, source_ip=source_ip)
    db.add(new_state)
    db.commit()
    db.refresh(new_state)
    return {"message": "State created"}

@app.get("/demands", response_model=List[DemandResponse])
def list_demands(db: Session = Depends(get_db)):
    demands = db.query(ElevatorDemand).all()
    return demands

@app.get("/states", response_model=List[StateResponse])
def list_states(db: Session = Depends(get_db)):
    states = db.query(ElevatorState).all()
    return states

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

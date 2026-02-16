from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from db import SessionLocal

app = FastAPI(title="ServiceNow Incidents API")


# DB Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Health check
@app.get("/health")
def health():
    return {"status": "ok"}


# Latest incidents
@app.get("/incidents/latest")
def latest_incidents(
    limit: int = Query(10, ge=1, le=500),
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT *
        FROM incidents_full
        ORDER BY opened_at DESC
        LIMIT :limit
    """)
    rows = db.execute(query, {"limit": limit}).fetchall()
    return [dict(row._mapping) for row in rows]


# Get incident by number
@app.get("/incidents/{number}")
def get_incident(number: str, db: Session = Depends(get_db)):
    query = text("""
        SELECT *
        FROM incidents_full
        WHERE number = :number
    """)
    row = db.execute(query, {"number": number}).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Incident not found")

    return dict(row._mapping)


# Incidents since date
@app.get("/incidents")
def incidents_since(
    since: datetime = Query(..., description="YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    query = text("""
    SELECT *
    FROM incidents_full
    WHERE opened_at >= CAST(:since AS TIMESTAMP)
    ORDER BY opened_at
    LIMIT :limit
""")

    rows = db.execute(query, {"since": since, "limit": limit}).fetchall()
    return [dict(row._mapping) for row in rows]

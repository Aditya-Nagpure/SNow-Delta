from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from db import SessionLocal, engine
from sqlalchemy import text

app = FastAPI(title="ServiceNow Incidents API")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/incidents/latest")
def latest_incidents(limit: int = 10, db: Session = Depends(get_db)):
    q = text("""
        SELECT number, opened_at, short_description
        FROM incidents_minimal
        ORDER BY opened_at DESC
        LIMIT :limit
    """)
    rows = db.execute(q, {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/incidents/{number}")
def get_incident(number: str, db: Session = Depends(get_db)):
    q = text("""
        SELECT number, opened_at, short_description
        FROM incidents_minimal
        WHERE number = :number
    """)
    row = db.execute(q, {"number": number}).fetchone()

    if not row:
        return {"error": "Not found"}

    return dict(row._mapping)


@app.get("/incidents")
def incidents_since(
    since: str = Query(..., example="2026-02-01"),
    limit: int = 100,
    db: Session = Depends(get_db),
):
    q = text("""
        SELECT number, opened_at, short_description
        FROM incidents_minimal
        WHERE opened_at >= :since
        ORDER BY opened_at
        LIMIT :limit
    """)
    rows = db.execute(q, {"since": since}).fetchall()
    return [dict(r._mapping) for r in rows]

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from fastapi.responses import Response
from . import models
from .database_config import SessionLocal

app = FastAPI(title="Dynamic Database API")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/{schema}/{table}/")
async def read_table_data(
    schema: str,
    table: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get all records from a specified schema.table
    """
    try:
        DynamicTable = models.get_dynamic_table(schema, table)
        result = db.query(DynamicTable).offset(skip).limit(limit).all()
        if not result:
            return {
                "message": f"No records found in {schema}.{table}",
                "data": []
            }
        return [row._asdict() for row in result]
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Error accessing {schema}.{table}: {str(e)}"
        )

@app.get("/{schema}/{table}/{id}")
async def read_record_by_id(
    schema: str,
    table: str,
    id: int,
    db: Session = Depends(get_db)
):
    """
    Get a single record by ID from a specified schema.table
    """
    try:
        DynamicTable = models.get_dynamic_table(schema, table)
        result = db.query(DynamicTable).filter(DynamicTable.c.id == id).first()
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Record {id} not found in {schema}.{table}"
            )
        return result._asdict()
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Error accessing {schema}.{table}: {str(e)}"
        )

@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    """Handle favicon requests"""
    return Response(status_code=204)
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from asyncpg.exceptions import DataError, UniqueViolationError
from pydantic import BaseModel
import asyncpg
import os
from dotenv import load_dotenv
from typing import List, Optional
import json
import boto3

s3_client = boto3.client("s3")
bucket_name = 'user-02-juanda-smm-ueia-so'

class Dataset(BaseModel):
    name: str
    type_1: str
    type_2: Optional[str]
    generation: int
    legendary: bool

class ResponseModel(BaseModel):
    data: List[Dataset]
    page: int
    limit: int

class FullDataset(BaseModel):
    id: int
    name: str
    type_1: str
    type_2: Optional[str]
    total: int
    hp: int
    attack: int
    defense: int
    sp_atk: int
    sp_def: int
    speed: int
    generation: int
    legendary: bool

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

async def get_connection():
    return await asyncpg.connect(DATABASE_URL)

@app.on_event("startup")
async def startup_event():
    app.state.db = await get_connection()

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.db.close()

@app.get("/dataset", response_model=ResponseModel)
async def read_dataset(page: int = Query(1, ge=1), limit: int = Query(1, le=100), poke_generation: Optional[int] = Query(None, ge=1)):
    try: 
        offset = (page - 1) * limit
        rows = await app.state.db.fetch(f"""
            SELECT name, type_1, type_2, generation, legendary
            FROM pokemon
            LIMIT $1 OFFSET $2;
        """, limit, offset)

        if not rows: 
            raise HTTPException(status_code = 404, detail = "No records found")

        if poke_generation is not None:
            if poke_generation <= 0:
                raise HTTPException(status_code = 422, detail = "poke_generation must be greater than 0")
            rows = [row for row in rows if row['generation'] == poke_generation]
        
        if not rows:
            raise HTTPException(status_code = 404, detail = "No records found after applying the poken_generation filter")
        
        return ResponseModel(
            data = [Dataset(**row) for row in rows],
            page = page,
            limit = limit
        )
    except HTTPException as e:
        raise e
    except Exception as e: 
        raise HTTPException(status_code = 500, detail = "Internal Server Error")

@app.post("/dataset")
async def insert_dataset(datasets: List[FullDataset]):
    insert_query = """
        INSERT INTO pokemon (
            id, name, type_1, type_2, total, hp, attack, defense, sp_atk, sp_def, 
            speed, generation, legendary
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
        );
    """
    try:
        if not datasets:
            raise HTTPException(status_code=400, detail="No dataset provided")
        for dataset in datasets:
            await app.state.db.execute(insert_query, 
                dataset.id, dataset.name, dataset.type_1, dataset.type_2, dataset.total,
                dataset.hp, dataset.attack, dataset.defense, dataset.sp_atk, dataset.sp_def, 
                dataset.speed, dataset.generation, dataset.legendary
            )

        total_records = await app.state.db.fetchval("SELECT COUNT(*) FROM pokemon")

        json_data = json.dumps(dataset.dict())
        object_key = f"datasets/{dataset.name}.json"

        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=json_data)

        return {
            "message": f"{len(datasets)} records inserted successfully",
            "inserted_records": len(datasets),
            "total_records": total_records
        }
    except DataError as e:
        raise HTTPException(status_code=400, detail="Data error: invalid data format")
    except UniqueViolationError as e:
        raise HTTPException(status_code=400, detail="Unique constraint violation: duplicated data")
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Error campos en el request body",
        },
    )
"""
Research Rule Packs API
"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
import json
import time

from models.database import fetch_all, fetch_one, async_session, engine
from sqlalchemy import text

router = APIRouter(prefix="/api/rules", tags=["Rules"])
SCHEMA = "rb20_v2_5"

# Request Models
class RulePackCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    target_lib: str  # enum: H, E, F
    rules_json: list # e.g. [{"metric": "density", "op": ">", "val": 30}]
    parent_id: Optional[int] = None

class RulePackUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    rules_json: Optional[list] = None

@router.on_event("startup")
async def init_rules_table():
    """Ensure rule_packs table exists on startup"""
    async with engine.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.research_rule_packs (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                target_lib VARCHAR(10) NOT NULL,
                parent_id INTEGER REFERENCES {SCHEMA}.research_rule_packs(id),
                rules_json JSONB NOT NULL,
                author VARCHAR(50) DEFAULT 'system',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

@router.get("/")
async def list_rule_packs(target_lib: Optional[str] = None):
    query = f"SELECT * FROM {SCHEMA}.research_rule_packs"
    params = {}
    if target_lib:
        query += " WHERE target_lib = :target_lib"
        params["target_lib"] = target_lib
    query += " ORDER BY updated_at DESC"
    
    rows = await fetch_all(query, params)
    return {"data": [dict(r) for r in rows]}

@router.post("/")
async def create_rule_pack(pack: RulePackCreate):
    sql = f"""
        INSERT INTO {SCHEMA}.research_rule_packs 
        (name, description, target_lib, parent_id, rules_json)
        VALUES (:name, :description, :target_lib, :parent_id, :rules_json)
        RETURNING id
    """
    async with async_session() as session:
        result = await session.execute(text(sql), {
            "name": pack.name,
            "description": pack.description,
            "target_lib": pack.target_lib,
            "parent_id": pack.parent_id,
            "rules_json": json.dumps(pack.rules_json)
        })
        await session.commit()
        new_id = result.scalar()
        return {"success": True, "id": new_id}

@router.delete("/{pack_id}")
async def delete_rule_pack(pack_id: int):
    sql = f"DELETE FROM {SCHEMA}.research_rule_packs WHERE id = :id"
    async with async_session() as session:
        await session.execute(text(sql), {"id": pack_id})
        await session.commit()
    return {"success": True}

@router.put("/{pack_id}")
async def update_rule_pack(pack_id: int, pack: RulePackUpdate):
    updates = []
    params = {"id": pack_id}
    if pack.name is not None:
        updates.append("name = :name")
        params["name"] = pack.name
    if pack.description is not None:
        updates.append("description = :description")
        params["description"] = pack.description
    if pack.rules_json is not None:
        updates.append("rules_json = :rules_json")
        params["rules_json"] = json.dumps(pack.rules_json)
    
    if updates:
        sql = f"UPDATE {SCHEMA}.research_rule_packs SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = :id"
        async with async_session() as session:
            await session.execute(text(sql), params)
            await session.commit()
    return {"success": True}

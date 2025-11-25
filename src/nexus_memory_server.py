#!/usr/bin/env python3
"""
NEXUS Memory OpenAPI Server for OpenWebUI
Provides CRUD operations for persistent memory storage

This is an External Tool Server that connects to the NEXUS PostgreSQL database
for unlimited context memory operations.

Environment Variables:
  PG_HOST: PostgreSQL host (default: localhost)
  PG_PORT: PostgreSQL port (default: 5432)
  PG_USER: PostgreSQL user (default: postgres)
  PG_PASSWORD: PostgreSQL password (required)
  PG_DATABASE: Database name (default: CLAUDE)
  PORT: Server port (default: 8001)
"""

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, List
from datetime import datetime

# Configuration from environment variables
DB_CONFIG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DATABASE", "CLAUDE"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres")
}

PORT = int(os.environ.get("PORT", "8001"))

app = FastAPI(
    title="NEXUS Memory Server",
    description="Search, store, and manage memories from persistent memory database. Provides unlimited context CRUD operations.",
    version="2.0.0",
    servers=[{"url": f"http://localhost:{PORT}"}]
)

# ==================== Request/Response Models ====================

class MemorySearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 10

class MemoryCreateRequest(BaseModel):
    content: str
    topic: Optional[str] = "general"
    importance: Optional[int] = 5
    source: Optional[str] = "openwebui"

class MemoryUpdateRequest(BaseModel):
    content: Optional[str] = None
    topic: Optional[str] = None
    importance: Optional[int] = None

class MemoryResponse(BaseModel):
    id: int
    content: str
    timestamp: str
    topic: Optional[str]
    importance: Optional[int]

# ==================== Database Helper ====================

def get_db_connection():
    """Get database connection with error handling"""
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# ==================== Endpoints ====================

@app.get("/")
def root():
    """Root endpoint with API info"""
    return {
        "name": "NEXUS Memory Server",
        "version": "2.0.0",
        "description": "Persistent memory CRUD operations",
        "endpoints": ["/search", "/memories", "/stats", "/health"]
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/search", operation_id="search_memories")
def search_memories(request: MemorySearchRequest):
    """
    Search memories by content using text matching.

    Use this to find relevant memories based on keywords or phrases.
    Returns memories sorted by most recent first.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, content, timestamp, topic, importance
            FROM claude_memory
            WHERE content ILIKE %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (f'%{request.query}%', request.limit))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        formatted_results = []
        for row in results:
            formatted_results.append({
                "id": row["id"],
                "content": row["content"][:500] + "..." if len(row["content"]) > 500 else row["content"],
                "timestamp": str(row["timestamp"]),
                "topic": row["topic"],
                "importance": row["importance"]
            })

        return {
            "results": formatted_results,
            "count": len(formatted_results),
            "query": request.query
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"results": [], "count": 0, "error": str(e)}

@app.get("/memories", operation_id="list_memories")
def list_memories(limit: int = 20, offset: int = 0, topic: Optional[str] = None):
    """
    List recent memories with optional topic filter.

    Use this to browse stored memories or filter by topic.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if topic:
            cursor.execute("""
                SELECT id, content, timestamp, topic, importance
                FROM claude_memory
                WHERE topic = %s
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s
            """, (topic, limit, offset))
        else:
            cursor.execute("""
                SELECT id, content, timestamp, topic, importance
                FROM claude_memory
                ORDER BY timestamp DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return {
            "memories": [dict(row) for row in results],
            "count": len(results),
            "limit": limit,
            "offset": offset
        }

    except HTTPException:
        raise
    except Exception as e:
        return {"memories": [], "count": 0, "error": str(e)}

@app.get("/memories/{memory_id}", operation_id="get_memory")
def get_memory(memory_id: int):
    """
    Get a specific memory by ID.

    Returns the full content of a single memory.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, content, timestamp, topic, importance
            FROM claude_memory
            WHERE id = %s
        """, (memory_id,))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"Memory {memory_id} not found")

        return dict(result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/memories", operation_id="create_memory")
def create_memory(request: MemoryCreateRequest):
    """
    Store a new memory in the database.

    Use this to save important information for future reference.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO claude_memory (content, topic, importance, source, timestamp)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING id, content, timestamp, topic, importance
        """, (request.content, request.topic, request.importance, request.source))

        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": "Memory created successfully",
            "memory": dict(result)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/memories/{memory_id}", operation_id="update_memory")
def update_memory(memory_id: int, request: MemoryUpdateRequest):
    """
    Update an existing memory.

    Use this to modify the content, topic, or importance of a stored memory.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Build dynamic update query
        updates = []
        values = []

        if request.content is not None:
            updates.append("content = %s")
            values.append(request.content)
        if request.topic is not None:
            updates.append("topic = %s")
            values.append(request.topic)
        if request.importance is not None:
            updates.append("importance = %s")
            values.append(request.importance)

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        values.append(memory_id)

        cursor.execute(f"""
            UPDATE claude_memory
            SET {', '.join(updates)}
            WHERE id = %s
            RETURNING id, content, timestamp, topic, importance
        """, values)

        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"Memory {memory_id} not found")

        return {
            "success": True,
            "message": "Memory updated successfully",
            "memory": dict(result)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/memories/{memory_id}", operation_id="delete_memory")
def delete_memory(memory_id: int):
    """
    Delete a memory by ID.

    Use this to remove outdated or incorrect memories.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM claude_memory
            WHERE id = %s
            RETURNING id
        """, (memory_id,))

        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"Memory {memory_id} not found")

        return {
            "success": True,
            "message": f"Memory {memory_id} deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", operation_id="get_memory_stats")
def get_memory_stats():
    """
    Get memory database statistics.

    Returns total count, recent count, and topics breakdown.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total count
        cursor.execute("SELECT COUNT(*) as count FROM claude_memory")
        total_count = cursor.fetchone()["count"]

        # Recent count (last 7 days)
        cursor.execute("""
            SELECT COUNT(*) as count FROM claude_memory
            WHERE timestamp > NOW() - INTERVAL '7 days'
        """)
        recent_count = cursor.fetchone()["count"]

        # Topics breakdown
        cursor.execute("""
            SELECT topic, COUNT(*) as count
            FROM claude_memory
            GROUP BY topic
            ORDER BY count DESC
            LIMIT 10
        """)
        topics = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "total_memories": total_count,
            "recent_memories_7d": recent_count,
            "topics": [dict(t) for t in topics],
            "database": DB_CONFIG["database"],
            "status": "connected"
        }

    except HTTPException:
        raise
    except Exception as e:
        return {
            "total_memories": 0,
            "recent_memories_7d": 0,
            "topics": [],
            "database": "error",
            "error": str(e)
        }

@app.get("/openapi.json")
def get_openapi():
    """Return OpenAPI spec for External Tool registration"""
    return app.openapi()

# ==================== Main ====================

if __name__ == "__main__":
    import uvicorn
    print(f"Starting NEXUS Memory Server on port {PORT}")
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

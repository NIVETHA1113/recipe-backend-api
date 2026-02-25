from fastapi import FastAPI
import pymysql
from db import get_connection

app = FastAPI()
ALLOWED_OPERATORS = [">=", "<=", ">", "<", "="]

def parse_filter(value: str):
    if not value:
        return None, None

    value = value.strip()

    for op in ALLOWED_OPERATORS:
        if value.startswith(op):
            number_part = value[len(op):].strip()
            if number_part:
                return op, number_part

    return None, None
@app.get("/")
def home():
    return {"message": "Recipe API is running"}
@app.get("/api/recipes")
def get_recipes(page: int = 1, limit: int = 10):
    offset = (page - 1) * limit

    conn = get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query = """
    SELECT cuisine, title, rating, prep_time, cook_time,
           total_time, description, nutrients, serves
    FROM recipes
    ORDER BY rating DESC
    LIMIT %s OFFSET %s
    """

    cursor.execute(query, (limit, offset))
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results
@app.get("/api/recipes/search")
def search_recipes(
    cuisine: str = None,
    title: str = None,
    rating: str = None,
    total_time: str = None,
    calories: str = None
):
    conn = get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query = """
    SELECT cuisine, title, rating, prep_time, cook_time,
           total_time, description, nutrients, serves
    FROM recipes
    WHERE 1=1
    """

    params = []

    if cuisine:
        query += " AND cuisine = %s"
        params.append(cuisine)

    if title:
        query += " AND title LIKE %s"
        params.append(f"%{title}%")

    if rating:
        op, val = parse_filter(rating)
        if op in ALLOWED_OPERATORS:
            query += f" AND rating {op} %s"
            params.append(float(val))

    if total_time:
        op, val = parse_filter(total_time)
        if op in ALLOWED_OPERATORS:
            query += f" AND total_time {op} %s"
            params.append(int(val))

    if calories:
        op, val = parse_filter(calories)
        if op in ALLOWED_OPERATORS:
            query += f" AND JSON_EXTRACT(nutrients, '$.calories') {op} %s"
            params.append(int(val))

    cursor.execute(query, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    if not results:
        return {"message": "No recipes found matching criteria"}

    return results

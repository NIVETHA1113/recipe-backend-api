import json
import pymysql
from db import get_connection


def extract_number(value):
    if not value:
        return None
    try:
        return int(value.split()[0])
    except:
        return None


def clean_recipe(recipe):
    nutrients = recipe.get("nutrients") or {}

    cleaned_nutrients = {
        "calories": extract_number(nutrients.get("calories")),
        "carbohydrateContent": extract_number(nutrients.get("carbohydrateContent")),
        "cholesterolContent": extract_number(nutrients.get("cholesterolContent")),
        "fiberContent": extract_number(nutrients.get("fiberContent")),
        "proteinContent": extract_number(nutrients.get("proteinContent")),
        "saturatedFatContent": extract_number(nutrients.get("saturatedFatContent")),
        "sodiumContent": extract_number(nutrients.get("sodiumContent")),
        "sugarContent": extract_number(nutrients.get("sugarContent")),
        "fatContent": extract_number(nutrients.get("fatContent")),
        "unsaturatedFatContent": extract_number(nutrients.get("unsaturatedFatContent")),
    }

    return (
        recipe.get("cuisine"),
        recipe.get("title"),
        recipe.get("rating"),
        recipe.get("prep_time"),
        recipe.get("cook_time"),
        recipe.get("total_time"),
        recipe.get("description"),
        json.dumps(cleaned_nutrients),
        recipe.get("serves"),
    )


def load_data():
    with open("data.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    recipes = list(data.values())

    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO recipes
    (cuisine, title, rating, prep_time, cook_time, total_time,
     description, nutrients, serves)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch_size = 1000

    try:
        for i in range(0, len(recipes), batch_size):
            batch = recipes[i:i+batch_size]

            print(f"Inserting batch {i // batch_size + 1}")

            for recipe in batch:
                values = clean_recipe(recipe)
                cursor.execute(insert_query, values)

            conn.commit()

        print("Data inserted successfully!")

    except Exception as e:
        conn.rollback()
        print("Error occurred:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_data()
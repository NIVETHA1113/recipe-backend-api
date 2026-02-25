# 🍽 Recipe Backend API

A secure backend API built using FastAPI and MySQL for managing recipes.

## 🚀 Features
- Add new recipes
- View all recipes
- Get recipe by ID
- Secure database connection
- REST API endpoints
- JSON responses

## 🛠 Tech Stack
- FastAPI
- Python
- MySQL
- Pydantic
- Uvicorn

## 📂 Project Structure
recipe-backend/
│── main.py
│── db.py
│── models.py
│── requirements.txt

## ▶ How to run

Install dependencies

pip install -r requirements.txt

Run server

uvicorn main:app --reload

Server runs on:
http://127.0.0.1:8000

## 📌 API Endpoints

GET /recipes  
POST /recipes  
GET /recipes/{id}

## 👩‍💻 Author
NIVETHA J
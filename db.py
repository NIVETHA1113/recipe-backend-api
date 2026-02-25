import pymysql
def get_connection():
    return pymysql.connect(host='localhost',
                           user='root',
                           password='Saran@17',
                           database='recipe_assign',
                           autocommit=False
    )

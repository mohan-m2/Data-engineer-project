from databricks import sql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

connection = sql.connect(
    server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME'),
    http_path = os.getenv('DATABRICKS_HTTP_PATH'),
    access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
)

cursor = connection.cursor()

# List all schemas
cursor.execute("SHOW SCHEMAS")
print("Available schemas:")
for schema in cursor.fetchall():
    print(schema)

# Check current schema
cursor.execute("SELECT current_schema()")
print("\nCurrent schema:", cursor.fetchone())

# List tables in your schema
cursor.execute("SHOW TABLES IN mohan")
print("\nAvailable tables in 'mohan' schema:")
for table in cursor.fetchall():
    print(table)
    
print("\nDepartment data:")
cursor.execute("SELECT * FROM mohan.dept LIMIT 10")
dept_data = cursor.fetchall()
for row in dept_data:
    print(row)

print("\nEmployee data:")
cursor.execute("SELECT * FROM mohan.employee LIMIT 10")
emp_data = cursor.fetchall()
for row in emp_data:
    print(row)

cursor.close()
connection.close()
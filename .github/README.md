# AI Coding Agent Instructions

## Project Architecture
This is a FastAPI-based REST API project that provides dynamic access to PostgreSQL database tables.

### Key Components
- `utilities/`: Core package containing:
  - `api.py`: FastAPI application with dynamic routing
  - `models.py`: Dynamic table reflection using SQLAlchemy
  - `database_config.py`: Database connection management

### Design Patterns
1. Dynamic Table Access:
```python
# utilities/models.py
def get_dynamic_table(schema_name: str, table_name: str):
    metadata = MetaData(schema=schema_name)
    return Table(table_name, metadata, autoload_with=engine)
```

2. API Route Pattern:
- `/{schema}/{table}/` - List records
- `/{schema}/{table}/{id}` - Single record

## Developer Workflow

### Environment Setup
```powershell
# Create and activate venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install fastapi uvicorn sqlalchemy psycopg2-binary python-dotenv
pip freeze > requirements.txt
```

### Database Configuration
Create `.env` in project root:
```
DATABASE_URL=postgresql://username:password@localhost:5432/dbname
```

### Run Application
```powershell
uvicorn utilities.api:app --reload
```

## Project Conventions

### Database Access
- Always use schema-qualified tables (e.g., `employee.emp`)
- Use dynamic table reflection instead of static models
- Return dictionary responses using `_asdict()`

### Error Handling
- Wrap database operations in try/except
- Return 404 for missing records
- Use FastAPI HTTPException for errors

## Integration Points
- PostgreSQL database connection via SQLAlchemy
- FastAPI REST endpoints
- Swagger UI at `/docs`
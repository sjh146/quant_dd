import os
import pandas as pd
from sqlalchemy import create_engine

def verify_db():
    db_url = os.getenv("DB_URL", "postgresql://quant_user:quant_pass@postgres:5432/quant_db")
    engine = create_engine(db_url)
    
    query = "SELECT * FROM signal_logs ORDER BY timestamp DESC LIMIT 5"
    df = pd.read_sql(query, engine)
    
    print("\n--- Recent Signal Logs in PostgreSQL ---")
    print(df.to_string())

if __name__ == "__main__":
    verify_db()

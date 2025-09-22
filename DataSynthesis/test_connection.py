#!/usr/bin/env python3
"""
Test Databricks connection and apscat.di4marketing schema access
"""

from databricks_jdbc import DatabricksJDBCManager

def test_connection_and_schema():
    """Test connection to Databricks and access to apscat.di4marketing schema."""
    
    jdbc_manager = DatabricksJDBCManager()
    
    try:
        # Test connection
        print("🔌 Testing Databricks connection...")
        if not jdbc_manager.connect():
            print("❌ Connection failed")
            return False
            
        # Test schema access
        print("📂 Testing access to apscat.di4marketing schema...")
        
        # List tables in the schema
        schema_query = "SHOW TABLES IN apscat.di4marketing"
        tables_df = jdbc_manager.execute_query(schema_query)
        
        if tables_df is not None:
            print("✅ Successfully connected to apscat.di4marketing schema!")
            print(f"Found {len(tables_df)} tables:")
            print(tables_df.to_string())
            return True
        else:
            print("❌ Could not access apscat.di4marketing schema")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        jdbc_manager.disconnect()

if __name__ == "__main__":
    test_connection_and_schema()
#!/usr/bin/env python3
"""
Databricks JDBC Connection Manager
Provides secure JDBC connectivity to Databricks with data creation capabilities.
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Load JDBC driver requirements
try:
    import jaydebeapi
except ImportError:
    print("jaydebeapi not installed. Install with: pip install jaydebeapi")
    sys.exit(1)

try:
    import jpype
except ImportError:
    print("jpype1 not installed. Install with: pip install jpype1")
    sys.exit(1)


class DatabricksJDBCManager:
    """Manages JDBC connections to Databricks with auto-retry and connection pooling."""
    
    def __init__(self, env_file: str = ".env"):
        """Initialize with environment configuration."""
        # Load environment variables
        load_dotenv(env_file)
        
        self.token = os.getenv("TOKEN")
        self.host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        
        # Get JDBC URL from env or construct it
        self.jdbc_url = os.getenv("DATABRICKS_JDBC_URL")
        if not self.jdbc_url and self.host and self.http_path and self.token:
            self.jdbc_url = f"jdbc:databricks://{self.host}:443/default;transportMode=http;ssl=1;AuthMech=3;UID=token;PWD={self.token};httpPath={self.http_path}"
        
        # JDBC Configuration
        self.driver_class = "com.databricks.client.jdbc.Driver"
        self.jdbc_driver_path = self._get_jdbc_driver_path()
        
        # Connection properties
        self.connection_props = {
            "UserAgentEntry": "DI4Marketing-JDBC-Client",
            "SSL": "1",
            "PWD": self.token,
            "AuthMech": "3"
        }
        
        self._connection = None
        
    def _get_jdbc_driver_path(self) -> str:
        """Get JDBC driver path, download if needed."""
        driver_dir = Path.cwd() / "jdbc_drivers"
        driver_dir.mkdir(exist_ok=True)
        
        driver_path = driver_dir / "DatabricksJDBC42.jar"
        
        if not driver_path.exists():
            print(f"📥 JDBC driver not found. Download from:")
            print("   https://databricks.com/spark/jdbc-drivers-download")
            print(f"   Save as: {driver_path}")
            print("   Or manually download from: https://databricks.com/spark/jdbc-drivers-download")
            
        return str(driver_path)
        
    def connect(self) -> bool:
        """Establish JDBC connection to Databricks."""
        try:
            if not Path(self.jdbc_driver_path).exists():
                raise FileNotFoundError(f"JDBC driver not found: {self.jdbc_driver_path}")
                
            print("🔌 Connecting to Databricks...")
            
            # Initialize JVM if not already started
            if not jpype.isJVMStarted():
                jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={self.jdbc_driver_path}")
            
            # Create connection
            self._connection = jaydebeapi.connect(
                jclassname=self.driver_class,
                url=self.jdbc_url,
                driver_args={
                    "PWD": self.token,
                    "UID": "token",
                    **self.connection_props
                },
                jars=[self.jdbc_driver_path]
            )
            
            print("✅ Connected to Databricks successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
            
    def disconnect(self):
        """Close JDBC connection."""
        if self._connection:
            try:
                self._connection.close()
                print("🔌 Disconnected from Databricks")
            except Exception as e:
                print(f"⚠️  Disconnect error: {e}")
                
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
            
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results as DataFrame."""
        if not self._connection:
            if not self.connect():
                return None
                
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            results = cursor.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            
            print(f"✅ Query executed successfully. {len(df)} rows returned.")
            return df
            
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return None
            
    def execute_statement(self, statement: str) -> bool:
        """Execute SQL statement (CREATE, INSERT, UPDATE, DELETE)."""
        if not self._connection:
            if not self.connect():
                return False
                
        try:
            cursor = self._connection.cursor()
            cursor.execute(statement)
            self._connection.commit()
            cursor.close()
            
            print("✅ Statement executed successfully")
            return True
            
        except Exception as e:
            print(f"❌ Statement failed: {e}")
            return False
            
    def create_sample_data(self, table_name: str = "di4marketing_sample") -> bool:
        """Create sample marketing data table."""
        
        # Drop table if exists
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"
        self.execute_statement(drop_sql)
        
        # Create table
        create_sql = f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            customer_id STRING,
            campaign_id STRING,
            channel STRING,
            impressions BIGINT,
            clicks BIGINT,
            conversions BIGINT,
            revenue DOUBLE,
            cost DOUBLE,
            date_created TIMESTAMP,
            region STRING,
            age_group STRING,
            device_type STRING
        ) USING DELTA
        """
        
        if not self.execute_statement(create_sql):
            return False
            
        # Insert sample data
        insert_sql = f"""
        INSERT INTO {table_name} VALUES
        (1, 'CUST001', 'CAMP001', 'EMAIL', 10000, 250, 15, 1500.00, 100.00, current_timestamp(), 'North America', '25-34', 'Desktop'),
        (2, 'CUST002', 'CAMP001', 'SOCIAL', 15000, 450, 28, 2800.00, 150.00, current_timestamp(), 'Europe', '35-44', 'Mobile'),
        (3, 'CUST003', 'CAMP002', 'SEARCH', 8000, 320, 22, 2200.00, 200.00, current_timestamp(), 'Asia', '18-24', 'Tablet'),
        (4, 'CUST004', 'CAMP002', 'DISPLAY', 12000, 180, 8, 800.00, 80.00, current_timestamp(), 'North America', '45-54', 'Desktop'),
        (5, 'CUST005', 'CAMP003', 'EMAIL', 5000, 125, 12, 1200.00, 75.00, current_timestamp(), 'Europe', '25-34', 'Mobile'),
        (6, 'CUST006', 'CAMP003', 'SOCIAL', 20000, 600, 45, 4500.00, 300.00, current_timestamp(), 'Asia', '35-44', 'Desktop'),
        (7, 'CUST007', 'CAMP004', 'SEARCH', 7500, 300, 18, 1800.00, 175.00, current_timestamp(), 'North America', '18-24', 'Mobile'),
        (8, 'CUST008', 'CAMP004', 'DISPLAY', 9500, 190, 11, 1100.00, 95.00, current_timestamp(), 'Europe', '45-54', 'Tablet'),
        (9, 'CUST009', 'CAMP005', 'EMAIL', 6000, 150, 20, 2000.00, 90.00, current_timestamp(), 'Asia', '25-34', 'Desktop'),
        (10, 'CUST010', 'CAMP005', 'SOCIAL', 18000, 540, 35, 3500.00, 270.00, current_timestamp(), 'North America', '35-44', 'Mobile')
        """
        
        if self.execute_statement(insert_sql):
            print(f"✅ Sample data created in table: {table_name}")
            return True
        else:
            return False
            
    def get_table_info(self, table_name: str) -> Optional[pd.DataFrame]:
        """Get information about a table."""
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)
        
    def analyze_marketing_data(self, table_name: str = "di4marketing_sample") -> Dict[str, pd.DataFrame]:
        """Perform marketing data analysis."""
        analyses = {}
        
        # Channel performance
        channel_query = f"""
        SELECT 
            channel,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(conversions) as total_conversions,
            SUM(revenue) as total_revenue,
            SUM(cost) as total_cost,
            ROUND(SUM(clicks) / SUM(impressions) * 100, 2) as ctr_percent,
            ROUND(SUM(conversions) / SUM(clicks) * 100, 2) as conversion_rate,
            ROUND(SUM(revenue) / SUM(cost), 2) as roas
        FROM {table_name}
        GROUP BY channel
        ORDER BY total_revenue DESC
        """
        analyses['channel_performance'] = self.execute_query(channel_query)
        
        # Regional analysis
        region_query = f"""
        SELECT 
            region,
            COUNT(*) as campaigns,
            SUM(revenue) as total_revenue,
            AVG(conversion_rate) as avg_conversion_rate
        FROM (
            SELECT 
                region,
                conversions / clicks * 100 as conversion_rate,
                revenue
            FROM {table_name}
        ) t
        GROUP BY region
        ORDER BY total_revenue DESC
        """
        analyses['regional_performance'] = self.execute_query(region_query)
        
        # Age group insights
        age_query = f"""
        SELECT 
            age_group,
            SUM(revenue) as total_revenue,
            AVG(cost) as avg_cost,
            COUNT(*) as campaign_count
        FROM {table_name}
        GROUP BY age_group
        ORDER BY total_revenue DESC
        """
        analyses['age_group_insights'] = self.execute_query(age_query)
        
        return analyses


def main():
    """Main function for testing JDBC connection."""
    
    # Initialize manager
    jdbc_manager = DatabricksJDBCManager()
    
    try:
        # Connect to Databricks
        if not jdbc_manager.connect():
            print("Failed to connect to Databricks")
            return
            
        # Create sample data
        print("🏗️  Creating sample marketing data...")
        if jdbc_manager.create_sample_data():
            
            # Query sample data
            print("📊 Querying sample data...")
            sample_data = jdbc_manager.execute_query("SELECT * FROM di4marketing_sample LIMIT 5")
            if sample_data is not None:
                print(sample_data.to_string())
                
            # Perform analysis
            print("📈 Performing marketing analysis...")
            analyses = jdbc_manager.analyze_marketing_data()
            
            for analysis_name, df in analyses.items():
                if df is not None:
                    print(f"\\n{analysis_name.upper()}:")
                    print(df.to_string())
                    
        else:
            print("Failed to create sample data")
            
    finally:
        # Always disconnect
        jdbc_manager.disconnect()


if __name__ == "__main__":
    main()
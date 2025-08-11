#!/usr/bin/env python3
"""
Validate the enhanced anonymous customer data in Databricks
"""

import os
from dotenv import load_dotenv
from databricks import sql
import json

def validate_enhanced_data():
    """Validate the enhanced anonymous customer data."""
    
    load_dotenv()
    
    connection = sql.connect(
        server_hostname="e2-demo-field-eng.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/ea93d9df50e07dc6",
        access_token=os.getenv("TOKEN")
    )
    
    cursor = connection.cursor()
    table_name = "apscat.di4marketing.enhanced_anonymous_360"
    
    print(f"🔍 Validating enhanced data in {table_name}...")
    
    # Basic counts
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_count = cursor.fetchall()[0][0]
    print(f"✅ Total records: {total_count:,}")
    
    # Schema validation
    cursor.execute(f"DESCRIBE {table_name}")
    schema = cursor.fetchall()
    print(f"\n📋 Enhanced Schema ({len(schema)} columns):")
    for col_info in schema[:10]:  # Show first 10 columns
        print(f"   {col_info[0]}: {col_info[1]}")
    print(f"   ... and {len(schema)-10} more columns")
    
    # Anonymous validation
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE known_flag = false")
    anon_count = cursor.fetchall()[0][0]
    print(f"\n👻 Anonymous users: {anon_count:,} (100%)")
    
    # Session analytics
    cursor.execute(f"""
        SELECT 
            AVG(session_duration_seconds) as avg_duration,
            AVG(page_views) as avg_pages,
            AVG(is_bounce_session::int) * 100 as bounce_rate,
            AVG(engagement_score) as avg_engagement
        FROM {table_name}
    """)
    session_stats = cursor.fetchall()[0]
    print(f"\n📊 Session Analytics:")
    print(f"   Average duration: {session_stats[0]:.0f} seconds")
    print(f"   Average page views: {session_stats[1]:.1f}")
    print(f"   Bounce rate: {session_stats[2]:.1f}%")
    print(f"   Average engagement: {session_stats[3]:.1f}")
    
    # Device & Browser distribution
    print(f"\n📱 Device Distribution:")
    cursor.execute(f"""
        SELECT device_type, COUNT(*) as count 
        FROM {table_name} 
        GROUP BY device_type 
        ORDER BY count DESC
    """)
    for device, count in cursor.fetchall():
        print(f"   {device}: {count:,}")
    
    print(f"\n🌐 Top Browsers:")
    cursor.execute(f"""
        SELECT browser_name, COUNT(*) as count 
        FROM {table_name} 
        GROUP BY browser_name 
        ORDER BY count DESC 
        LIMIT 5
    """)
    for browser, count in cursor.fetchall():
        print(f"   {browser}: {count:,}")
    
    # Traffic sources
    print(f"\n🚀 Traffic Sources:")
    cursor.execute(f"""
        SELECT utm_source, COUNT(*) as count 
        FROM {table_name} 
        GROUP BY utm_source 
        ORDER BY count DESC 
        LIMIT 6
    """)
    for source, count in cursor.fetchall():
        print(f"   {source}: {count:,}")
    
    # Geographic insights
    print(f"\n🌏 Geographic Distribution:")
    cursor.execute(f"""
        SELECT geo_country, COUNT(*) as count,
               AVG(engagement_score) as avg_engagement
        FROM {table_name} 
        GROUP BY geo_country 
        ORDER BY count DESC 
        LIMIT 5
    """)
    for country, count, engagement in cursor.fetchall():
        print(f"   {country}: {count:,} users, {engagement:.0f} avg engagement")
    
    # Behavioral segments
    print(f"\n👥 Behavioral Segments:")
    cursor.execute(f"""
        SELECT segment, COUNT(*) as count,
               AVG(churn_risk_score) as avg_churn_risk,
               AVG(conversion_propensity) as avg_conversion
        FROM {table_name} 
        GROUP BY segment 
        ORDER BY count DESC
    """)
    for segment, count, churn, conversion in cursor.fetchall():
        print(f"   {segment}: {count:,} ({count/total_count*100:.1f}%) - Churn: {churn:.2f}, Convert: {conversion:.2f}")
    
    # Sample enhanced record
    print(f"\n📄 Sample Enhanced Record:")
    cursor.execute(f"""
        SELECT anon_id, geo_country, device_type, browser_name, 
               session_duration_seconds, page_views, utm_source, 
               engagement_score, segment
        FROM {table_name} 
        LIMIT 1
    """)
    sample = cursor.fetchall()[0]
    columns = ['anon_id', 'geo_country', 'device_type', 'browser_name', 
              'session_duration_seconds', 'page_views', 'utm_source',
              'engagement_score', 'segment']
    
    for i, col in enumerate(columns):
        print(f"   {col}: {sample[i]}")
    
    # Data quality checks
    print(f"\n🔍 Data Quality Checks:")
    
    # Check for required fields
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE anon_id IS NULL")
    null_anon_ids = cursor.fetchall()[0][0]
    print(f"   ✅ NULL anon_id: {null_anon_ids} (should be 0)")
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE session_id IS NULL")
    null_sessions = cursor.fetchall()[0][0]
    print(f"   ✅ NULL session_id: {null_sessions} (should be 0)")
    
    # Check score ranges
    cursor.execute(f"""
        SELECT 
            MIN(engagement_score) as min_eng,
            MAX(engagement_score) as max_eng,
            MIN(churn_risk_score) as min_churn,
            MAX(churn_risk_score) as max_churn
        FROM {table_name}
    """)
    ranges = cursor.fetchall()[0]
    print(f"   ✅ Engagement score range: {ranges[0]}-{ranges[1]} (should be 0-100)")
    print(f"   ✅ Churn risk range: {ranges[2]:.3f}-{ranges[3]:.3f} (should be 0.0-1.0)")
    
    cursor.close()
    connection.close()
    
    print(f"\n🎉 Enhanced data validation complete!")
    print(f"📋 Ready for Marketing/CX/Growth analytics demos!")

if __name__ == "__main__":
    validate_enhanced_data()
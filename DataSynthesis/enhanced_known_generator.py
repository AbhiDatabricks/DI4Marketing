#!/usr/bin/env python3
"""
Enhanced Known Customer Data Generator
Generates comprehensive known customer data with advanced tracking columns and personal identifiers.
"""

import pandas as pd
import numpy as np
import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
import os
from dotenv import load_dotenv
from databricks import sql

fake = Faker(['en_AU', 'ja_JP', 'ko_KR', 'zh_CN', 'en_IN'])

class EnhancedKnownGenerator:
    """Generates enhanced known customer data with realistic patterns and personal identifiers."""
    
    def __init__(self, num_records=90000):
        self.num_records = num_records
        
        # APJ-focused countries (same as anonymous version)
        self.apj_countries = {
            'Australia': {'weight': 15, 'cities': ['Sydney', 'Melbourne', 'Brisbane', 'Perth'], 'states': ['NSW', 'VIC', 'QLD', 'WA'], 'timezone': 'AEDT'},
            'Japan': {'weight': 20, 'cities': ['Tokyo', 'Osaka', 'Yokohama', 'Nagoya'], 'states': ['Tokyo', 'Osaka', 'Kanagawa', 'Aichi'], 'timezone': 'JST'},
            'South Korea': {'weight': 12, 'cities': ['Seoul', 'Busan', 'Incheon', 'Daegu'], 'states': ['Seoul', 'Busan', 'Incheon', 'Daegu'], 'timezone': 'KST'},
            'China': {'weight': 25, 'cities': ['Shanghai', 'Beijing', 'Guangzhou', 'Shenzhen'], 'states': ['Shanghai', 'Beijing', 'Guangdong', 'Guangdong'], 'timezone': 'CST'},
            'India': {'weight': 18, 'cities': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai'], 'states': ['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu'], 'timezone': 'IST'},
            'Singapore': {'weight': 5, 'cities': ['Singapore'], 'states': ['Singapore'], 'timezone': 'SGT'},
            'Thailand': {'weight': 3, 'cities': ['Bangkok', 'Chiang Mai'], 'states': ['Bangkok', 'Chiang Mai'], 'timezone': 'ICT'},
            'Malaysia': {'weight': 2, 'cities': ['Kuala Lumpur', 'Penang'], 'states': ['Selangor', 'Penang'], 'timezone': 'MYT'}
        }
        
        # Device and browser data (same as anonymous)
        self.devices = {
            'mobile': {'browsers': ['Chrome Mobile', 'Safari Mobile', 'Samsung Internet', 'Firefox Mobile'], 'os': ['Android', 'iOS']},
            'desktop': {'browsers': ['Chrome', 'Safari', 'Firefox', 'Edge'], 'os': ['Windows', 'macOS', 'Linux']},
            'tablet': {'browsers': ['Chrome Mobile', 'Safari Mobile'], 'os': ['Android', 'iOS']}
        }
        
        # Traffic sources (same as anonymous)
        self.utm_sources = ['google', 'facebook', 'instagram', 'linkedin', 'twitter', 'tiktok', 'youtube', 'bing', 'direct', 'organic']
        self.utm_mediums = ['cpc', 'organic', 'social', 'email', 'referral', 'direct', 'display', 'video']
        
        # Engagement segments (enhanced for known customers)
        self.segments = ['new-customer', 'returning', 'high-value', 'medium-value', 'low-value', 'vip', 'at-risk', 'loyal', 're-engaged']
        
        # Email domains by country
        self.email_domains = {
            'Australia': ['gmail.com', 'yahoo.com.au', 'outlook.com.au', 'bigpond.com', 'optusnet.com.au'],
            'Japan': ['gmail.com', 'yahoo.co.jp', 'outlook.jp', 'nifty.com', 'biglobe.ne.jp'],
            'South Korea': ['gmail.com', 'naver.com', 'daum.net', 'hanmail.net', 'korea.com'],
            'China': ['gmail.com', 'qq.com', '163.com', '126.com', 'sina.com'],
            'India': ['gmail.com', 'yahoo.in', 'rediffmail.com', 'outlook.in', 'hotmail.com'],
            'Singapore': ['gmail.com', 'yahoo.com.sg', 'outlook.sg', 'singnet.com.sg'],
            'Thailand': ['gmail.com', 'yahoo.co.th', 'hotmail.com', 'outlook.co.th'],
            'Malaysia': ['gmail.com', 'yahoo.com.my', 'hotmail.my', 'outlook.my']
        }
        
    def _generate_customer_id(self):
        """Generate realistic customer ID."""
        # Format: CUST_YYYYMMDD_XXXXXX (date + random)
        date_part = fake.date_between(start_date='-2y', end_date='now').strftime('%Y%m%d')
        random_part = f"{random.randint(100000, 999999):06d}"
        return f"CUST_{date_part}_{random_part}"
        
    def _generate_email(self, country):
        """Generate realistic email address based on country."""
        domains = self.email_domains.get(country, ['gmail.com', 'yahoo.com', 'outlook.com'])
        domain = random.choice(domains)
        
        # Generate realistic username
        first_name = fake.first_name().lower()
        last_name = fake.last_name().lower()
        
        # Various email patterns
        patterns = [
            f"{first_name}.{last_name}",
            f"{first_name}{last_name}",
            f"{first_name}.{last_name}{random.randint(1, 99)}",
            f"{first_name[0]}.{last_name}",
            f"{first_name}.{last_name[0]}",
            f"{first_name}{random.randint(1980, 2005)}"
        ]
        
        username = random.choice(patterns)
        # Remove any non-ascii characters for email compatibility
        username = ''.join(c for c in username if ord(c) < 128)
        
        return f"{username}@{domain}"
        
    def _generate_phone_number(self, country):
        """Generate realistic phone number based on country."""
        phone_patterns = {
            'Australia': '+61 4{} {} {}',
            'Japan': '+81 90 {} {}',
            'South Korea': '+82 10 {} {}',
            'China': '+86 138 {} {}',
            'India': '+91 98{} {} {}',
            'Singapore': '+65 9{} {}',
            'Thailand': '+66 8{} {}',
            'Malaysia': '+60 12 {} {}'
        }
        
        pattern = phone_patterns.get(country, '+1 555 {} {}')
        
        if country == 'Australia':
            return pattern.format(
                f"{random.randint(10, 99):02d}",
                f"{random.randint(100, 999):03d}",
                f"{random.randint(100, 999):03d}"
            )
        elif country in ['Japan', 'South Korea', 'Singapore']:
            return pattern.format(
                f"{random.randint(1000, 9999):04d}",
                f"{random.randint(1000, 9999):04d}"
            )
        elif country == 'China':
            return pattern.format(
                f"{random.randint(1000, 9999):04d}",
                f"{random.randint(1000, 9999):04d}"
            )
        elif country in ['Thailand', 'Malaysia']:
            return pattern.format(
                f"{random.randint(10, 99):02d}",
                f"{random.randint(1000, 9999):04d}"
            )
        elif country == 'India':
            return pattern.format(
                f"{random.randint(10, 99):02d}",
                f"{random.randint(100, 999):03d}",
                f"{random.randint(100, 999):03d}"
            )
        else:
            return pattern.format(
                f"{random.randint(100, 999):03d}",
                f"{random.randint(1000, 9999):04d}"
            )
        
    def _generate_geo_data(self):
        """Generate geographic data with timezone."""
        countries = list(self.apj_countries.keys())
        weights = [self.apj_countries[c]['weight'] for c in countries]
        
        country = np.random.choice(countries, p=np.array(weights)/sum(weights))
        country_data = self.apj_countries[country]
        
        city = random.choice(country_data['cities'])
        state = random.choice(country_data['states'])
        timezone = country_data['timezone']
        
        return country, state, city, timezone
        
    def _generate_device_data(self):
        """Generate realistic device, browser, and OS data."""
        device_type = np.random.choice(['mobile', 'desktop', 'tablet'], p=[0.65, 0.25, 0.10])
        device_info = self.devices[device_type]
        
        browser = random.choice(device_info['browsers'])
        os = random.choice(device_info['os'])
        
        # Generate realistic specs
        if device_type == 'mobile':
            screen_resolution = random.choice(['375x667', '414x896', '360x640', '393x851', '428x926'])
            viewport_size = screen_resolution
        elif device_type == 'desktop':
            screen_resolution = random.choice(['1920x1080', '1366x768', '1536x864', '2560x1440', '1440x900'])
            viewport_size = random.choice(['1200x800', '1366x768', '1536x864', '1920x1080'])
        else:  # tablet
            screen_resolution = random.choice(['768x1024', '1024x768', '820x1180', '810x1080'])
            viewport_size = screen_resolution
            
        return device_type, browser, os, screen_resolution, viewport_size
        
    def _generate_session_data(self):
        """Generate realistic session behavior data (known customers tend to engage more)."""
        # Known customers have better engagement patterns
        if random.random() < 0.15:  # 15% bounce (lower than anonymous)
            duration = random.randint(15, 60)
            page_views = 1
            is_bounce = True
        else:
            duration = np.random.gamma(3, 150)  # Slightly longer sessions
            duration = max(45, min(duration, 4800))  # 45sec to 1.3hr
            page_views = max(2, int(np.random.poisson(4.2)))  # More page views
            is_bounce = False
            
        # Calculate derived metrics
        avg_time_per_page = duration / page_views if page_views > 0 else duration
        scroll_depth = random.randint(35, 100) if not is_bounce else random.randint(15, 50)
        click_count = random.randint(1, 3) if is_bounce else random.randint(3, 20)
        
        return duration, page_views, is_bounce, avg_time_per_page, scroll_depth, click_count
        
    def _generate_utm_data(self):
        """Generate marketing attribution data."""
        # Known customers more likely to come from email/referrals
        if random.random() < 0.3:
            utm_source = random.choice(['direct', 'organic'])
            utm_medium = 'organic' if utm_source == 'organic' else 'direct'
            utm_campaign = None
        else:
            utm_source = random.choice(self.utm_sources)
            utm_medium = random.choice(self.utm_mediums)
            campaigns = ['customer_retention', 'loyalty_program', 'new_product_launch', 'personalized_offer', 'winback_campaign']
            utm_campaign = random.choice(campaigns)
            
        return utm_source, utm_medium, utm_campaign
        
    def _generate_engagement_scores(self):
        """Generate realistic engagement and propensity scores (higher for known customers)."""
        # Known customers have higher engagement
        engagement_score = max(0, min(100, int(np.random.normal(65, 18))))
        
        # Lower churn risk for known customers
        churn_score = np.random.beta(1.5, 6)  # Even more skewed towards lower churn
        
        # Higher conversion propensity
        conversion_propensity = np.random.beta(4, 6)  # Better conversion rates
        
        return engagement_score, round(churn_score, 3), round(conversion_propensity, 3)
        
    def _generate_event_sequence(self, page_views, click_count):
        """Generate realistic event sequence data."""
        events = []
        
        # Known customers visit more diverse pages
        page_types = ['homepage', 'product', 'category', 'search','cart', 'checkout', 'account', 'help', 'about', 'profile', 'orders', 'wishlist']
        
        for i in range(page_views):
            if i == 0:
                page = random.choice(['homepage', 'account', 'product'])  # Multiple entry points
            else:
                page = random.choice(page_types)
                
            event = {
                'page': page,
                'timestamp': fake.date_time_between(start_date='-1h', end_date='now').isoformat(),
                'duration_seconds': random.randint(15, 450)
            }
            events.append(event)
            
        return json.dumps(events)
        
    def generate_enhanced_dataset(self):
        """Generate the complete enhanced known customer dataset."""
        print(f"🏗️  Generating {self.num_records} enhanced known customer records...")
        
        data = []
        
        for i in range(self.num_records):
            # Basic geo and device data
            country, state, city, timezone = self._generate_geo_data()
            device_type, browser, os, screen_res, viewport = self._generate_device_data()
            
            # Customer identification
            customer_id = self._generate_customer_id()
            email = self._generate_email(country)
            phone_number = self._generate_phone_number(country)
            
            # Session behavior
            duration, page_views, is_bounce, avg_time_per_page, scroll_depth, click_count = self._generate_session_data()
            
            # Marketing attribution
            utm_source, utm_medium, utm_campaign = self._generate_utm_data()
            
            # Engagement metrics
            engagement_score, churn_score, conversion_propensity = self._generate_engagement_scores()
            
            # Event data
            event_count = max(2, page_views + random.randint(1, 8))  # Known customers have more events
            last_event = fake.date_time_between(start_date='-30d', end_date='now')  # More recent activity
            event_sequence = self._generate_event_sequence(page_views, click_count)
            
            # IP address (same logic as anonymous)
            ip_ranges = {
                'Australia': ['1.0.0', '14.0.0'],
                'Japan': ['126.0.0', '133.0.0'],
                'South Korea': ['1.201.0', '14.32.0'],
                'China': ['1.2.0', '14.144.0'],
                'India': ['1.23.0', '14.96.0'],
                'Singapore': ['8.20.0', '103.10.0'],
                'Thailand': ['1.46.0', '14.207.0'],
                'Malaysia': ['1.9.0', '14.102.0']
            }
            base_ip = random.choice(ip_ranges.get(country, ['192.168.0']))
            ip_address = f"{base_ip}.{random.randint(1, 254)}"
            
            # Enhanced segment assignment for known customers
            if is_bounce and engagement_score < 30:
                segment = 'low-engagement'
            if engagement_score > 85 and conversion_propensity > 0.7:
                segment = 'vip'
            elif engagement_score > 70:
                segment = 'high-engagement'
            elif churn_score > 0.7:
                segment = 'at-risk'
            elif event_count > 10:
                segment = 'loyal'
            elif engagement_score > 50:
                segment = 'medium-engagement'
            elif event_count < 3:
                segment = 'new-visitor'
            elif churn_score < 0.3 and engagement_score > 40:
                segment = 're-engaged'
            else:
                segment = 'returning'
            
            record = {
                # Core identification columns (KEY DIFFERENCE: These are populated)
                'customer_id': customer_id,
                'known_flag': True,
                'anon_id': f"KNOWN_{str(uuid.uuid4()).replace('-', '')[:12].upper()}",
                'email': email,
                'phone_number': phone_number,
                'geo_country': country,
                'geo_state': state,
                'geo_city': city,
                'ip_address': ip_address,
                'device_type': device_type,
                'event_count': event_count,
                'last_event_date': last_event,
                'segment': segment,
                
                # Enhanced columns for known customer tracking
                'session_id': f"SESS_{str(uuid.uuid4()).replace('-', '')[:16].upper()}",
                'session_duration_seconds': int(duration),
                'page_views': page_views,
                'is_bounce_session': is_bounce,
                'avg_time_per_page_seconds': round(avg_time_per_page, 1),
                'scroll_depth_percent': scroll_depth,
                'click_count': click_count,
                
                # Device & Tech
                'browser_name': browser,
                'operating_system': os,
                'screen_resolution': screen_res,
                'viewport_size': viewport,
                'timezone': timezone,
                
                # Marketing Attribution  
                'utm_source': utm_source,
                'utm_medium': utm_medium,
                'utm_campaign': utm_campaign,
                
                # Behavioral Analytics
                'engagement_score': engagement_score,
                'churn_risk_score': churn_score,
                'conversion_propensity': conversion_propensity,
                
                # Event Data
                'event_sequence_json': event_sequence,
                'landing_page': random.choice(['homepage', 'product', 'account', 'category']),
                'referrer_domain': utm_source if utm_source not in ['direct', 'organic'] else None,
                
                # Timing
                'local_visit_hour': random.randint(7, 22),  # Known customers during business hours
                'day_of_week': random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']),
                'is_weekend': random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']) in ['Saturday', 'Sunday']
            }
            
            data.append(record)
            
            if (i + 1) % 1000 == 0:
                print(f"   Generated {i + 1:,} known customer records...")
        
        df = pd.DataFrame(data)
        
        print("✅ Enhanced known customer dataset generation complete!")
        self._print_enhanced_summary(df)
        
        return df
        
    def _print_enhanced_summary(self, df):
        """Print enhanced dataset summary."""
        print(f"\n📊 Enhanced Known Customer Dataset Summary:")
        print(f"   Total records: {len(df):,}")
        print(f"   Average session duration: {df['session_duration_seconds'].mean():.0f} seconds")
        print(f"   Average page views: {df['page_views'].mean():.1f}")
        print(f"   Bounce rate: {df['is_bounce_session'].mean()*100:.1f}%")
        print(f"   Average engagement score: {df['engagement_score'].mean():.1f}")
        
        print(f"\n🌏 Top Countries:")
        for country, count in df['geo_country'].value_counts().head().items():
            print(f"   {country}: {count:,} ({count/len(df)*100:.1f}%)")
            
        print(f"\n📱 Browser Distribution:")
        for browser, count in df['browser_name'].value_counts().head().items():
            print(f"   {browser}: {count:,}")
            
        print(f"\n🚀 Traffic Sources:")
        for source, count in df['utm_source'].value_counts().head().items():
            print(f"   {source}: {count:,}")
            
        print(f"\n👑 Customer Segments:")
        for segment, count in df['segment'].value_counts().head().items():
            print(f"   {segment}: {count:,}")

class EnhancedDatabricksUploader:
    """Upload enhanced known customer data to Databricks."""
    
    def __init__(self):
        load_dotenv()
        self.server_hostname = "e2-demo-field-eng.cloud.databricks.com"
        self.http_path = "/sql/1.0/warehouses/862f1d757f0424f7"
        self.access_token = os.getenv("TOKEN")
        
    def upload_enhanced_data(self, df, table_name="enhanced_known_360"):
        """Upload enhanced known customer dataset to Databricks."""
        
        if not self.access_token:
            print("❌ TOKEN not found")
            return False
            
        try:
            print("🔌 Connecting to Databricks...")
            
            connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            cursor = connection.cursor()
            schema_name = "apscat.di4marketing"
            full_table_name = f"{schema_name}.{table_name}"
            
            print(f"🏗️  Creating enhanced known customer table {full_table_name}...")
            
            cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            
            # Enhanced table schema (same structure as anonymous but with populated identity fields)
            create_sql = f"""
            CREATE TABLE {full_table_name} (
                customer_id STRING,
                known_flag BOOLEAN,
                anon_id STRING,
                email STRING,
                phone_number STRING,
                geo_country STRING,
                geo_state STRING,
                geo_city STRING,
                ip_address STRING,
                device_type STRING,
                event_count BIGINT,
                last_event_date TIMESTAMP,
                segment STRING,
                
                session_id STRING,
                session_duration_seconds BIGINT,
                page_views BIGINT,
                is_bounce_session BOOLEAN,
                avg_time_per_page_seconds DOUBLE,
                scroll_depth_percent BIGINT,
                click_count BIGINT,
                
                browser_name STRING,
                operating_system STRING,
                screen_resolution STRING,
                viewport_size STRING,
                timezone STRING,
                
                utm_source STRING,
                utm_medium STRING,
                utm_campaign STRING,
                
                engagement_score BIGINT,
                churn_risk_score DOUBLE,
                conversion_propensity DOUBLE,
                
                event_sequence_json STRING,
                landing_page STRING,
                referrer_domain STRING,
                
                local_visit_hour BIGINT,
                day_of_week STRING,
                is_weekend BOOLEAN
            ) USING DELTA
            """
            
            cursor.execute(create_sql)
            print("✅ Enhanced known customer table created")
            
            # Upload in batches
            batch_size = 300
            total_batches = (len(df) + batch_size - 1) // batch_size
            
            print(f"📤 Uploading {len(df):,} enhanced known customer records in {total_batches} batches...")
            
            for i, batch_start in enumerate(range(0, len(df), batch_size)):
                batch_end = min(batch_start + batch_size, len(df))
                batch_df = df.iloc[batch_start:batch_end]
                
                values_list = []
                for _, row in batch_df.iterrows():
                    values = []
                    for col in df.columns:
                        val = row[col]
                        if pd.isna(val) or val is None:
                            values.append('NULL')
                        elif isinstance(val, str):
                            clean_val = val.replace("'", "''").replace('"', '""')
                            values.append(f"'{clean_val}'")
                        elif isinstance(val, bool):
                            values.append('true' if val else 'false')
                        elif isinstance(val, pd.Timestamp):
                            values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else:
                            values.append(str(val))
                    values_list.append(f"({', '.join(values)})")
                
                if values_list:
                    insert_sql = f"INSERT INTO {full_table_name} VALUES " + ', '.join(values_list)
                    cursor.execute(insert_sql)
                    print(f"   ✅ Batch {i+1}/{total_batches}")
            
            # Verify upload
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            count = cursor.fetchall()[0][0]
            
            cursor.close()
            connection.close()
            
            print(f"🎉 Enhanced known customer upload complete! {count:,} records in {full_table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Enhanced known customer upload failed: {e}")
            return False

def main():
    """Generate and upload enhanced known customer dataset."""
    
    # Generate enhanced known customer dataset
    generator = EnhancedKnownGenerator(num_records=90000)
    df = generator.generate_enhanced_dataset()
    
    # Save backup
    csv_file = "../enhanced_known_customer_data.csv"
    df.to_csv(csv_file, index=False)
    print(f"💾 Enhanced known customer data saved to {csv_file}")
    
    # Upload to Databricks
    uploader = EnhancedDatabricksUploader()
    success = uploader.upload_enhanced_data(df, "enhanced_known_360")
    
    if success:
        print("🚀 Enhanced known customer data ready in Databricks!")
        print("   Table: apscat.di4marketing.enhanced_known_360")
    
    return df

if __name__ == "__main__":
    main()

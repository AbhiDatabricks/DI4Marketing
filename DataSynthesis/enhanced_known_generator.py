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
        
        # APJ-focused countries with enhanced locality data
        self.apj_countries = {
            'Australia': {
                'weight': 15, 
                'cities': ['Sydney', 'Melbourne', 'Brisbane', 'Perth'], 
                'states': ['NSW', 'VIC', 'QLD', 'WA'], 
                'timezone': 'AEDT',
                'city_types': {'Sydney': 'metro', 'Melbourne': 'metro', 'Brisbane': 'metro', 'Perth': 'metro'}
            },
            'Japan': {
                'weight': 20, 
                'cities': ['Tokyo', 'Osaka', 'Yokohama', 'Nagoya'], 
                'states': ['Tokyo', 'Osaka', 'Kanagawa', 'Aichi'], 
                'timezone': 'JST',
                'city_types': {'Tokyo': 'metro', 'Osaka': 'metro', 'Yokohama': 'metro', 'Nagoya': 'metro'}
            },
            'South Korea': {
                'weight': 12, 
                'cities': ['Seoul', 'Busan', 'Incheon', 'Daegu'], 
                'states': ['Seoul', 'Busan', 'Incheon', 'Daegu'], 
                'timezone': 'KST',
                'city_types': {'Seoul': 'metro', 'Busan': 'metro', 'Incheon': 'metro', 'Daegu': 'metro'}
            },
            'China': {
                'weight': 25, 
                'cities': ['Shanghai', 'Beijing', 'Guangzhou', 'Shenzhen'], 
                'states': ['Shanghai', 'Beijing', 'Guangdong', 'Guangdong'], 
                'timezone': 'CST',
                'city_types': {'Shanghai': 'metro', 'Beijing': 'metro', 'Guangzhou': 'metro', 'Shenzhen': 'metro'}
            },
            'India': {
                'weight': 18, 
                'cities': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai'], 
                'states': ['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu'], 
                'timezone': 'IST',
                'city_types': {'Mumbai': 'metro', 'Delhi': 'metro', 'Bangalore': 'metro', 'Chennai': 'metro'}
            },
            'Singapore': {
                'weight': 5, 
                'cities': ['Singapore'], 
                'states': ['Singapore'], 
                'timezone': 'SGT',
                'city_types': {'Singapore': 'metro'}
            },
            'Thailand': {
                'weight': 3, 
                'cities': ['Bangkok', 'Chiang Mai'], 
                'states': ['Bangkok', 'Chiang Mai'], 
                'timezone': 'ICT',
                'city_types': {'Bangkok': 'metro', 'Chiang Mai': 'metro'}
            },
            'Malaysia': {
                'weight': 2, 
                'cities': ['Kuala Lumpur', 'Penang'], 
                'states': ['Selangor', 'Penang'], 
                'timezone': 'MYT',
                'city_types': {'Kuala Lumpur': 'metro', 'Penang': 'metro'}
            }
        }
        
        # Geographic locality types with behavioral implications
        self.locality_types = ['metro', 'suburban', 'regional']
        
        # Locality type distributions (can be overridden by city-specific mapping)
        self.locality_weights = [0.55, 0.30, 0.15]  # Metro-dominant APJ region
        
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
        # Age band definitions and weights (skewed toward younger adults)
        self.age_bands = [
            (18, 24, '18-24'),
            (25, 34, '25-34'),
            (35, 44, '35-44'),
            (45, 54, '45-54'),
            (55, 64, '55-64'),
            (65, 99, '65+')
        ]
        self.age_band_weights = [0.28, 0.32, 0.18, 0.12, 0.07, 0.03]  # Skewed toward 18-34
        
        # Income level definitions with age-based probability matrices
        self.income_levels = ['budget_conscious', 'mid_tier', 'luxury']
        
        # Income level probabilities by age band (rows: age bands, cols: income levels)
        # Younger people tend to be more budget conscious, older people have higher luxury rates
        self.income_probabilities = {
            '18-24': [0.65, 0.30, 0.05],  # Students and entry-level workers
            '25-34': [0.45, 0.45, 0.10],  # Young professionals
            '35-44': [0.30, 0.50, 0.20],  # Peak earning years begin
            '45-54': [0.25, 0.45, 0.30],  # Peak earning years
            '55-64': [0.30, 0.40, 0.30],  # High earners but some downsizing
            '65+': [0.40, 0.35, 0.25]     # Fixed income but accumulated wealth
        }
        
        # Life stage definitions with age-based probability matrices
        self.life_stages = ['student', 'young_professional', 'parent', 'empty_nester', 'retiree']
        
        # Life stage probabilities by age band
        self.life_stage_probabilities = {
            '18-24': [0.60, 0.35, 0.05, 0.00, 0.00],  # Mostly students and young professionals
            '25-34': [0.10, 0.55, 0.35, 0.00, 0.00],  # Career focus and starting families
            '35-44': [0.02, 0.25, 0.70, 0.03, 0.00],  # Peak parenting years
            '45-54': [0.00, 0.15, 0.60, 0.25, 0.00],  # Still parents but some empty nesters
            '55-64': [0.00, 0.10, 0.20, 0.60, 0.10],  # Empty nesters and pre-retirees
            '65+': [0.00, 0.05, 0.05, 0.20, 0.70]     # Mostly retirees
        }
        
        # Purchase behavior categories
        self.purchase_frequencies = ['low_frequency', 'medium_frequency', 'high_frequency']
        self.purchase_values = ['low_value', 'medium_value', 'high_value']
        self.brand_loyalties = ['brand_switcher', 'moderately_loyal', 'brand_loyal']
        self.shopping_channels = ['online_only', 'offline_only', 'omnichannel']
        
        # Purchase frequency by income level (purchases per month)
        self.purchase_freq_by_income = {
            'budget_conscious': [0.70, 0.25, 0.05],  # Mostly low frequency
            'mid_tier': [0.30, 0.55, 0.15],          # Balanced toward medium
            'luxury': [0.15, 0.35, 0.50]             # Higher frequency for luxury
        }
        
        # Purchase value by age band and income level
        self.purchase_value_matrix = {
            ('18-24', 'budget_conscious'): [0.85, 0.15, 0.00],
            ('18-24', 'mid_tier'): [0.60, 0.35, 0.05],
            ('18-24', 'luxury'): [0.30, 0.50, 0.20],
            ('25-34', 'budget_conscious'): [0.75, 0.20, 0.05],
            ('25-34', 'mid_tier'): [0.45, 0.45, 0.10],
            ('25-34', 'luxury'): [0.20, 0.50, 0.30],
            ('35-44', 'budget_conscious'): [0.70, 0.25, 0.05],
            ('35-44', 'mid_tier'): [0.35, 0.50, 0.15],
            ('35-44', 'luxury'): [0.15, 0.45, 0.40],
            ('45-54', 'budget_conscious'): [0.65, 0.30, 0.05],
            ('45-54', 'mid_tier'): [0.30, 0.50, 0.20],
            ('45-54', 'luxury'): [0.10, 0.40, 0.50],
            ('55-64', 'budget_conscious'): [0.70, 0.25, 0.05],
            ('55-64', 'mid_tier'): [0.35, 0.45, 0.20],
            ('55-64', 'luxury'): [0.15, 0.35, 0.50],
            ('65+', 'budget_conscious'): [0.75, 0.20, 0.05],
            ('65+', 'mid_tier'): [0.40, 0.45, 0.15],
            ('65+', 'luxury'): [0.20, 0.40, 0.40]
        }
        
        # Brand loyalty by life stage
        self.brand_loyalty_by_life_stage = {
            'student': [0.60, 0.30, 0.10],           # Price-sensitive, brand switching
            'young_professional': [0.45, 0.40, 0.15], # Exploring brands
            'parent': [0.25, 0.45, 0.30],            # More loyal for family brands
            'empty_nester': [0.30, 0.40, 0.30],      # Established preferences
            'retiree': [0.20, 0.35, 0.45]            # Very loyal to trusted brands
        }
        
        # Shopping channel preferences by locality and age
        self.channel_by_locality = {
            'metro': [0.45, 0.15, 0.40],      # High omnichannel adoption
            'suburban': [0.35, 0.25, 0.40],   # Balanced across channels
            'regional': [0.25, 0.45, 0.30]    # More offline preference
        }
        
        # Behavioral traits based on existing features
        self.engagement_behaviors = ['passive', 'browser', 'researcher', 'active_buyer']
        self.price_sensitivities = ['price_conscious', 'value_seeker', 'premium_buyer']
        self.product_interests = ['electronics', 'fashion', 'home_garden', 'health_beauty', 'books_media', 'sports_outdoors']

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
        """Generate geographic data with timezone and locality type."""
        countries = list(self.apj_countries.keys())
        weights = [self.apj_countries[c]['weight'] for c in countries]
        country = np.random.choice(countries, p=np.array(weights)/sum(weights))
        country_data = self.apj_countries[country]
        city = random.choice(country_data['cities'])
        state = random.choice(country_data['states'])
        timezone = country_data['timezone']
        
        # Get locality type from city mapping or random if not specified
        if 'city_types' in country_data and city in country_data['city_types']:
            locality_type = country_data['city_types'][city]
        else:
            locality_type = np.random.choice(self.locality_types, p=self.locality_weights)
        
        return country, state, city, timezone, locality_type
        
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
    
    def _generate_age_and_band(self):
        # Choose an age band based on weights
        band_idx = np.random.choice(len(self.age_bands), p=self.age_band_weights)
        band = self.age_bands[band_idx]
        age = random.randint(band[0], band[1])
        today = datetime.today()
        dob = today - timedelta(days=age*365 + random.randint(0, 364))
        return dob.date(), age, band[2]
    
    def _generate_income_level(self, age_band):
        """Generate income level based on age band with realistic distribution."""
        probabilities = self.income_probabilities[age_band]
        income_idx = np.random.choice(len(self.income_levels), p=probabilities)
        return self.income_levels[income_idx]
    
    def _generate_life_stage(self, age_band):
        """Generate life stage based on age band with realistic distribution."""
        probabilities = self.life_stage_probabilities[age_band]
        life_stage_idx = np.random.choice(len(self.life_stages), p=probabilities)
        return self.life_stages[life_stage_idx]
    
    def _generate_purchase_behavior(self, age_band, income_level, life_stage, locality_type):
        """Generate comprehensive purchase behavior based on demographics."""
        # Purchase frequency based on income level
        freq_probs = self.purchase_freq_by_income[income_level]
        purchase_frequency = np.random.choice(self.purchase_frequencies, p=freq_probs)
        
        # Purchase value based on age and income combination
        value_key = (age_band, income_level)
        if value_key in self.purchase_value_matrix:
            value_probs = self.purchase_value_matrix[value_key]
        else:
            # Default probabilities if combination not found
            value_probs = [0.50, 0.35, 0.15]
        purchase_value = np.random.choice(self.purchase_values, p=value_probs)
        
        # Brand loyalty based on life stage
        loyalty_probs = self.brand_loyalty_by_life_stage[life_stage]
        brand_loyalty = np.random.choice(self.brand_loyalties, p=loyalty_probs)
        
        # Shopping channel based on locality
        channel_probs = self.channel_by_locality[locality_type]
        shopping_channel = np.random.choice(self.shopping_channels, p=channel_probs)
        
        return purchase_frequency, purchase_value, brand_loyalty, shopping_channel
    
    def _generate_behavioral_traits(self, engagement_score, income_level, age_band, life_stage):
        """Generate behavioral traits based on existing engagement and demographic data."""
        # Engagement behavior based on engagement score
        if engagement_score >= 80:
            engagement_behavior = 'active_buyer'
        elif engagement_score >= 60:
            engagement_behavior = 'researcher'
        elif engagement_score >= 40:
            engagement_behavior = 'browser'
        else:
            engagement_behavior = 'passive'
        
        # Price sensitivity based on income level and life stage
        if income_level == 'luxury':
            price_sensitivity = np.random.choice(self.price_sensitivities, p=[0.15, 0.35, 0.50])
        elif income_level == 'mid_tier':
            price_sensitivity = np.random.choice(self.price_sensitivities, p=[0.30, 0.50, 0.20])
        else:  # budget_conscious
            price_sensitivity = np.random.choice(self.price_sensitivities, p=[0.70, 0.25, 0.05])
        
        # Product interest based on age band and life stage
        if life_stage == 'student':
            product_interest = np.random.choice(['electronics', 'fashion', 'books_media'])
        elif life_stage == 'young_professional':
            product_interest = np.random.choice(['electronics', 'fashion', 'health_beauty'])
        elif life_stage == 'parent':
            product_interest = np.random.choice(['home_garden', 'health_beauty', 'electronics'])
        elif life_stage == 'empty_nester':
            product_interest = np.random.choice(['home_garden', 'health_beauty', 'sports_outdoors'])
        else:  # retiree
            product_interest = np.random.choice(['home_garden', 'health_beauty', 'books_media'])
        
        return engagement_behavior, price_sensitivity, product_interest
    
    def generate_enhanced_dataset(self):
        """Generate the complete enhanced known customer dataset."""
        print(f"\U0001F3D7️  Generating {self.num_records} enhanced known customer records...")
        data = []
        for i in range(self.num_records):
            # Basic geo and device data
            country, state, city, timezone, locality_type = self._generate_geo_data()
            device_type, browser, os, screen_res, viewport = self._generate_device_data()
            # Customer identification
            customer_id = self._generate_customer_id()
            email = self._generate_email(country)
            phone_number = self._generate_phone_number(country)
            # Age and band
            dob, age, age_band = self._generate_age_and_band()
            # Demographics based on age
            income_level = self._generate_income_level(age_band)
            life_stage = self._generate_life_stage(age_band)
            # Session behavior
            duration, page_views, is_bounce, avg_time_per_page, scroll_depth, click_count = self._generate_session_data()
            # Marketing attribution
            utm_source, utm_medium, utm_campaign = self._generate_utm_data()
            # Engagement metrics
            engagement_score, churn_score, conversion_propensity = self._generate_engagement_scores()
            # Purchase behavior and behavioral traits
            purchase_frequency, purchase_value, brand_loyalty, shopping_channel = self._generate_purchase_behavior(
                age_band, income_level, life_stage, locality_type)
            engagement_behavior, price_sensitivity, product_interest = self._generate_behavioral_traits(
                engagement_score, income_level, age_band, life_stage)
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
                'dob': str(dob),
                'age': age,
                'age_band': age_band,
                'income_level': income_level,
                'life_stage': life_stage,
                'locality_type': locality_type,
                'purchase_frequency': purchase_frequency,
                'purchase_value': purchase_value,
                'brand_loyalty': brand_loyalty,
                'shopping_channel': shopping_channel,
                'engagement_behavior': engagement_behavior,
                'price_sensitivity': price_sensitivity,
                'product_interest': product_interest,
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
        print(f"   Age band distribution:")
        print(df['age_band'].value_counts(normalize=True).sort_index())
        print(f"\n💰 Income Level Distribution:")
        print(df['income_level'].value_counts(normalize=True).sort_index())
        print(f"\n🎯 Life Stage Distribution:")
        print(df['life_stage'].value_counts(normalize=True).sort_index())
        print(f"\n🏢 Locality Type Distribution:")
        print(df['locality_type'].value_counts(normalize=True).sort_index())
        print(f"\n🛒 Purchase Behavior Summary:")
        print(f"   Purchase Frequency: {df['purchase_frequency'].value_counts(normalize=True).to_dict()}")
        print(f"   Purchase Value: {df['purchase_value'].value_counts(normalize=True).to_dict()}")
        print(f"   Brand Loyalty: {df['brand_loyalty'].value_counts(normalize=True).to_dict()}")
        print(f"   Shopping Channel: {df['shopping_channel'].value_counts(normalize=True).to_dict()}")
        print(f"\n🎯 Behavioral Traits:")
        print(f"   Engagement Behavior: {df['engagement_behavior'].value_counts(normalize=True).to_dict()}")
        print(f"   Price Sensitivity: {df['price_sensitivity'].value_counts(normalize=True).to_dict()}")
        print(f"   Top Product Interests: {dict(df['product_interest'].value_counts(normalize=True).head(3))}")
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
            print(f"\U0001F3D7️  Creating enhanced known customer table {full_table_name}...")
            cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
            # Enhanced table schema (same structure as anonymous but with populated identity fields)
            create_sql = f"""
            CREATE TABLE {full_table_name} (
                customer_id STRING,
                known_flag BOOLEAN,
                anon_id STRING,
                email STRING,
                phone_number STRING,
                dob DATE,
                age INT,
                age_band STRING,
                income_level STRING,
                life_stage STRING,
                locality_type STRING,
                purchase_frequency STRING,
                purchase_value STRING,
                brand_loyalty STRING,
                shopping_channel STRING,
                engagement_behavior STRING,
                price_sensitivity STRING,
                product_interest STRING,
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
            batch_size = 300
            total_batches = (len(df) + batch_size - 1) // batch_size
            print(f"\U0001F4E4 Uploading {len(df):,} enhanced known customer records in {total_batches} batches...")
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
                        elif isinstance(val, (pd._libs.tslibs.nattype.NaTType, type(None))):
                            values.append('NULL')
                        else:
                            values.append(str(val))
                    values_list.append(f"({', '.join(values)})")
                if values_list:
                    insert_sql = f"INSERT INTO {full_table_name} VALUES " + ', '.join(values_list)
                    cursor.execute(insert_sql)
                    print(f"   ✅ Batch {i+1}/{total_batches}")
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

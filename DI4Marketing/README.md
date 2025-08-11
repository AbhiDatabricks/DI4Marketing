# DI4Marketing - Databricks for Marketing Demo Suite

## Overview

This project addresses the growing opportunity for Databricks to support Marketing, Customer Experience (CX), and Growth initiatives across the APJ region. It provides reusable technical demos and assets to showcase Databricks' capabilities in marketing analytics and customer data management.

## Problem Statement

There is an increasing demand from accounts and prospects (from startups to large enterprises) for Databricks solutions in marketing and CX domains. The challenges are:

1. **Lack of Technical Assets**: While pitch decks exist, there's a shortage of reusable, compelling demos and technical assets to support sales opportunities
2. **Industry Knowledge Gap**: Need for deeper understanding of marketing/CX/growth use-cases, particularly in APJ where Data-Marketing collaboration is emerging

## Solution Approach

This repository is part of a comprehensive initiative to:

1. Build a suite of demos showcasing Databricks capabilities and MarTech integrations
2. Create supporting documentation and solution accelerators
3. Foster regional/local partner collaboration
4. Maintain a centralized repository for field resources

## Key Use Cases

### Currently Implemented
- **Basic Customer 360** - Generate synthetic customer data for demo purposes
- **Data Generation** - Create realistic APJ-focused customer datasets

### Planned Use Cases
- Custom Identity Resolution with marketing tool sync
- Customer clustering/segmentation
- Multi-vertical Customer 360/MDM
- ML-enriched customer segments
- Partner-specific demos (Adobe Marketing Cloud, Braze, Amperity)
- A/B testing for campaigns
- GenAI use-cases (insights for marketing teams, content personalization)

## Project Structure

```
DI4Marketing/
├── DataSynthesis/          # Synthetic data generation module
├── .gitignore             # Git ignore configuration
└── README.md              # This file
```

## DataSynthesis Module

The DataSynthesis module generates realistic, anonymous customer data tailored for APJ markets, perfect for demonstrations and POCs.

### Features
- **10,000+ synthetic customer records** with APJ regional focus
- **Comprehensive customer attributes** including:
  - Demographics (age, location, income)
  - Behavioral data (engagement, purchase history)
  - Digital footprint (device, browser, UTM tracking)
  - Customer lifetime value metrics
- **Direct Databricks integration** for seamless data upload
- **Data validation** utilities

### Prerequisites

1. **Python 3.8+**
2. **Databricks workspace** with SQL warehouse access
3. **Environment variables** in `.env` file:
```bash
# Databricks Configuration
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
TOKEN=your-databricks-token
```

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd DI4Marketing

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install pandas numpy faker python-dotenv databricks-sql-connector
```

### Usage

#### Generate Synthetic Data

```python
from DataSynthesis.enhanced_anonymous_generator import EnhancedAnonymousGenerator

# Create generator instance
generator = EnhancedAnonymousGenerator(num_records=10000)

# Generate dataset
df = generator.generate_enhanced_dataset()

# Save to CSV
df.to_csv("customer_data.csv", index=False)
```

#### Upload to Databricks

```python
from DataSynthesis.enhanced_anonymous_generator import EnhancedDatabricksUploader

# Initialize uploader
uploader = EnhancedDatabricksUploader()

# Upload data to Databricks
success = uploader.upload_enhanced_data(df, "customer_360_demo")
```

#### Validate Data

```bash
# Test connection to Databricks
python DataSynthesis/test_connection.py

# Validate uploaded data
python DataSynthesis/validate_enhanced_data.py
```

### Generated Data Schema

The synthetic data includes:
- **Customer ID** - Unique identifier
- **Demographics** - Age, gender, location (APJ cities/countries)
- **Contact** - Email, phone (anonymized)
- **Behavioral** - Engagement score, lifetime value, churn risk
- **Digital** - Device type, browser, traffic source
- **Transactional** - Purchase history, order values
- **Timestamps** - Account creation, last activity

## Contributing

This project is designed to be modular. To add new capabilities:

1. Create a new module directory (e.g., `IdentityResolution/`, `Segmentation/`)
2. Include module-specific README with usage instructions
3. Follow existing patterns for Databricks integration
4. Add appropriate tests and validation scripts

## Future Modules

Planned modules include:
- **IdentityResolution** - Custom identity matching algorithms
- **Segmentation** - ML-based customer clustering
- **MarTechSync** - Integrations with marketing platforms
- **ABTesting** - Campaign testing framework
- **GenAIInsights** - LLM-powered marketing insights

## Support

For questions or contributions, please reach out to the Field Engineering team.

## License

Internal use only - Databricks Field Engineering
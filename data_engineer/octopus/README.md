# 🐙 Octopus Analysis 🐙

Octopus Analysis is a data analysis project designed to process and analyze Scopus data using Apache Spark.

## Table of Contents

- [Installation](#installation)
- [usage_and_configuration](#usage_and_configuration)
## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/punchanabu/octopus-analysis.git
   cd octopus-analysis
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Add data**
  Copy paste your `scopus` data folder into the `/data/` directory
   ```
      |-- data
      |   |-- scopus
      |   |   `-- 2023
      |   `-- scrape
   ```
4. **Optional ( If you want to develop on local)**
   ```bash
   docker compose up -d
   ```

## Usage_and_Configuration
1. copy the [.env.example](.env.example) to `.env` and update path of your scopus data
   ```
   DB_HOST="localhost"
   DB_PORT="9042"
   SCOPUS_DATA_PATH="/Users/punpun/Documents/Personal/cedt/dsde-project/octopus-analysis/data/scopus"
   SCOPUS_SCRAPE_PATH="/Users/punpun/Documents/Personal/cedt/dsde-project/octopus-analysis/data/scrape/"
   ```

2. **Run the Application**:
   ```bash
   python main.py
   ```
   This will read and stream data into your Cassandra database.

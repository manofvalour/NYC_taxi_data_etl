etl_pipeline_project/
│
├── config/                 # Configuration files (YAML, JSON, .env)
│   ├── config.yaml
│
│
├── logs/                   # Log files for debugging and monitoring
│
├── notebooks/              # Jupyter notebooks for exploration & prototyping
│

├── src/                    # Main source code
│   ├── extract/            # Data extraction modules
│   │   ├── api_extractor.py
│   │   └── scrapper.py
│   │
│   ├── transform/          # Data cleaning & transformation logic
│   │   ├── transformations.py
│   │
│   ├── load/               # Data loading modules
│   │   └── to_parquet.py
│   │
│   ├── utils/              # Helper functions (logging, config parsing, etc.)
│   │   └── common_utils.py
│   │
│   └── main.py              # Entry point for running the ETL pipeline
│
├── requirements.txt        # Python dependencies
├── README.md               # Project overview & setup instructions
└── .gitignore              # Ignore unnecessary files in version control

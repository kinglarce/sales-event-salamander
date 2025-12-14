# Sales Data Pipeline Salamander 🦎

A comprehensive data ingestion and processing system for Vivenu events and tickets, supporting both legacy (v1) and modern (v2) architectures.

## 🏗️ Project Structure

```
├── main.py                    # Main entry point (supports both v1 and v2)
├── README.md                  # This file
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # Docker configuration
├── Dockerfile                 # Main Docker image
├── Dockerfile.cron            # Cron job Docker image
│
├── v1/                        # Legacy system (v1)
│   ├── run_ingest.py         # Main v1 orchestration
│   ├── ingest_*.py           # v1 ingestion scripts
│   ├── ticket_analytics.py   # v1 analytics
│   └── ...                   # Other v1 scripts
│
├── v2/                        # Modern system (v2)
│   ├── run_ingest_v2.py      # Main v2 orchestration
│   ├── core/                 # Core modules
│   │   ├── config.py         # Configuration management
│   │   ├── logging.py        # Logging setup
│   │   ├── database.py       # Database management
│   │   ├── http_client.py    # HTTP client factory
│   │   ├── batch_processor.py # Batch processing
│   │   └── pipeline.py       # Pipeline management
│   ├── ingest_*_v2.py        # v2 ingestion scripts
│   ├── pipeline_configs/     # Pipeline configurations
│   ├── examples/             # Usage examples
│   └── README_V2.md          # v2 documentation
│
├── models/                    # Shared data models
│   ├── __init__.py
│   └── database.py
│
├── sql/                       # Shared SQL queries
│   ├── get_*.sql             # Query files
│   ├── setup_*.sql           # Setup scripts
│   └── upsert_*.sql          # Upsert scripts
│
├── utils/                     # Shared utilities
│   ├── __init__.py
│   ├── event_processor.py
│   ├── addon_processor.py
│   └── under_shop_processor.py
│
├── scripts/                   # Shared scripts
│
├── data/                      # Data files
├── data_static/              # Static configuration data
├── logs/                     # Log files
├── slack_bot/                # Slack bot integration
└── references/               # Reference implementations
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- PostgreSQL database (can be run in Docker)

## Configuration

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd vivenu-events-ticket-scrapper
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Build and start Docker containers**
   ```bash
   docker-compose up -d
   ```

### Running the System

#### Using Docker (Recommended)

```bash

docker-compose up --build -d app
docker-compose up --build -d app cron

# Run v1 system (default)
docker exec -it vivenu-app python main.py --version v1

# Run v2 system
docker exec -it vivenu-app python main.py --version v2

# Run v2 with custom pipeline
docker exec -it vivenu-app python main.py --version v2 --pipeline-config v2/pipeline_configs/custom.yaml

# Run v1 with specific script
docker exec -it vivenu-app python main.py --version v1 --script ingest_events_tickets

# Age group
docker exec -it vivenu-app python main.py --version v1 --script ingest_age_groups

# Reporting Excel
docker exec -it vivenu-app python main.py --version v1 --script ingest_age_groups

#other
# Run for Reporting Registration Data to Slack
docker exec -it vivenu-app python ticket_analytics.py

# Run for Reporting only for sending Specatator sales to Slack
docker exec -it vivenu-app python main.py --version v1 && docker exec -it vivenu-app python main.py --version v1 --script spectator_analytics.py

# Run for Reporting Excel data and sending to Slack
docker exec -it vivenu-app python v1/reporting_analytics.py --excel
docker exec -it vivenu-app python reporting_analytics.py --slack --excel

# Run for Reporting for Cou[ons] Excel data and sending to Slack
docker exec -it vivenu-app python ingest_coupons.py
# Full report with Slack notification
docker exec -it vivenu-app python coupon_analytics.py --slack
# Console summary only
docker exec -it vivenu-app python coupon_analytics.py --summary
# Excel with custom path and Slack
docker exec -it vivenu-app python coupon_analytics.py --excel my_report.xlsx
```

#### Using Docker Compose

```bash
# Run v1 system
docker-compose exec app python main.py --version v1

# Run v2 system
docker-compose exec app python main.py --version v2

# Run with debug logging
docker-compose exec app python main.py --version v2 --debug
```

#### Direct Docker Execution

```bash
# Build image
docker build -t vivenu-scrapper .

# Run v1 system
docker run -v $(pwd)/.env:/app/.env vivenu-scrapper python main.py --version v1

# Run v2 system
docker run -v $(pwd)/.env:/app/.env vivenu-scrapper python main.py --version v2
```

#### Local Development (Optional)

If you want to run locally without Docker:

```bash
# Install dependencies
pip install -r requirements.txt

# Run v1 system
python main.py --version v1

# Run v2 system
python main.py --version v2
```

## 📊 System Versions

### V1 System (Legacy)
- **Purpose**: Original implementation
- **Architecture**: Monolithic scripts
- **Features**: Basic ingestion and processing
- **Use Case**: Legacy support, simple workflows

**Key Scripts:**
- `v1/run_ingest.py` - Main orchestration
- `v1/ingest_events_tickets.py` - Events and tickets
- `v1/ingest_coupons.py` - Coupon processing
- `v1/ticket_analytics.py` - Analytics

### V2 System (Modern)
- **Purpose**: Refactored with best practices
- **Architecture**: Modular, configurable
- **Features**: Pipeline system, parallel execution, monitoring
- **Use Case**: Production workloads, complex workflows

**Key Components:**
- `v2/core/` - Core modules
- `v2/run_ingest_v2.py` - Main orchestration
- `v2/pipeline_configs/` - Pipeline configurations
- `v2/examples/` - Usage examples

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database
DATABASE_URL=postgresql://user:password@host:port/database

# API Configuration
VIVENU_API_TOKEN=your_token_here
EVENT_API_BASE_URL=https://api.vivenu.com

# Logging
LOG_LEVEL=INFO
ENABLE_FILE_LOGGING=true
DEBUG_MODE=false

# HTTP Client (v2)
HTTPX_TIMEOUT_CONNECT=10.0
HTTPX_TIMEOUT_READ=30.0
HTTPX_MAX_CONNECTIONS=100
HTTPX_RETRIES=3
HTTPX_HTTP2=false
HTTPX_VERIFY_SSL=false

# Event Configurations
EVENT_CONFIGS__australia__token=token_australia
EVENT_CONFIGS__australia__event_id=event_id_australia
EVENT_CONFIGS__australia__schema_name=australia
```

### V2 Pipeline Configuration

Create custom pipeline configurations in YAML or JSON:

```yaml
# v2/pipeline_configs/custom.yaml
name: "custom_ingestion"
description: "Custom ingestion pipeline"
parallel_execution: true
max_parallel_steps: 3
stop_on_failure: true

steps:
  - name: "static_data_ingestion"
    function: "static_data_ingestion"
    enabled: true
    timeout: 300.0
    depends_on: []
    
  - name: "events_tickets_ingestion"
    function: "events_tickets_ingestion"
    enabled: true
    timeout: 1800.0
    depends_on: ["static_data_ingestion"]
```

## 🐳 Docker Usage

### Quick Start with Docker

```bash
# 1. Start the application container
docker-compose up -d
docker-compose up --build -d app
docker-compose up --build -d app cron

# 2. Run v1 system
docker exec -it vivenu-app python main.py --version v1

# 3. Run v2 system
docker exec -it vivenu-app python main.py --version v2
```

### Common Docker Commands

```bash
# Start containers
docker-compose up -d

# Stop containers
docker-compose down

# View logs
docker-compose logs -f app

# Access container shell
docker exec -it vivenu-app bash

# Run specific scripts
docker exec -it vivenu-app python v1/run_ingest.py
docker exec -it vivenu-app python v2/run_ingest_v2.py

# Run with debug logging
docker exec -it vivenu-app python main.py --version v2 --debug

# Run with custom pipeline
docker exec -it vivenu-app python main.py --version v2 --pipeline-config v2/pipeline_configs/custom.yaml
```

### Docker Compose Services

```bash
# Run v1 system
docker-compose exec app python main.py --version v1

# Run v2 system
docker-compose exec app python main.py --version v2

# Run with environment variables
docker-compose exec -e DEBUG_MODE=true app python main.py --version v2 --debug
```

### Building Custom Images

```bash
# Build main image
docker build -t vivenu-scrapper .

# Build cron image
docker build -f Dockerfile.cron -t vivenu-scrapper-cron .

# Run with custom image
docker run -v $(pwd)/.env:/app/.env vivenu-scrapper python main.py --version v1
```

### Cron Job Setup

For scheduled execution using Docker:

```bash
# Build cron image
docker build -f Dockerfile.cron -t vivenu-scrapper-cron .

# Run cron job (v1 system)
docker run --rm -v $(pwd)/.env:/app/.env vivenu-scrapper-cron

# Run cron job (v2 system)
docker run --rm -v $(pwd)/.env:/app/.env -e VERSION=v2 vivenu-scrapper-cron

# Run with custom pipeline
docker run --rm -v $(pwd)/.env:/app/.env -e VERSION=v2 -e PIPELINE_CONFIG=v2/pipeline_configs/custom.yaml vivenu-scrapper-cron
```

### Docker Environment Variables

```bash
# Set version
docker exec -it vivenu-app -e VERSION=v2 python main.py

# Set debug mode
docker exec -it vivenu-app -e DEBUG_MODE=true python main.py --version v2 --debug

# Set custom pipeline
docker exec -it vivenu-app -e PIPELINE_CONFIG=v2/pipeline_configs/custom.yaml python main.py --version v2
```

### Docker Cron Job Examples

```bash
# Run v1 system via cron
docker exec -it vivenu-app python main.py --version v1

# Run v2 system via cron
docker exec -it vivenu-app python main.py --version v2

# Run v2 with debug logging
docker exec -it vivenu-app python main.py --version v2 --debug

# Run v2 with custom pipeline
docker exec -it vivenu-app python main.py --version v2 --pipeline-config v2/pipeline_configs/custom.yaml

# Run v2 with predefined pipeline
docker exec -it vivenu-app python main.py --version v2 --pipeline-name minimal
```

### Docker Compose with Environment Variables

```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: .
    environment:
      - VERSION=v2
      - DEBUG_MODE=false
      - PIPELINE_CONFIG=v2/pipeline_configs/default.yaml
    volumes:
      - ./.env:/app/.env
      - ./logs:/app/logs
```

```bash
# Run with environment variables
docker-compose exec app python main.py
```

## 📈 Monitoring and Logging

### Log Files
- **Location**: `logs/` directory
- **Format**: `{script_name}_{timestamp}.log`
- **Levels**: DEBUG, INFO, WARNING, ERROR

### V2 Performance Monitoring
- Built-in performance tracking
- Step execution times
- Resource usage monitoring
- Progress tracking

## 🧪 Testing

### Unit Tests
```bash
# Run v1 tests
python -m pytest v1/tests/

# Run v2 tests
python -m pytest v2/tests/
```

### Integration Tests
```bash
# Test v1 system
python v1/run_ingest.py --test

# Test v2 system
python v2/run_ingest_v2.py --test
```

## 🔄 Migration Guide

### From V1 to V2

1. **Update imports**:
   ```python
   # Old
   from ingest_events_tickets import main_ingest_events_tickets
   
   # New
   from v2.ingest_events_tickets_v2 import main_ingest_events_tickets
   ```

2. **Update configuration**:
   ```python
   # Old
   from dotenv import load_dotenv
   load_dotenv()
   
   # New
   from v2.core import get_config
   config = get_config()
   ```

3. **Update pipeline**:
   ```python
   # Old
   scripts = ['ingest_static_data.py', 'ingest_events_tickets.py']
   
   # New
   from v2.core.pipeline import PipelineBuilder
   pipeline = PipelineBuilder("custom", "Custom pipeline")
   pipeline.add_step("static_data", "static_data_ingestion")
   pipeline.add_step_with_dependencies("events_tickets", "events_tickets_ingestion", ["static_data"])
   ```

## 🛠️ Development

### Adding New Scripts

**V1 System:**
1. Create script in `v1/` directory
2. Add to `v1/run_ingest.py` if needed
3. Update documentation

**V2 System:**
1. Create script in `v2/` directory
2. Add to pipeline configuration
3. Update core modules if needed
4. Add tests

### Shared Components

- **`models/`**: Data models and database schemas
- **`sql/`**: SQL queries and scripts
- **`utils/`**: Utility functions and processors
- **`scripts/`**: Shared scripts

## 📚 Documentation

- **Main README**: This file
- **V2 Documentation**: `v2/README_V2.md`
- **Architecture**: `v2/ARCHITECTURE.md`
- **Examples**: `v2/examples/`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 🆘 Support

For issues and questions:

1. Check the logs for error details
2. Review the documentation
3. Test with minimal configuration
4. Create an issue with details

## 🔮 Roadmap

### V2 Enhancements
- Web UI for pipeline configuration
- Real-time monitoring dashboard
- Advanced scheduling
- Auto-scaling capabilities
- Enhanced error recovery

### V1 Maintenance
- Bug fixes
- Security updates
- Compatibility improvements
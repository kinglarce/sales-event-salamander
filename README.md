# Sales Data Pipeline Salamander 🦎

A robust data pipeline for fetching and processing events and ticket data, built with Python, PostgreSQL, and Docker.

## Project Structure
```
pipeline/
├── models/
│   └── database.py      # SQLAlchemy models
├── data_static/ 
│   └── schemas/         # SQL schema for static data
│       └── region1.json
├── sql/
│   ├── get_current_summary.sql
│   └── get_ticket_counts.sql
├── .env.example
├── Dockerfile
├── Dockerfile.cron
├── docker-compose.yml
├── ingest.py            # Main ingestion script
├── requirements.txt
└── ticket_analytics.py  # Analytics processing
```

## Features

- Asynchronous API data fetching with rate limiting
- Concurrent processing of multiple regions
- Automatic schema and table creation
- Upsert logic for data updates
- Ticket type summary generation
- Category capacity tracking
- Docker containerization
- Environment-based configuration
- Database visualization with pgweb
- Automated data ingestion via cron jobs
- Slack notification for ticket sales report
- Static data for event capacity and ticket capacity

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- PostgreSQL 15+

## Configuration

1. Create a `.env` file based on `.env.example`:

```env
# Database settings
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=yourdb

# Slack config
SLACK_API_TOKEN=xoxb-XXXX

# Ticket analytics config
PROJECTION_MINUTES=3
HISTORY_MINUTES=15
ENABLE_GROWTH_ANALYSIS=false
ENABLE_PROJECTIONS=false
ENABLE_FILE_LOGGING=true

# API config
EVENT_API_BASE_URL=

# Event configurations
EVENT_CONFIGS__region1__token=your_api_token_1
EVENT_CONFIGS__region1__event_id=your_event_id_1
EVENT_CONFIGS__region1__schema_name=region1
EVENT_CONFIGS__region1__max_capacity=5000
EVENT_CONFIGS__region1__start_wave=50
EVENT_CONFIGS__region1__price_tier=L1
EVENT_CONFIGS__region1__REPORTING_CHANNEL=region1-channel

EVENT_CONFIGS__region2__token=your_api_token_2
EVENT_CONFIGS__region2__event_id=your_event_id_2
EVENT_CONFIGS__region2__schema_name=region2
EVENT_CONFIGS__region2__max_capacity=4000
EVENT_CONFIGS__region2__start_wave=40
EVENT_CONFIGS__region2__price_tier=L2
EVENT_CONFIGS__region2__REGISTRATION_CHANNEL=region2-channel
EVENT_CONFIGS__region2__REPORTING_CHANNEL=region2-channel
EVENT_CONFIGS__region2__summary_breakdown_day=true
EVENT_CONFIGS__region2__field_gender=custom_local_gender_identified

EVENT_CONFIGS__region3__token=your_api_token_3
EVENT_CONFIGS__region3__event_id=your_event_id_3
EVENT_CONFIGS__region3__schema_name=region3_city
EVENT_CONFIGS__region3__max_capacity=5000
EVENT_CONFIGS__region3__start_wave=55
EVENT_CONFIGS__region3__price_tier=L1
EVENT_CONFIGS__region3__REGISTRATION_CHANNEL=region3-channel
EVENT_CONFIGS__region3__REPORTING_CHANNEL=region3-channel
EVENT_CONFIGS__region3__summary_breakdown_day=true
EVENT_CONFIGS__region3__exclude_adaptive_sunday=true
EVENT_CONFIGS__region3__field_gender=custom_local_gender_identified
```

## Installation & Usage

### Using Docker (Recommended)

1. Build and start all containers:
```bash
docker-compose up --build
```

2. Run in detached mode:
```bash
docker-compose up -d
```


3. View logs:
```bash
# View all container logs
docker-compose logs -f

# View specific container logs
docker-compose logs -f app
docker-compose logs -f cron
```

4. Stop containers:
```bash
docker-compose down
```
### Automatic Ingestion, Ticket Analytics and Slack Notification
```bash
# Re-build app for Environment changes
docker-compose up --build -d app
docker-compose up --build -d app cron

# Run for Ingesting Event & Tickets Data 
docker exec -it vivenu-app python scripts/run_ingest.py

# Run for Ingesting Age Group Data
docker exec -it vivenu-app python ingest_age_groups.py

# Run for Ingesting Static Data
docker exec -it vivenu-app python ingest_static_data.py

# Run for Reporting Registration Data to Slack
docker exec -it vivenu-app python ticket_analytics.py

# Run for Reporting only for sending Specatator sales to Slack
docker exec -it vivenu-app python scripts/run_ingest.py && docker exec -it vivenu-app python spectator_analytics.py

# Run for Reporting Excel data and sending to Slack
docker exec -it vivenu-app python reporting_analytics.py --excel
docker exec -it vivenu-app python reporting_analytics.py --slack --excel
```


### Manual Data Ingestion

Run the ingest script manually:
```bash
# Regular ingestion
python ingest.py

# With debug logging
python ingest.py --debug

# Skip API fetch (only update summaries)
python ingest.py --skip_fetch
```

### Running Analytics

Run the ticket analytics script:
```bash
python ticket_analytics.py
```

## Undershops

- **PARTNERACCESS**: Use this prefix for under shops that are part of the partner access program.
- **HTCACCESS**: Use this prefix for under shops associated with HTC access.

## Cron Jobs

The pipeline includes automated data ingestion and analytics via cron jobs. The schedule is configured in `Dockerfile.cron`:

```dockerfile
# Default schedule (every 5 minutes)
*/5 * * * * cd /app && python ingest.py >> /app/logs/cron.log 2>&1

# Analytics schedule (every 15 minutes)
*/15 * * * * cd /app && python ticket_analytics.py >> /app/logs/analytics.log 2>&1
```

To modify the schedule:
1. Edit `Dockerfile.cron`
2. Rebuild the cron container:
```bash
docker-compose up -d --build app cron
```

## Database Schema

The pipeline creates the following tables for each configured region:

1. `events` - Stores event information
   - id (PK)
   - name
   - location_name
   - start_date
   - end_date
   - timezone

2. `tickets` - Stores ticket information
   - id (PK)
   - event_id (FK)
   - ticket_type_id
   - status
   - created_at
   - customer_info

3. `ticket_summary` - Summarizes ticket counts by type
   - id (PK)
   - event_id (FK)
   - ticket_type_id
   - total_count
   - updated_at

4. `summary_report` - Stores historical ticket count data
   - id (PK)
   - event_id
   - ticket_group
   - total_count
   - created_at

5. `ticket_under_shops` - Stores under shop information
   - id (PK)
   - shop_name
   - shop_category
   - created_at
   - updated_at

### Accessing pgweb

Once the containers are running, access the pgweb interface at:
```
http://localhost:8081
```

## Troubleshooting

1. Check container status:
```bash
docker-compose ps
```

2. View container logs:
```bash
docker-compose logs -f [service_name]
```

3. Access container shell:
```bash
docker-compose exec [service_name] bash
```

4. Reset database:
```bash
docker-compose down -v
docker-compose up -d
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

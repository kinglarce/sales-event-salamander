# Vivenu Data Pipeline

A robust data pipeline for fetching and processing Vivenu event and ticket data, built with Python, PostgreSQL, and Docker.

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
- Real-time Slack notifications

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
POSTGRES_DB=vivenu_db

# Slack config
SLACK_API_TOKEN=xoxb-your-slack-token
SLACK_CHANNEL=your-channel-name

# Ticket analytics config
PROJECTION_MINUTES=3
HISTORY_MINUTES=15

# API config
EVENT_API_BASE_URL=https://vivenu.com/api

# Event configurations
EVENT_CONFIGS__region1__token=your_api_token_1
EVENT_CONFIGS__region1__event_id=your_event_id_1
EVENT_CONFIGS__region1__schema_name=region1

EVENT_CONFIGS__region2__token=your_api_token_2
EVENT_CONFIGS__region2__event_id=your_event_id_2
EVENT_CONFIGS__region2__schema_name=region2
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
docker-compose up -d --build cron
```

## Database Schema

The pipeline creates the following tables for each configured region:

1. `events` - Stores event information
2. `tickets` - Stores ticket information
3. `ticket_type_summary` - Summarizes ticket counts by type
4. `summary_report` - Stores historical ticket count data

## Accessing pgweb

Once the containers are running, access the pgweb interface at:
```
http://localhost:8081
```

pgweb provides:
- Database table browsing
- SQL query execution
- Data export
- Table relationship visualization
- Database statistics monitoring

## Slack Notifications

The pipeline sends automated reports to Slack including:
- Current ticket counts
- Detailed breakdowns by ticket type
- Growth analysis
- Sales projections

Configure Slack notifications in `.env`:
```env
SLACK_API_TOKEN=your-token
SLACK_CHANNEL=your-channel
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

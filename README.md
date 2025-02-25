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

## Project Structure
├── docker/
│   └── dump/
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── api_client.py
│   │   ├── database.py
│   │   └── models.py
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── events.py
│   │   └── tickets.py
│   └── main.py
├── Dockerfile
├── docker-compose.yml
├── .env
├── .env.example
├── requirements.txt
└── README.md

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- PostgreSQL 15+

## Configuration

1. Create a `.env` file based on `.env.example`:
```
# Database settings
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_DB=

# Region configurations
REGION_CONFIGStaiwan_token=your_taiwan_token
REGION_CONFIGStaiwan_event_id=your_taiwan_event_id
REGION_CONFIGSaustralia_token=your_australia_token
REGION_CONFIGSaustralia_event_id=your_australia_event_id
```

## Database Schema

The pipeline creates the following tables:

1. `events` - Stores event information
2. `tickets` - Stores ticket information
3. `ticket_type_summary` - Summarizes ticket counts by type
4. `category_capacities` - Tracks capacity limits and current counts

## Installation & Usage

### Using Docker (Recommended)

1. Build and start the containers:
```
docker-compose up --build
```
2. To run in detached mode:
```
docker-compose up -d
```
3. View logs:
```
docker-compose logs -f app
```

### Accessing pgweb

Once the containers are running, you can access the pgweb interface at:
```
http://localhost:8081
```

pgweb provides a web-based interface to:
- Browse database tables and schemas
- Execute SQL queries
- Export data
- View table relationships
- Monitor database statistics

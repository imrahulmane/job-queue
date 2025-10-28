
# Job-Queue

A PostgreSQL-based job queue with concurrent worker processing. Uses `FOR UPDATE SKIP LOCKED` for atomic job claiming without external message brokers.

## Quick Start

### 1. Setup Environment

```bash
cd docker/dev
cp .env.example .env
# Edit .env with your values
```

### 2. Start Services

```bash
docker-compose up -d
```

Services:
- API: http://localhost:8001
- API Docs: http://localhost:8001/docs
- PostgreSQL: localhost:5432

### 3. Create Database Tables

```bash
# Run table creation script
docker-compose exec api python -m app.db.create_tables

# Or if API container is not running, use worker container
docker-compose exec worker python -m app.db.create_tables
```

You should see:
```
Creating tables...
Tables created successfully!
```

### 4. Verify Setup

```bash
# Check if services are running
docker-compose ps

# Test API
curl http://localhost:8001/api/v1/queue/jobs/stats/overview
```

## Usage

### Create a Job

```bash
curl -X POST http://localhost:8001/api/v1/queue/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "emails",
    "payload": {"to": "user@example.com", "subject": "Test"},
    "queue_name": "default",
    "max_tries": 3
  }'
```

### Check Job Status

View all endpoints: http://localhost:8001/docs

```bash
# Get statistics
curl http://localhost:8001/api/v1/queue/jobs/stats/overview

# List jobs
curl http://localhost:8001/api/v1/queue/jobs?status=pending

# Get specific job
curl http://localhost:8001/api/v1/queue/jobs/{job_id}
```

### Monitor Workers

```bash
docker-compose logs -f worker
```

## Scaling

### Add More Workers

```bash
docker-compose up -d --scale worker=3
```

### Adjust Concurrency

Edit `docker-compose.yml`:

```yaml
worker:
  environment:
    MAX_CONCURRENT_JOBS: 5  # Jobs per worker
```

### Connection Limits

Each worker uses up to 15 database connections:
- `pool_size: 5` (persistent)
- `max_overflow: 10` (temporary)

Safe worker count: `(PostgreSQL max_connections - 10) / 15`

With default PostgreSQL (100 connections): ~6 workers maximum

## Job Types

Available handlers:
- `emails` - Email sending
- `report_generation` - Report generation
- `images_processing` - Image processing

Add new handlers in `app/handlers/`

## Architecture

- **FastAPI** - REST API
- **PostgreSQL 15** - Database and queue
- **SQLAlchemy (async)** - ORM
- **Docker Compose** - Container orchestration

### Key Design Decisions

1. **PostgreSQL instead of Redis**: Simpler infrastructure, ACID guarantees, suitable for moderate workloads
2. **Asyncio concurrency**: Perfect for I/O-bound jobs, lower overhead than threads
3. **Immediate commits**: Prevents race conditions when multiple workers claim jobs
4. **Unique worker IDs**: Uses container hostname for automatic identification

## Environment Variables

```bash
# Worker
MAX_CONCURRENT_JOBS=3
WORKER_QUEUES=default
POLL_INTERVAL=1.0

# Database
DB_URL=job_user:job_password@postgres:5432/job_queue

# Application
DEBUG=true
```

## Project Structure

```
app/
├── api/v1/endpoints/tasks.py    # REST endpoints
├── handlers/                    # Job handlers
├── repositories/                # Database queries
├── worker/worker.py             # Worker implementation
├── main.py                      # API entry point
└── worker_main.py               # Worker entry point
```

## Troubleshooting

**"Too many connections"**
- Reduce number of workers or connections per worker

**Jobs not processing**
- Check worker logs: `docker-compose logs worker`
- Verify database is running: `docker-compose ps`

**Same job claimed twice**
- Ensure commit happens immediately after job claim

## Testing

```bash
# Send 10 jobs
for i in {1..10}; do
  curl -X POST http://localhost:8001/api/v1/queue/jobs \
    -H "Content-Type: application/json" \
    -d "{\"job_type\":\"emails\",\"payload\":{\"to\":\"test$i@example.com\"},\"queue_name\":\"default\",\"max_tries\":3}" &
done

# Watch processing
docker-compose logs -f worker
```

## License

MIT
=======
>>>>>>> 825be33700cc70b309c1b55d811e254e25a82e33

# Shadow Database API

HTTP API server for semantic search in PostgreSQL shadow databases with 768D embeddings. Designed for OpenWebUI integration.

## Quick Deploy

```bash
git clone <this-repo>
cd shadow-database-api
docker-compose up -d
```

## Environment Variables

```env
PG_HOST=postgres-host
PG_PORT=5432
PG_USER=nexus
PG_PASSWORD=your-password
NODE_ENV=production
PORT=8000
```

## API Endpoints

- `GET /health` - Health check
- `GET /openapi.json` - OpenAPI specification
- `POST /search` - Semantic search
- `POST /list-tables` - List available tables

## Integration with PHASE 2 Automation

This repository is automatically cloned and deployed by the MegaCity PHASE 2 automation system.

## Production Deployment

The service runs with tsx for TypeScript execution:
```bash
npx tsx shadow-db-mcp-server.ts
```
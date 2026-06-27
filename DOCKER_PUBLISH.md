# Docker publish

## Local run

1. Optionally copy the Docker env template:

```bash
cp .env.docker.example .env
```

The compose file loads `.env.docker.example` by default and overrides it with `.env` when that file exists.

2. Edit `.env` and set at least:

```bash
AUTHENTICATION_API_KEY=your-api-key
SERVER_URL=https://your-domain.example
CHATWOOT_IMPORT_DATABASE_CONNECTION_URI=postgresql://user:pass@chatwoot-postgres:5432/chatwoot
```

3. Build and run:

```bash
docker compose -f docker-compose.publish.yaml up -d --build
```

The API will be available on port `8080`; the embedded manager is served at `/manager`.

## Manual image publish

The GitHub token currently available to this workspace does not include the `workflow` scope, so this import does not include GitHub Actions workflows.

To publish manually to GitHub Container Registry:

```bash
docker build -t ghcr.io/guirtvidal/evolution-api-chatwoot-sync:latest .
docker login ghcr.io
docker push ghcr.io/guirtvidal/evolution-api-chatwoot-sync:latest
```

Image name:

```text
ghcr.io/guirtvidal/evolution-api-chatwoot-sync
```

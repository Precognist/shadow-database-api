FROM node:18-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package.json .

# Install npm dependencies
RUN npm install

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000/health', (r) => {if (r.statusCode !== 200) throw new Error(r.statusCode)})" || exit 1

# Start MCP server
CMD ["npx", "tsx", "shadow-db-mcp-server.ts"]

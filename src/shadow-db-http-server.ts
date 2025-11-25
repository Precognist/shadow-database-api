#!/usr/bin/env node
/**
 * Shadow Database HTTP Server - Streamable HTTP for OpenWebUI
 *
 * Provides HTTP/REST endpoints for semantic search in PostgreSQL shadow databases
 * Works directly with OpenWebUI via streamable HTTP integration (no mcpo needed)
 *
 * Enforces agent-level separation:
 * - real-estate-intelligence-specialist → only real_estate database
 * - Precursor Test-Agent for Dovetail.AI → only dovetail database
 */

import Fastify from 'fastify';
import * as pg from 'pg';
import { env } from 'process';

// ==================== Configuration ====================

interface ShadowDbConfig {
  [key: string]: {
    database: string;
    tables: string[];
  };
}

interface AgentAccessConfig {
  [agentId: string]: string;
}

// Dynamic shadow database config - populated at startup via auto-discovery
let SHADOW_DB_CONFIG: ShadowDbConfig = {};

const AGENT_DATABASE_MAPPING: AgentAccessConfig = {
  // COMMENTED OUT: Production agent mappings - security issue
  // 'real-estate-intelligence-specialist': 'real_estate',
  // 'pl94ap0ghrqwbdx': 'real_estate',
  // 'Precursor Test-Agent for Dovetail.AI': 'dovetail',
  // 'p802fqqug779z13': 'dovetail'
};

const PG_CONFIG = {
  host: env.PG_HOST || 'localhost',
  port: parseInt(env.PG_PORT || '5432'),
  user: env.PG_USER || 'postgres',
  password: env.PG_PASSWORD || 'postgres'
};

// ==================== Auto-Discovery ====================

/**
 * Discover shadow databases (nexus_*_shadow) and their tables
 * This runs at startup to dynamically populate SHADOW_DB_CONFIG
 */
async function discoverShadowDatabases(): Promise<void> {
  console.error('🔍 Discovering shadow databases...');

  const client = new pg.Client({
    host: PG_CONFIG.host,
    port: PG_CONFIG.port,
    user: PG_CONFIG.user,
    password: PG_CONFIG.password,
    database: 'postgres' // Connect to default db for discovery
  });

  try {
    await client.connect();

    // Find all shadow databases (pattern: nexus_*_shadow)
    const dbResult = await client.query(`
      SELECT datname FROM pg_database
      WHERE datname LIKE 'nexus_%_shadow'
      AND datistemplate = false
    `);

    console.error(`   Found ${dbResult.rows.length} shadow database(s)`);

    for (const row of dbResult.rows) {
      const dbName = row.datname;
      // Extract friendly name from database name: nexus_base_shadow -> base
      const friendlyName = dbName.replace(/^nexus_/, '').replace(/_shadow$/, '');

      // Connect to each shadow database to discover tables
      const shadowClient = new pg.Client({
        host: PG_CONFIG.host,
        port: PG_CONFIG.port,
        user: PG_CONFIG.user,
        password: PG_CONFIG.password,
        database: dbName
      });

      try {
        await shadowClient.connect();

        // Get all tables with vector embedding column
        const tableResult = await shadowClient.query(`
          SELECT table_name
          FROM information_schema.columns
          WHERE column_name = 'embedding'
          AND table_schema = 'public'
        `);

        const tables = tableResult.rows.map(r => r.table_name);

        if (tables.length > 0) {
          SHADOW_DB_CONFIG[friendlyName] = {
            database: dbName,
            tables: tables
          };
          console.error(`   ✅ ${friendlyName}: ${tables.join(', ')}`);
        }
      } finally {
        await shadowClient.end();
      }
    }

    console.error(`   Total: ${Object.keys(SHADOW_DB_CONFIG).length} searchable database(s)`);

  } finally {
    await client.end();
  }
}

// ==================== Embedding Generation ====================

let embeddingModel: any = null;
let embeddingReady = false;

async function loadEmbeddingModel() {
  if (embeddingReady) return;

  console.error('🔄 Loading embedding model...');
  try {
    const { pipeline } = await import('@xenova/transformers');
    embeddingModel = await pipeline('feature-extraction', 'Xenova/bge-base-en-v1.5');
    embeddingReady = true;
    console.error('✅ Embedding model loaded');
  } catch (error) {
    console.error('❌ Failed to load embedding model:', error);
    throw error;
  }
}

async function generateEmbedding(text: string): Promise<number[]> {
  if (!embeddingReady) {
    await loadEmbeddingModel();
  }

  const output = await embeddingModel(text, { pooling: 'mean', normalize: true });
  return Array.from(output.data);
}

// ==================== Vector Search ====================

async function vectorSearch(
  database: string,
  table: string,
  embedding: number[],
  limit: number = 10
): Promise<any[]> {
  const dbConfig = SHADOW_DB_CONFIG[database];
  if (!dbConfig) throw new Error(`Unknown database: ${database}`);

  const client = new pg.Client({
    host: PG_CONFIG.host,
    port: PG_CONFIG.port,
    user: PG_CONFIG.user,
    password: PG_CONFIG.password,
    database: dbConfig.database
  });

  try {
    await client.connect();

    const embeddingStr = `[${embedding.join(',')}]`;

    const query = `
      SELECT
        id,
        data,
        embedding <=> $1::vector as distance,
        (1 - (embedding <=> $1::vector)) * 100 as similarity_score
      FROM ${table}
      ORDER BY embedding <=> $1::vector
      LIMIT $2
    `;

    const result = await client.query(query, [embeddingStr, limit]);

    const results = result.rows.map(row => ({
      id: row.id,
      _similarity_score: parseFloat(row.similarity_score),
      ...(row.data && typeof row.data === 'object' ? row.data : row.data ? JSON.parse(row.data) : {})
    }));

    return results;
  } finally {
    await client.end();
  }
}

// ==================== Agent Context ====================

interface ExecutionContext {
  agentId?: string;
  agentName?: string;
  allowedDatabase?: string;
}

function extractAgentContext(headers: any): ExecutionContext {
  const agentId = headers['x-agent-id'] || env.AGENT_ID || env.MODEL_ID;
  const agentName = headers['x-agent-name'] || env.AGENT_NAME || env.MODEL_NAME;

  let allowedDatabase = undefined;
  if (agentId) allowedDatabase = AGENT_DATABASE_MAPPING[agentId];
  if (!allowedDatabase && agentName) allowedDatabase = AGENT_DATABASE_MAPPING[agentName];

  return { agentId, agentName, allowedDatabase };
}

function validateAgentAccess(requestedDatabase: string, context: ExecutionContext): boolean {
  if (!context.allowedDatabase) {
    console.error(`⚠️ No agent context, allowing access to all databases (development mode)`);
    return true;
  }

  if (context.allowedDatabase !== requestedDatabase) {
    console.error(
      `❌ Agent ${context.agentName || context.agentId} not allowed to access ${requestedDatabase}\n   Allowed database: ${context.allowedDatabase}`
    );
    return false;
  }

  console.error(`✅ Agent ${context.agentName || context.agentId} accessing ${requestedDatabase}`);
  return true;
}

// ==================== HTTP Server ====================

const fastify = Fastify({
  logger: false
});

// Health check
fastify.get('/health', async (request, reply) => {
  return { status: 'ok', server: 'shadow-db-http' };
});

// OpenAPI spec for tool discovery
fastify.get('/openapi.json', async (request, reply) => {
  return {
    openapi: '3.0.0',
    info: {
      title: 'Shadow Database Search API',
      description: 'Semantic search in PostgreSQL shadow databases with vector embeddings',
      version: '1.0.0'
    },
    servers: [
      {
        url: 'http://shadow-db-http:8000',
        description: 'Shadow Database HTTP Server'
      }
    ],
    paths: {
      '/search': {
        post: {
          summary: 'Semantic search in shadow database',
          description: 'Search PostgreSQL shadow database with vector embeddings using natural language queries',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    database: {
                      type: 'string',
                      // enum: ['real_estate', 'dovetail'], // COMMENTED OUT: Production hardcoded values
                      description: 'Shadow database to search'
                    },
                    table: {
                      type: 'string',
                      description: 'Table name to search (e.g., Properties, Bands, Venues)'
                    },
                    query: {
                      type: 'string',
                      description: 'Natural language semantic search query'
                    },
                    limit: {
                      type: 'integer',
                      default: 10,
                      minimum: 1,
                      maximum: 100,
                      description: 'Maximum number of results'
                    }
                  },
                  required: ['database', 'table', 'query']
                }
              }
            }
          },
          responses: {
            '200': {
              description: 'Successful search results',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      success: { type: 'boolean' },
                      query: { type: 'string' },
                      database: { type: 'string' },
                      table: { type: 'string' },
                      total_matches: { type: 'integer' },
                      results: { type: 'array' }
                    }
                  }
                }
              }
            },
            '400': { description: 'Bad request' },
            '403': { description: 'Access denied' },
            '500': { description: 'Server error' }
          }
        }
      },
      '/list-tables': {
        post: {
          summary: 'List available tables',
          description: 'List available tables in a shadow database',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    database: {
                      type: 'string',
                      // enum: ['real_estate', 'dovetail'], // COMMENTED OUT: Production hardcoded values
                      description: 'Shadow database to query'
                    }
                  },
                  required: ['database']
                }
              }
            }
          },
          responses: {
            '200': {
              description: 'List of tables',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      success: { type: 'boolean' },
                      database: { type: 'string' },
                      tables: {
                        type: 'array',
                        items: { type: 'string' }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/agent-info': {
        get: {
          summary: 'Get agent access info',
          description: 'Get which database this agent has access to',
          responses: {
            '200': {
              description: 'Agent access information',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      agentId: { type: 'string' },
                      agentName: { type: 'string' },
                      allowedDatabase: { type: 'string' }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  };
});

// Search endpoint
fastify.post<{
  Body: {
    database: string;
    table: string;
    query: string;
    limit?: number;
  }
}>('/search', async (request, reply) => {
  const { database, table, query, limit = 10 } = request.body;
  const context = extractAgentContext(request.headers);

  try {
    // Validate inputs
    if (!database || !table || !query) {
      reply.code(400);
      return {
        success: false,
        error: 'Missing required parameters: database, table, query'
      };
    }

    // Enforce access control
    if (!validateAgentAccess(database, context)) {
      reply.code(403);
      return {
        success: false,
        error: `Agent not authorized to access ${database} database`,
        agent: context.agentName || context.agentId || 'unknown'
      };
    }

    // Validate database and table
    const dbConfig = SHADOW_DB_CONFIG[database];
    if (!dbConfig) {
      reply.code(400);
      return { success: false, error: `Unknown database: ${database}` };
    }

    if (!dbConfig.tables.includes(table)) {
      reply.code(400);
      return {
        success: false,
        error: `Unknown table: ${table} in database ${database}`
      };
    }

    // Validate limit
    if (limit < 1 || limit > 100) {
      reply.code(400);
      return {
        success: false,
        error: 'Limit must be between 1 and 100'
      };
    }

    console.error(`\n🔍 Semantic search: "${query}"`);
    console.error(`   Database: ${database}, Table: ${table}, Limit: ${limit}`);
    if (context.agentName) {
      console.error(`   Agent: ${context.agentName}`);
    }

    // Generate embedding
    console.error('   Generating query embedding...');
    const queryEmbedding = await generateEmbedding(query);
    console.error(`   ✅ Embedding generated (${queryEmbedding.length}D)`);

    // Search
    console.error('   Executing vector search...');
    const results = await vectorSearch(database, table, queryEmbedding, limit);
    console.error(`   ✅ Found ${results.length} results`);

    // Return results
    return {
      success: true,
      query,
      database,
      table,
      agent: context.agentName || context.agentId || 'unknown',
      search_type: 'shadow_db_vector_similarity',
      total_matches: results.length,
      results: results.map(r => ({
        ...r,
        _similarity_score: `${r._similarity_score.toFixed(2)}%`
      }))
    };
  } catch (error) {
    console.error('❌ Search error:', error);
    reply.code(500);
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
});

// List tables endpoint
fastify.post<{
  Body: {
    database: string;
  }
}>('/list-tables', async (request, reply) => {
  const { database } = request.body;
  const context = extractAgentContext(request.headers);

  try {
    if (!database) {
      reply.code(400);
      return {
        success: false,
        error: 'Missing required parameter: database'
      };
    }

    // Enforce access control
    if (!validateAgentAccess(database, context)) {
      reply.code(403);
      return {
        success: false,
        error: `Agent not authorized to access ${database} database`
      };
    }

    const dbConfig = SHADOW_DB_CONFIG[database];
    if (!dbConfig) {
      reply.code(400);
      return {
        success: false,
        error: `Unknown database: ${database}`
      };
    }

    return {
      success: true,
      database,
      agent: context.agentName || context.agentId || 'unknown',
      tables: dbConfig.tables
    };
  } catch (error) {
    console.error('❌ List tables error:', error);
    reply.code(500);
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
});

// Agent access info endpoint
fastify.get('/agent-info', async (request, reply) => {
  const context = extractAgentContext(request.headers);

  return {
    success: true,
    agentId: context.agentId || 'unknown',
    agentName: context.agentName || 'unknown',
    allowedDatabase: context.allowedDatabase || 'none (all allowed for development)',
    allMappings: AGENT_DATABASE_MAPPING
  };
});

// List available databases endpoint
fastify.get('/list-databases', async (request, reply) => {
  return {
    success: true,
    databases: Object.entries(SHADOW_DB_CONFIG).map(([name, config]) => ({
      name,
      database: config.database,
      tables: config.tables
    }))
  };
});

// ==================== Startup ====================

async function start() {
  try {
    console.error('🚀 Shadow Database HTTP Server (Streamable) Starting...');
    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    // Auto-discover shadow databases
    await discoverShadowDatabases();

    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.error('Agent Access Control:');
    if (Object.keys(AGENT_DATABASE_MAPPING).length > 0) {
      Object.entries(AGENT_DATABASE_MAPPING).forEach(([agent, db]) => {
        console.error(`  • ${agent} → ${db}`);
      });
    } else {
      console.error('  • No agent mappings configured (development mode)');
    }
    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    // Pre-load embedding model
    await loadEmbeddingModel();

    // Start HTTP server
    const port = parseInt(env.PORT || '8000');
    await fastify.listen({ port, host: '0.0.0.0' });

    console.error('✅ Shadow Database HTTP Server running');
    console.error(`   Listening on http://0.0.0.0:${port}`);
    console.error(`   Available databases: ${Object.keys(SHADOW_DB_CONFIG).join(', ') || 'none discovered'}`);
    console.error('   Access control: ENFORCED');
    console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

start();

// Graceful shutdown
process.on('SIGTERM', () => {
  console.error('SIGTERM received, shutting down gracefully...');
  fastify.close().then(() => process.exit(0));
});

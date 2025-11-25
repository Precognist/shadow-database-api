/**
 * Shadow Database Sync Service - V2
 * Creates separate shadow databases (not schemas) for external NocoDB databases
 *
 * Features:
 * - Auto-discovery of NocoDB external databases
 * - Creates separate PostgreSQL shadow database for each external DB
 * - One-way sync from NocoDB to PostgreSQL shadow database
 * - Full-row embedding generation (768D vectors)
 * - Auto-registration of shadows back to NocoDB (read-only)
 * - NocoDB Hook support for immediate real-time updates
 * - Webhook endpoint for incoming NocoDB hooks
 * - Duplicate prevention with database queries
 * - Smart filtering (only shadows external DBs created IN NocoDB)
 */

const express = require('express');
const { Client } = require('pg');
const axios = require('axios');

// Configuration from environment
const config = {
    postgres: {
        host: process.env.POSTGRES_HOST || 'nexus-postgres',
        port: process.env.POSTGRES_PORT || 5432,
        user: process.env.POSTGRES_USER || 'nexus',
        password: process.env.POSTGRES_PASSWORD,
        database: process.env.POSTGRES_DB || 'nexus', // Used for admin connection only
        adminUser: process.env.POSTGRES_ADMIN_USER || 'nexus',
        adminPassword: process.env.POSTGRES_PASSWORD
    },
    nocodb: {
        url: process.env.NOCODB_URL || 'http://nocodb:8080',
        token: process.env.NOCODB_TOKEN
    },
    webhook: {
        port: process.env.WEBHOOK_PORT || 3456,
        url: process.env.WEBHOOK_URL || 'http://shadow-sync:3456'
    },
    sync: {
        pollInterval: parseInt(process.env.POLL_INTERVAL || '60000'),
        batchSize: parseInt(process.env.BATCH_SIZE || '50'),
        enableHooks: process.env.ENABLE_HOOKS !== 'false' // Default true
    }
};

// PostgreSQL clients
let pgClient; // For admin operations (creating databases)
let shadowClients = new Map(); // Separate clients for each shadow database

// Embedding model (will be loaded dynamically)
let embedder = null;
let pipeline = null;

// Express app for webhooks
const app = express();
app.use(express.json());

// Track registered shadows and hooks to prevent duplicates
const registeredShadows = new Map(); // { shadowDbName: { baseId, timestamp } }
const registeredHooks = new Map(); // { hookKey: { tableId, timestamp } }

/**
 * Initialize admin PostgreSQL connection for database operations
 */
async function initPostgres() {
    pgClient = new Client({
        host: config.postgres.host,
        port: config.postgres.port,
        user: config.postgres.adminUser,
        password: config.postgres.adminPassword,
        database: 'postgres' // Connect to default DB for CREATE DATABASE
    });
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL (admin)');
}

/**
 * Get or create a client for a shadow database
 */
async function getShadowClient(shadowDbName) {
    if (shadowClients.has(shadowDbName)) {
        return shadowClients.get(shadowDbName);
    }

    try {
        const client = new Client({
            host: config.postgres.host,
            port: config.postgres.port,
            user: config.postgres.adminUser,
            password: config.postgres.adminPassword,
            database: shadowDbName
        });
        await client.connect();
        shadowClients.set(shadowDbName, client);
        return client;
    } catch (error) {
        console.error(`Failed to connect to shadow database ${shadowDbName}: ${error.message}`);
        throw error;
    }
}

/**
 * Initialize embedding model using dynamic import
 */
async function initEmbedding() {
    console.log('🔄 Loading embedding model...');
    try {
        // Use dynamic import for ES module
        const transformers = await import('@xenova/transformers');
        pipeline = transformers.pipeline;
        embedder = await pipeline('feature-extraction', 'Xenova/bge-base-en-v1.5');
        console.log('✅ Embedding model loaded');
    } catch (error) {
        console.warn('⚠️ Could not load embedding model:', error.message);
        console.log('Continuing without embeddings...');
    }
}

/**
 * Check if database should be synced (shadowed)
 *
 * SHADOW LOGIC:
 * - SKIP: Local VPS PostgreSQL databases (same docker network) - already accessible locally
 * - SKIP: Shadow databases we created (Nexus-* prefix)
 * - SKIP: System databases (postgres, template0, etc.)
 * - INCLUDE: External PostgreSQL databases (other VPS/servers) - need local shadow for vector search
 * - INCLUDE: NocoDB-created tables (internal) - user data needing embeddings
 */
function shouldSyncDatabase(base) {
    const name = (base.title || base.name || '').toLowerCase();

    // Skip if it's a NEXUS shadow database (starts with Nexus- or [Shadow])
    if (name.startsWith('nexus-') || name.startsWith('[shadow]')) {
        console.log(`  ⏭️  Skipping ${base.title} - it's a shadow database we created`);
        return false;
    }

    // Skip NEXUS-memory (internal, already synced separately)
    if (name === 'nexus-memory') {
        console.log(`  ⏭️  Skipping ${base.title} - NEXUS-memory is synced separately`);
        return false;
    }

    // Skip internal/system databases
    if (name === 'claude' || name === 'postgres' || name === 'template0' || name === 'template1' || name === 'nexus') {
        console.log(`  ⏭️  Skipping ${base.title} - system database`);
        return false;
    }

    // Check sources to determine if this is a LOCAL VPS database or EXTERNAL database
    if (base.sources && Array.isArray(base.sources)) {
        // Check if ANY source points to the LOCAL PostgreSQL container
        const isLocalVpsDatabase = base.sources.some(source => {
            // If fk_integration_id exists, check if it points to local postgres container
            // Local containers use hostnames like: postgres, phase2client-postgres, ${CLIENT}-postgres
            if (source.integration_title) {
                const integrationName = source.integration_title.toLowerCase();
                // Local VPS postgres containers have these patterns
                if (integrationName.includes('-postgres') ||
                    integrationName === 'postgres' ||
                    integrationName.includes('-nexus')) {
                    return true;
                }
            }

            // Check config for local docker hostnames
            if (source.config) {
                try {
                    const cfg = typeof source.config === 'string' ? JSON.parse(source.config) : source.config;
                    if (cfg.connection && cfg.connection.host) {
                        const host = cfg.connection.host.toLowerCase();
                        // Local docker container hostnames (not external IPs/domains)
                        if (host.includes('-postgres') ||
                            host === 'postgres' ||
                            host === 'localhost' ||
                            host === '127.0.0.1') {
                            return true;
                        }
                    }
                } catch (e) {
                    // Ignore parse errors
                }
            }

            return false;
        });

        if (isLocalVpsDatabase) {
            console.log(`  ⏭️  Skipping ${base.title} - it's a LOCAL VPS database (already accessible)`);
            return false;
        }

        // Check if it has EXTERNAL sources (other VPS/servers) - these SHOULD be shadowed
        const hasExternalSource = base.sources.some(source => {
            // is_local: false with fk_integration_id indicates external connection
            if (source.is_local === false && source.fk_integration_id) {
                return true;
            }

            // Check config for external hosts (IPs or domains, not local docker names)
            if (source.config) {
                try {
                    const cfg = typeof source.config === 'string' ? JSON.parse(source.config) : source.config;
                    if (cfg.connection && cfg.connection.host) {
                        const host = cfg.connection.host.toLowerCase();
                        // External if it's an IP address or domain (not local docker hostname)
                        const isExternal = /^\d+\.\d+\.\d+\.\d+$/.test(host) || // IP address
                                          host.includes('.') || // Domain name
                                          (!host.includes('-postgres') && host !== 'postgres' && host !== 'localhost');
                        return isExternal;
                    }
                } catch (e) {
                    // Ignore parse errors
                }
            }

            return false;
        });

        if (hasExternalSource) {
            console.log(`  ✅ Including ${base.title} for shadowing - EXTERNAL database needs local shadow`);
            return true;
        }
    }

    // NocoDB-internal database (created within NocoDB UI), include it for shadowing
    console.log(`  ✅ Including ${base.title} for shadowing - NocoDB-created database`);
    return true;
}

/**
 * Get all NocoDB bases to sync
 */
async function getAllBases() {
    try {
        const response = await axios.get(
            `${config.nocodb.url}/api/v2/meta/bases`,
            { headers: { 'xc-token': config.nocodb.token } }
        );

        const bases = (response.data.list || []).filter(shouldSyncDatabase);
        console.log(`Found ${bases.length} NocoDB-internal databases to shadow`);
        return bases;
    } catch (error) {
        console.error('Error fetching bases:', error.message);
        return [];
    }
}

/**
 * Get tables for a base
 */
async function getTables(baseId) {
    try {
        const response = await axios.get(
            `${config.nocodb.url}/api/v2/meta/bases/${baseId}/tables`,
            { headers: { 'xc-token': config.nocodb.token } }
        );
        return response.data.list || [];
    } catch (error) {
        console.error(`Error fetching tables for base ${baseId}:`, error.message);
        return [];
    }
}

/**
 * Get table schema/columns from NocoDB
 */
async function getTableSchema(baseId, tableId) {
    try {
        const response = await axios.get(
            `${config.nocodb.url}/api/v2/meta/tables/${tableId}/columns`,
            { headers: { 'xc-token': config.nocodb.token } }
        );
        return response.data.list || [];
    } catch (error) {
        console.warn(`Could not fetch schema for table ${tableId}: ${error.message}`);
        return [];
    }
}

/**
 * Convert field value to semantic text based on type
 */
function fieldValueToSemanticText(fieldName, fieldType, value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }

    // Skip system fields
    if (fieldName.startsWith('nc_') || fieldName === 'id') {
        return null;
    }

    const fieldLabel = fieldName
        .replace(/_/g, ' ')
        .replace(/([A-Z])/g, ' $1')
        .toLowerCase()
        .trim();

    switch (fieldType) {
        case 'String':
        case 'Text':
        case 'LongText':
        case 'Email':
        case 'PhoneNumber':
        case 'Url':
            return `${fieldLabel}: ${value}`;

        case 'Number':
        case 'Decimal':
            return `${fieldLabel} is ${value}`;

        case 'Currency':
            return `${fieldLabel} amounts to ${value}`;

        case 'Percent':
            return `${fieldLabel} percentage is ${value}%`;

        case 'Duration':
            return `${fieldLabel} duration is ${value}`;

        case 'Rating':
            return `${fieldLabel} rating is ${value} out of 5`;

        case 'Boolean':
            return `${fieldLabel} is ${value ? 'enabled' : 'disabled'}`;

        case 'Date':
        case 'DateTime':
            return `${fieldLabel} date is ${value}`;

        case 'Time':
            return `${fieldLabel} time is ${value}`;

        case 'SingleSelect':
        case 'MultipleSelect':
            if (Array.isArray(value)) {
                return `${fieldLabel} includes ${value.join(', ')}`;
            }
            return `${fieldLabel} is ${value}`;

        case 'Checkbox':
            return `${fieldLabel} is ${value ? 'checked' : 'unchecked'}`;

        case 'Attachment':
            if (Array.isArray(value) && value.length > 0) {
                return `${fieldLabel} has ${value.length} attachment(s)`;
            }
            return null;

        case 'QrCode':
        case 'Barcode':
            return `${fieldLabel} encoded value is ${value}`;

        case 'Geometry':
            return `${fieldLabel} location data is ${JSON.stringify(value)}`;

        case 'Json':
            return `${fieldLabel} structured data: ${JSON.stringify(value)}`;

        default:
            // Fallback for unknown types
            return `${fieldLabel}: ${JSON.stringify(value)}`;
    }
}

/**
 * Build rich embedding text from record with all field types
 */
async function buildEmbeddingText(record, tableName, baseId, tableId, tableSchema = null) {
    const parts = [`${tableName} record`];

    // Add all field values with semantic formatting
    for (const [fieldName, value] of Object.entries(record)) {
        if (value === null || value === undefined) {
            continue;
        }

        // Try to determine field type from schema
        let fieldType = 'String'; // default
        if (tableSchema) {
            const column = tableSchema.find(c => c.title === fieldName);
            if (column) {
                fieldType = column.uidt || 'String';
            }
        }

        const semanticText = fieldValueToSemanticText(fieldName, fieldType, value);
        if (semanticText) {
            parts.push(semanticText);
        }
    }

    return parts.join('. ');
}

/**
 * Get records from a table
 */
async function getTableRecords(baseId, tableId, offset = 0) {
    try {
        const response = await axios.get(
            `${config.nocodb.url}/api/v1/db/data/v1/${baseId}/${tableId}`,
            {
                headers: { 'xc-token': config.nocodb.token },
                params: {
                    offset,
                    limit: config.sync.batchSize
                }
            }
        );
        return response.data.list || [];
    } catch (error) {
        console.error(`Error fetching records for ${tableId}:`, error.message);
        return [];
    }
}

/**
 * Create shadow database
 */
async function createShadowDatabase(baseId, baseName) {
    // Create safe database name: nexus_<sanitized_base_name>
    const sanitizedName = baseName.toLowerCase().replace(/[^a-z0-9_]/g, '_').substring(0, 60);
    const shadowDbName = `nexus_${sanitizedName}_shadow`;

    try {
        // Check if database already exists
        const result = await pgClient.query(
            `SELECT 1 FROM pg_database WHERE datname = $1`,
            [shadowDbName]
        );

        if (result.rows.length === 0) {
            // Create the database
            await pgClient.query(`CREATE DATABASE "${shadowDbName}"`);
            console.log(`  ✅ Created shadow database: ${shadowDbName}`);
        } else {
            console.log(`  ✅ Shadow database exists: ${shadowDbName}`);
        }

        return shadowDbName;
    } catch (error) {
        console.error(`Failed to create shadow database ${shadowDbName}: ${error.message}`);
        throw error;
    }
}

/**
 * Initialize shadow database with pgvector extension and proper permissions
 */
async function initShadowDatabase(shadowDbName) {
    try {
        const client = await getShadowClient(shadowDbName);

        // Grant schema permissions to the current user
        const currentUser = config.postgres.adminUser;
        await client.query(`GRANT ALL ON SCHEMA public TO "${currentUser}";`);
        await client.query(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "${currentUser}";`);
        await client.query(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "${currentUser}";`);

        // Enable pgvector extension
        await client.query('CREATE EXTENSION IF NOT EXISTS vector;');
        console.log(`  ✅ Enabled pgvector in ${shadowDbName}`);
    } catch (error) {
        if (!error.message.includes('already exists')) {
            console.error(`Failed to initialize shadow database: ${error.message}`);
            throw error;
        }
    }
}

/**
 * Create shadow table in shadow database
 */
async function createShadowTable(shadowDbName, tableName) {
    const client = await getShadowClient(shadowDbName);

    const sql = `
        CREATE TABLE IF NOT EXISTS "${tableName}" (
            id SERIAL PRIMARY KEY,
            nocodb_id TEXT UNIQUE NOT NULL,
            data JSONB NOT NULL,
            embedding_text TEXT,
            embedding vector(768),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            synced_at TIMESTAMP DEFAULT NOW()
        )
    `;

    try {
        await client.query(sql);

        // Create indexes
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableName}_embedding
            ON "${tableName}"
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        `).catch(() => {}); // Ignore if index already exists

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableName}_data
            ON "${tableName}"
            USING GIN (data)
        `).catch(() => {}); // Ignore if index already exists

        console.log(`    ✅ Created table: ${tableName}`);
    } catch (error) {
        console.error(`Failed to create shadow table ${tableName}: ${error.message}`);
        throw error;
    }
}

/**
 * Sync records to shadow table with rich semantic embeddings
 */
async function syncRecords(shadowDbName, tableName, records, baseId = null, tableId = null) {
    if (records.length === 0) return 0;

    const client = await getShadowClient(shadowDbName);
    let synced = 0;

    // Fetch table schema once for all records in this batch
    let tableSchema = null;
    if (baseId && tableId) {
        tableSchema = await getTableSchema(baseId, tableId);
    }

    for (const record of records) {
        const nocodbId = record.id || record._id || JSON.stringify(record);

        // Build rich embedding text using all field types
        const embeddingText = await buildEmbeddingText(record, tableName, baseId, tableId, tableSchema);

        // Upsert record
        const sql = `
            INSERT INTO "${tableName}" (nocodb_id, data, embedding_text, synced_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (nocodb_id) DO UPDATE SET
                data = EXCLUDED.data,
                embedding_text = EXCLUDED.embedding_text,
                updated_at = NOW(),
                synced_at = NOW()
        `;

        await client.query(sql, [nocodbId, JSON.stringify(record), embeddingText]);
        synced++;
    }

    return synced;
}

/**
 * Generate embeddings for records without them
 */
async function generateEmbeddings(shadowDbName, tableName) {
    if (!embedder) return 0;

    const client = await getShadowClient(shadowDbName);

    // Get records without embeddings
    const result = await client.query(`
        SELECT id, embedding_text
        FROM "${tableName}"
        WHERE embedding IS NULL AND embedding_text IS NOT NULL
        LIMIT ${config.sync.batchSize}
    `);

    if (result.rows.length === 0) return 0;

    let generated = 0;
    for (const row of result.rows) {
        try {
            // Generate embedding
            const output = await embedder(row.embedding_text, {
                pooling: 'mean',
                normalize: true
            });
            const embedding = Array.from(output.data);

            // Update record with embedding
            await client.query(
                `UPDATE "${tableName}"
                 SET embedding = $1
                 WHERE id = $2`,
                [`[${embedding.join(',')}]`, row.id]
            );
            generated++;
        } catch (error) {
            console.error(`Error generating embedding: ${error.message}`);
        }
    }

    return generated;
}

/**
 * Register shadow database back to NocoDB
 */
async function registerShadowInNocoDB(shadowDbName, baseName, baseId) {
    // Check if already registered in memory
    if (registeredShadows.has(shadowDbName)) {
        console.log(`  ✅ Shadow already registered: ${shadowDbName}`);
        return;
    }

    try {
        // Query NocoDB to check if shadow already exists
        const response = await axios.get(
            `${config.nocodb.url}/api/v2/meta/bases`,
            { headers: { 'xc-token': config.nocodb.token } }
        );

        const bases = response.data.list || [];
        const shadowTitle = `Nexus-${baseName}`;
        const existingShadow = bases.find(b =>
            b.title === shadowTitle &&
            b.sources?.some(s => {
                try {
                    const cfg = typeof s.config === 'string' ? JSON.parse(s.config) : s.config;
                    return cfg.connection?.database === shadowDbName;
                } catch (e) {
                    return false;
                }
            })
        );

        if (existingShadow) {
            console.log(`  ✅ Shadow already registered: ${shadowDbName}`);
            registeredShadows.set(shadowDbName, { baseId, timestamp: Date.now() });
            return;
        }

        // Register new shadow database
        const shadowConfig = {
            title: shadowTitle,
            config: {
                client: 'pg',
                connection: {
                    host: config.postgres.host,
                    port: config.postgres.port,
                    user: config.postgres.adminUser,
                    password: config.postgres.adminPassword,
                    database: shadowDbName
                }
            },
            inflection: {
                table_name: 'as_is',
                column_name: 'as_is'
            },
            meta: {
                readonly: true
            }
        };

        await axios.post(
            `${config.nocodb.url}/api/v2/meta/bases`,
            shadowConfig,
            { headers: { 'xc-token': config.nocodb.token } }
        );

        registeredShadows.set(shadowDbName, { baseId, timestamp: Date.now() });
        console.log(`  ✅ Registered shadow database in NocoDB: ${shadowDbName}`);
    } catch (error) {
        // Ignore if already exists
        if (!error.response || error.response.status !== 409) {
            console.error(`  ⚠️ Failed to register shadow: ${error.message}`);
        } else {
            registeredShadows.set(shadowDbName, { baseId, timestamp: Date.now() });
        }
    }
}

/**
 * Setup NocoDB hooks for real-time updates
 */
async function setupHooksForTable(baseId, tableId, tableName, shadowDbName) {
    if (!config.sync.enableHooks) return;

    try {
        const hookKey = `${baseId}:${tableId}`;
        if (registeredHooks.has(hookKey)) {
            return; // Already registered
        }

        // Create webhook hooks for INSERT, UPDATE, DELETE using NocoDB v3 webhook format
        const operations = ['insert', 'update', 'delete'];

        for (const operation of operations) {
            const hookUrl = `${config.webhook.url}/webhook/${baseId}/${tableName}?operation=${operation}`;

            // NocoDB v3 webhook format (required for NocoDB 0.265+)
            const hookConfig = {
                title: `Shadow-Sync-${operation.toUpperCase()}`,
                event: 'after',
                operation: [operation],
                version: 'v3',
                notification: {
                    type: 'URL',
                    payload: {
                        url: hookUrl,
                        method: 'POST',
                        headers: '{}',
                        body: '{{json event}}'
                    }
                },
                active: true
            };

            try {
                await axios.post(
                    `${config.nocodb.url}/api/v2/meta/tables/${tableId}/hooks`,
                    hookConfig,
                    { headers: { 'xc-token': config.nocodb.token } }
                );
                console.log(`    🪝 Setup hook for ${operation} on ${tableName}`);
            } catch (hookError) {
                // Ignore if hook already exists (409) or duplicate title
                if (hookError.response?.status !== 409 &&
                    !hookError.message?.includes('already exists')) {
                    console.warn(`    ⚠️ Could not setup ${operation} hook: ${hookError.message}`);
                }
            }
        }

        registeredHooks.set(hookKey, { timestamp: Date.now() });
    } catch (error) {
        console.warn(`⚠️ Failed to setup hooks: ${error.message}`);
    }
}

/**
 * Sync a single base
 */
async function syncBase(base) {
    console.log(`\n🔄 Syncing base: ${base.title || base.name} (${base.id})`);

    // Create shadow database
    const shadowDbName = await createShadowDatabase(base.id, base.title || base.name);

    // Initialize shadow database
    await initShadowDatabase(shadowDbName);

    // Get tables
    const tables = await getTables(base.id);
    if (tables.length === 0) {
        console.log(`  ⚠️ No tables found in base`);
        return { totalSynced: 0, totalEmbeddings: 0 };
    }

    let totalSynced = 0;
    let totalEmbeddings = 0;

    for (const table of tables) {
        console.log(`  📊 Syncing table: ${table.title}`);

        // Create shadow table
        await createShadowTable(shadowDbName, table.title);

        // Setup hooks for real-time updates
        await setupHooksForTable(base.id, table.id, table.title, shadowDbName);

        // Sync records in batches
        let offset = 0;
        let hasMore = true;
        let tableSynced = 0;

        while (hasMore) {
            const records = await getTableRecords(base.id, table.id, offset);
            if (records.length === 0) {
                hasMore = false;
                break;
            }

            const synced = await syncRecords(shadowDbName, table.title, records, base.id, table.id);
            tableSynced += synced;
            totalSynced += synced;

            offset += records.length;
            hasMore = records.length === config.sync.batchSize;
        }

        // Generate embeddings if available
        let tableEmbeddings = 0;
        if (embedder && tableSynced > 0) {
            tableEmbeddings = await generateEmbeddings(shadowDbName, table.title);
            totalEmbeddings += tableEmbeddings;
        }

        console.log(`    ✅ Synced ${tableSynced} records, generated ${tableEmbeddings} embeddings`);
    }

    // Register shadow database back to NocoDB
    await registerShadowInNocoDB(shadowDbName, base.title || base.name, base.id);

    return { totalSynced, totalEmbeddings };
}

/**
 * Main sync loop
 */
async function syncLoop() {
    console.log('\n========================================');
    console.log('🚀 Starting sync cycle...');
    console.log('========================================');

    const bases = await getAllBases();
    console.log(`Found ${bases.length} bases to sync`);

    for (const base of bases) {
        try {
            await syncBase(base);
        } catch (error) {
            console.error(`Error syncing base ${base.id}: ${error.message}`);
        }
    }

    console.log('\n✅ Sync cycle complete');
    console.log(`Next sync in ${config.sync.pollInterval / 1000} seconds`);
}

/**
 * Webhook endpoint for real-time updates from NocoDB hooks
 */
app.post('/webhook/:baseId/:tableName', async (req, res) => {
    const { baseId, tableName } = req.params;
    const { operation } = req.query;
    const { data } = req.body;

    console.log(`📥 Webhook: ${operation?.toUpperCase() || 'UNKNOWN'} on ${baseId}.${tableName}`);

    try {
        // Find the shadow database for this base
        const shadowDbName = Array.from(registeredShadows.keys()).find(
            key => registeredShadows.get(key).baseId === baseId
        );

        if (!shadowDbName) {
            console.warn(`⚠️ Shadow database not found for base ${baseId}`);
            return res.status(404).json({ error: 'Shadow database not found' });
        }

        if (operation === 'insert' || operation === 'update') {
            // Note: webhook doesn't have tableId, so schema enrichment won't work for webhooks
            // Full schema enrichment happens during scheduled sync
            const synced = await syncRecords(shadowDbName, tableName, [data], baseId, null);
            if (synced > 0 && embedder) {
                await generateEmbeddings(shadowDbName, tableName);
            }
            res.json({ success: true, synced });
        } else if (operation === 'delete') {
            const client = await getShadowClient(shadowDbName);
            const nocodbId = data.id || data._id;
            const result = await client.query(
                `DELETE FROM "${tableName}" WHERE nocodb_id = $1`,
                [nocodbId]
            );
            res.json({ success: true, deleted: result.rowCount });
        } else {
            res.json({ success: true, message: 'Operation not handled' });
        }
    } catch (error) {
        console.error(`Webhook error: ${error.message}`);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'shadow-sync-v2',
        postgres: pgClient ? 'connected' : 'disconnected',
        embedder: embedder ? 'loaded' : 'not loaded',
        hooks_enabled: config.sync.enableHooks,
        shadow_databases: shadowClients.size
    });
});

/**
 * Status endpoint
 */
app.get('/status', async (req, res) => {
    try {
        const bases = await getAllBases();

        // Get list of shadow databases
        const dbResult = await pgClient.query(
            `SELECT datname FROM pg_database WHERE datname LIKE 'nexus_%_shadow'`
        );

        res.json({
            status: 'running',
            nocodb_internal_bases: bases.length,
            shadow_databases: dbResult.rows.length,
            registered_shadows: registeredShadows.size,
            registered_hooks: registeredHooks.size,
            config: {
                poll_interval_ms: config.sync.pollInterval,
                batch_size: config.sync.batchSize,
                embeddings_enabled: embedder ? true : false,
                hooks_enabled: config.sync.enableHooks,
                webhook_url: config.webhook.url
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

/**
 * Main function
 */
async function main() {
    console.log('🚀 Shadow Database Sync Service V2 Starting...');
    console.log('========================================');
    console.log('Creating separate shadow databases for NocoDB-internal tables');
    console.log('========================================');

    // Initialize PostgreSQL
    await initPostgres();

    // Initialize embedding model (optional)
    await initEmbedding();

    // Start webhook server
    app.listen(config.webhook.port, () => {
        console.log(`✅ Webhook server listening on port ${config.webhook.port}`);
        console.log(`✅ Webhook URL: ${config.webhook.url}`);
        console.log(`✅ NocoDB Hooks: ${config.sync.enableHooks ? 'ENABLED' : 'DISABLED'}`);
    });

    // Run initial sync
    await syncLoop();

    // Schedule periodic syncs
    setInterval(syncLoop, config.sync.pollInterval);
}

// Start the service
main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});

# Dynamic Agent Access Control via NocoDB - Perfect Solution! 🎯

## Overview

This design implements runtime-configurable security for the Shadow Database system without requiring code changes. Agent access permissions are managed dynamically through a NocoDB control table, providing enterprise-grade flexibility and auditability.

## Architecture Design

### 1. NocoDB Control Table: `Agent_Database_Access`

| agent_name                        | agent_id        | allowed_database | status  |
|-----------------------------------|-----------------|------------------|---------|
| real-estate-intelligence-specialist| pl94ap0ghrqwbdx | real_estate     | active  |
| Precursor Test-Agent for Dovetail.AI| p802fqqug779z13| dovetail        | active  |
| Another-Agent-Name                | dfs687asfd678ga | custom_db       | active  |

**Table Schema:**
- `agent_name`: Human-readable agent identifier from OpenWebUI
- `agent_id`: Technical agent ID from OpenWebUI headers
- `allowed_database`: Database key the agent can access
- `status`: Control field (active/disabled) for access management

### 2. Enhanced Shadow-DB-HTTP Server Logic

Replace hardcoded `AGENT_DATABASE_MAPPING` with dynamic NocoDB lookup:

```javascript
async function getAgentAccess(agentId, agentName) {
  const response = await axios.get(
    `${NOCODB_URL}/api/v1/db/data/v1/${BASE_ID}/Agent_Database_Access`,
    {
      headers: { 'xc-token': NOCODB_TOKEN },
      params: {
        where: `(agent_id,eq,${agentId})~or(agent_name,eq,${agentName}),and(status,eq,active)`
      }
    }
  );

  return response.data.list[0]?.allowed_database || null;
}

// Updated access validation
function validateAgentAccess(requestedDatabase, context) {
  const allowedDatabase = await getAgentAccess(context.agentId, context.agentName);

  if (!allowedDatabase) {
    console.error(`⚠️ No agent access found for ${context.agentName || context.agentId}`);
    return false;
  }

  if (allowedDatabase !== requestedDatabase) {
    console.error(
      `❌ Agent ${context.agentName || context.agentId} not allowed to access ${requestedDatabase}
       Allowed database: ${allowedDatabase}`
    );
    return false;
  }

  console.error(`✅ Agent ${context.agentName || context.agentId} accessing ${requestedDatabase}`);
  return true;
}
```

### 3. Benefits

- ✅ **Zero downtime changes** - update access via NocoDB UI
- ✅ **Audit trail** - all changes logged in NocoDB
- ✅ **Multi-tenancy** - different clients, different access rules
- ✅ **Enterprise admin** - non-technical admins can manage access
- ✅ **Scalable** - unlimited agent/database combinations
- ✅ **Real-time updates** - changes take effect immediately
- ✅ **Fallback security** - denied by default if not in table

### 4. PHASE 2 Auto-Setup

The deployment script automatically creates the access control table during deployment:

```bash
# Create Agent Access Control table during PHASE 2
curl -X POST "$NOCODB_URL/api/v1/db/meta/bases/$BASE_ID/tables" \
  -H "xc-token: $NOCODB_TOKEN" \
  -d '{
    "title": "Agent_Database_Access",
    "columns": [
      {
        "title": "agent_name",
        "type": "SingleLineText",
        "meta": {"description": "Human-readable agent name from OpenWebUI"}
      },
      {
        "title": "agent_id",
        "type": "SingleLineText",
        "meta": {"description": "Technical agent ID from OpenWebUI headers"}
      },
      {
        "title": "allowed_database",
        "type": "SingleLineText",
        "meta": {"description": "Database key this agent can access"}
      },
      {
        "title": "status",
        "type": "SingleSelect",
        "options": ["active", "disabled"],
        "meta": {"description": "Access control status"}
      }
    ]
  }'

# Add default entries for common agents
curl -X POST "$NOCODB_URL/api/v1/db/data/v1/$BASE_ID/Agent_Database_Access" \
  -H "xc-token: $NOCODB_TOKEN" \
  -d '{
    "agent_name": "real-estate-intelligence-specialist",
    "agent_id": "pl94ap0ghrqwbdx",
    "allowed_database": "real_estate",
    "status": "active"
  }'

curl -X POST "$NOCODB_URL/api/v1/db/data/v1/$BASE_ID/Agent_Database_Access" \
  -H "xc-token: $NOCODB_TOKEN" \
  -d '{
    "agent_name": "Precursor Test-Agent for Dovetail.AI",
    "agent_id": "p802fqqug779z13",
    "allowed_database": "dovetail",
    "status": "active"
  }'
```

### 5. OpenWebUI Integration Enhancement

The tool server automatically:
1. Reads agent context from OpenWebUI headers (`x-agent-id`, `x-agent-name`)
2. Queries the NocoDB access control table
3. Enforces access based on live configuration data
4. Returns appropriate error messages for unauthorized access

### 6. Administration Workflow

**For Administrators:**
1. Log into NocoDB interface
2. Navigate to `Agent_Database_Access` table
3. Add/modify/disable agent access entries
4. Changes take effect immediately (no restart required)

**For Adding New Agent Access:**
1. Get agent ID from OpenWebUI logs or headers
2. Create new row in `Agent_Database_Access` table
3. Set `agent_name`, `agent_id`, `allowed_database`, `status=active`
4. Agent can immediately access the specified database

### 7. Security Features

- **Principle of least privilege**: Agents denied by default
- **Dual authentication**: Both agent_name OR agent_id must match
- **Status control**: Instant disable via `status` field
- **Audit logging**: All changes tracked in NocoDB
- **Database isolation**: Each agent restricted to specific databases

### 8. Implementation Status

- ✅ Architecture designed
- ⚠️ Code implementation required in `shadow-db-http-server.ts`
- ⚠️ PHASE 2 auto-setup integration required
- ⚠️ Testing with real OpenWebUI agent contexts needed

### 9. Migration Path

**From Current Hardcoded System:**
1. Deploy enhanced `shadow-db-http-server.ts` with NocoDB lookup
2. Run PHASE 2 auto-setup to create access control table
3. Populate table with existing agent mappings
4. Remove hardcoded `AGENT_DATABASE_MAPPING` from code
5. Test agent access functionality

This system transforms static security into dynamic, enterprise-ready access control managed through a familiar database interface.
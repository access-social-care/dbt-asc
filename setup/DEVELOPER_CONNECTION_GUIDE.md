# Developer Connection Guide: Snowflake ANALYTICS Schema

**Audience**: Developers building tenant reports, data APIs, or analytical tools that query dbt mart tables.

**Access Level**: Read-only on `ANALYTICS.PUBLIC` schema (cannot modify raw data or write to tables).

**Authentication**: RSA key pair (no password).

---

## 1. Get Your Credentials

**Request from**: Data team lead (Amit)

**You will receive**:
- **Snowflake account URL**: `https://RWPQBWL-QD95964.snowflakecomputing.com`
- **Username**: `TENANT_REPORTS_USER` (shared service account)
- **Private key file**: `tenant_reports_rsa_key.p8`
- **Warehouse**: `COMPUTE_WH`
- **Database**: `ANALYTICS`
- **Schema**: `PUBLIC`

**Security**: Store the private key securely. Do NOT commit to Git or share publicly.

---

## 2. Setup: Environment Variables

Set these on your development machine:

### Windows (PowerShell - User level)

```powershell
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_SERVER', 'RWPQBWL-QD95964.snowflakecomputing.com', [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_USER', 'TENANT_REPORTS_USER', [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_KEY_FILE', 'C:\path\to\tenant_reports_rsa_key.p8', [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_WAREHOUSE', 'COMPUTE_WH', [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_DATABASE', 'ANALYTICS', [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('SNOWFLAKE_TENANT_REPORTS_SCHEMA', 'PUBLIC', [System.EnvironmentVariableTarget]::User)
```

**Restart your terminal** after setting environment variables.

### Linux/Mac (bash)

```bash
# Add to ~/.bashrc or ~/.profile
export SNOWFLAKE_TENANT_REPORTS_SERVER="RWPQBWL-QD95964.snowflakecomputing.com"
export SNOWFLAKE_TENANT_REPORTS_USER="TENANT_REPORTS_USER"
export SNOWFLAKE_TENANT_REPORTS_KEY_FILE="$HOME/.ssh/snowflake/tenant_reports_rsa_key.p8"
export SNOWFLAKE_TENANT_REPORTS_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_TENANT_REPORTS_DATABASE="ANALYTICS"
export SNOWFLAKE_TENANT_REPORTS_SCHEMA="PUBLIC"

# Apply
source ~/.bashrc
```

---

## 3. Connection Examples

### Python (snowflake-connector-python)

**Install**:
```bash
pip install snowflake-connector-python cryptography
```

**Connect**:
```python
import os
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load private key
with open(os.getenv('SNOWFLAKE_TENANT_REPORTS_KEY_FILE'), 'rb') as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account='RWPQBWL-QD95964',
    user=os.getenv('SNOWFLAKE_TENANT_REPORTS_USER'),
    private_key=pkb,
    warehouse=os.getenv('SNOWFLAKE_TENANT_REPORTS_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_TENANT_REPORTS_DATABASE'),
    schema=os.getenv('SNOWFLAKE_TENANT_REPORTS_SCHEMA'),
    role='ROLE_TENANT_REPORTS_READ'
)

# Query
cursor = conn.cursor()
cursor.execute("""
    SELECT TENANT_NAME, TOTAL_CONVERSATIONS
    FROM MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL
    ORDER BY TOTAL_CONVERSATIONS DESC
    LIMIT 10
""")

for row in cursor:
    print(row)

cursor.close()
conn.close()
```

---

### R (odbc)

**Install** (if not already):
```r
install.packages("odbc")
install.packages("DBI")
```

**Snowflake ODBC driver** must be installed: [Download here](https://docs.snowflake.com/en/user-guide/odbc-download.html)

**Connect**:
```r
library(DBI)
library(odbc)

con <- dbConnect(
  odbc::odbc(),
  Driver = "SnowflakeDSIIDriver",
  Server = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_SERVER"),
  UID = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_USER"),
  Warehouse = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_WAREHOUSE"),
  Database = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_DATABASE"),
  Schema = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_SCHEMA"),
  authenticator = "snowflake_jwt",
  priv_key_file = Sys.getenv("SNOWFLAKE_TENANT_REPORTS_KEY_FILE"),
  Role = "ROLE_TENANT_REPORTS_READ"
)

# Query
df <- dbGetQuery(con, "
  SELECT TENANT_NAME, TOTAL_CONVERSATIONS
  FROM MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL
  ORDER BY TOTAL_CONVERSATIONS DESC
  LIMIT 10
")

print(df)
dbDisconnect(con)
```

---

### Node.js (snowflake-sdk)

**Install**:
```bash
npm install snowflake-sdk
```

**Connect**:
```javascript
const snowflake = require('snowflake-sdk');
const fs = require('fs');

// Load private key
const privateKeyPath = process.env.SNOWFLAKE_TENANT_REPORTS_KEY_FILE;
const privateKey = fs.readFileSync(privateKeyPath, 'utf8');

const connection = snowflake.createConnection({
  account: 'RWPQBWL-QD95964',
  username: process.env.SNOWFLAKE_TENANT_REPORTS_USER,
  privateKey: privateKey,
  warehouse: process.env.SNOWFLAKE_TENANT_REPORTS_WAREHOUSE,
  database: process.env.SNOWFLAKE_TENANT_REPORTS_DATABASE,
  schema: process.env.SNOWFLAKE_TENANT_REPORTS_SCHEMA,
  role: 'ROLE_TENANT_REPORTS_READ'
});

connection.connect((err, conn) => {
  if (err) {
    console.error('Unable to connect:', err.message);
    return;
  }
  
  console.log('Successfully connected to Snowflake');
  
  conn.execute({
    sqlText: `
      SELECT TENANT_NAME, TOTAL_CONVERSATIONS
      FROM MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL
      ORDER BY TOTAL_CONVERSATIONS DESC
      LIMIT 10
    `,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Failed to execute:', err.message);
      } else {
        console.log('Query results:', rows);
      }
      connection.destroy();
    }
  });
});
```

---

### JDBC (Java, Scala, etc.)

**Driver**: [Download Snowflake JDBC](https://docs.snowflake.com/en/user-guide/jdbc-download.html)

**Connection string**:
```
jdbc:snowflake://RWPQBWL-QD95964.snowflakecomputing.com/?warehouse=COMPUTE_WH&db=ANALYTICS&schema=PUBLIC&role=ROLE_TENANT_REPORTS_READ
```

**Properties**:
```properties
user=TENANT_REPORTS_USER
privateKeyFile=/path/to/tenant_reports_rsa_key.p8
authenticator=SNOWFLAKE_JWT
```

---

## 4. Available Tables

Query the data dictionary:

```sql
-- List all available tables
SHOW TABLES IN SCHEMA ANALYTICS.PUBLIC;

-- Describe table structure
DESC TABLE ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY;

-- View column descriptions (from dbt docs)
SELECT * FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME LIKE 'MART_%';
```

**Current tables** (as of 2026-03-16):
- `MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY` - Monthly conversation counts per tenant
- `MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL` - All-time conversation totals per tenant

**Future tables** (check dbt docs for updates):
- Casework marts (legal casework aggregations)
- Unified citizen journey marts (cross-source linkage)
- Canonical dimension tables (local authorities, members, etc.)

---

## 5. Query Best Practices

### Use fully qualified table names
```sql
-- ✅ Good
SELECT * FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;

-- ❌ Avoid (depends on session defaults)
SELECT * FROM MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;
```

### Limit result sets for exploratory queries
```sql
-- Add LIMIT when testing
SELECT * FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY
LIMIT 100;
```

### Use WHERE filters to reduce data scanned
```sql
-- Filter early
SELECT TENANT_NAME, SUM(CONVERSATION_COUNT) AS TOTAL
FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_MONTHLY
WHERE CONVERSATION_MONTH >= '2025-01-01'
GROUP BY TENANT_NAME;
```

### Identifier Case Sensitivity
**CRITICAL**: Column names in ANALYTICS tables are **UPPERCASE** (Snowflake default). Always use uppercase in SQL queries:

```sql
-- ✅ Correct
SELECT TENANT_NAME, TOTAL_CONVERSATIONS
FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;

-- ❌ Wrong (will fail with "invalid identifier")
SELECT tenant_name, total_conversations
FROM ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;
```

**Why**: dbt models output with uppercase column names (Snowflake default for unquoted identifiers). Source tables (AVA.PUBLIC.ACCESSAVA) use quoted lowercase from ETL, but marts normalize to uppercase.

---

## 6. Troubleshooting

### "Invalid username or password"
- **Check**: Private key file path is correct and accessible
- **Verify**: Environment variables are set (restart terminal after setting)
- **Test**: `echo $SNOWFLAKE_TENANT_REPORTS_KEY_FILE` (Linux/Mac) or `$env:SNOWFLAKE_TENANT_REPORTS_KEY_FILE` (PowerShell)

### "Object does not exist: MART_*"
- **Check**: Database and schema are correct (`ANALYTICS.PUBLIC`, not `AVA.PUBLIC`)
- **Verify**: `USE ANALYTICS.PUBLIC;` before querying
- **Test**: `SHOW TABLES IN SCHEMA ANALYTICS.PUBLIC;`

### "Invalid identifier 'tenant_name'"
- **Fix**: Use uppercase column names (`TENANT_NAME` not `tenant_name`)
- **Reason**: Snowflake stores unquoted identifiers as uppercase
- **Check**: `DESC TABLE ANALYTICS.PUBLIC.MART_CHATBOT_CONVERSATIONS_BY_TENANT_TOTAL;`

### "Insufficient privileges"
- **Role issue**: Ensure you're using `ROLE_TENANT_REPORTS_READ` role
- **Check**: `SELECT CURRENT_ROLE();` should return `ROLE_TENANT_REPORTS_READ`
- **Fix**: Add `USE ROLE ROLE_TENANT_REPORTS_READ;` to your connection setup

### "Error reading private key"
- **Format**: Private key must be PKCS#8 format (`.p8` file)
- **Permissions**: File should be readable (`chmod 600` on Linux/Mac)
- **Encoding**: Should be PEM format (text file starting with `-----BEGIN PRIVATE KEY-----`)

---

## 7. Getting Help

**Data dictionary and lineage**: http://data.accesscharity.org.uk:8082 (dbt docs UI - requires VPN/tunnel)

**Snowflake Docs**: https://docs.snowflake.com/en/user-guide/python-connector.html

**Questions**: Contact data team (Amit) via Slack or amit@accesscharity.org.uk

**Report bugs**: Create issue in [dbt-asc GitHub repo](https://github.com/access-social-care/dbt-asc)

---

## 8. Comparison to Power BI Access

| Feature | Power BI User | Developer (TENANT_REPORTS_USER) |
|---------|---------------|----------------------------------|
| **Role** | `ROLE_PBI_READ` | `ROLE_TENANT_REPORTS_READ` |
| **Access** | ANALYTICS.PUBLIC (read-only) | ANALYTICS.PUBLIC (read-only) |
| **Auth** | Username/password | RSA key pair (no password) |
| **Use Case** | Interactive dashboards | Programmatic queries, report generation, APIs |
| **Credentials** | Individual user account | Shared service account |
| **Tools** | Power BI Desktop, Power BI Service | Python, R, Node.js, Java, SQL clients |

Both roles have equivalent **read permissions** - only authentication method differs.

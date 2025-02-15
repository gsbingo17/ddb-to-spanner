# DynamoDB Mock Data Generator

This script generates mock data with various data types and writes it to a DynamoDB table.

## Features

- Generates mock data with various field types:
  - String
  - Float
  - Integer
  - Boolean
  - Timestamp
  - JSON objects
  - Complex arrays
- Configurable table name and number of rows
- Batch writing for efficient data insertion
- Progress logging

## Data Structure

The generated mock data includes the following fields:

```javascript
{
  "id": "1",                    // String (Primary Key)
  "stringField": "test_abc123", // String
  "floatField": 123.45,         // Float
  "integerField": 42,           // Integer
  "booleanField": true,         // Boolean
  "timestampField": "2023-01-01T12:00:00Z", // ISO Timestamp
  "jsonField": {                // Nested JSON
    "id": "id_xyz789",
    "count": 50,
    "enabled": true,
    "metadata": {
      "created": "2023-01-01T10:00:00Z",
      "modified": "2023-01-02T15:30:00Z",
      "version": 2.5
    }
  },
  "arrayField": [              // Complex Array
    {
      "a": "string1_abc",
      "b": "value1_xyz"
    },
    {
      "a": "string2_def",
      "b": "value2_uvw"
    }
  ],
  "tags": ["tag1", "tag2"]     // Simple Array
}
```

## Prerequisites

- Node.js (version 14 or later recommended)
- AWS account with access to DynamoDB
- AWS credentials configured (see [AWS Credentials Configuration](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/setting-credentials-node.html))

## Installation

1. Install dependencies:

   ```bash
   npm install @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
   ```

## Configuration

Edit the config object in the script to customize:

```javascript
const config = {
  tableName: 'mock-data-table', // Your DynamoDB table name
  rowCount: 100,                // Number of rows to generate
  region: 'us-west-2'           // Your AWS region
};
```

## Creating the DynamoDB Table

Before running the generator, create the DynamoDB table using AWS CLI:

```bash
aws dynamodb create-table \
    --table-name mock-data-table \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --region us-west-2 \
    --endpoint-url http://localhost:32331
```

```bash
aws dynamodb create-table \
    --table-name mock-data-table \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --region us-west-2 \
    --endpoint-url http://localhost:32331
```

This command:
- Creates a table named 'mock-data-table'
- Sets 'id' as the primary key (String type)
- Configures provisioned throughput with 5 read and write capacity units

To verify the table was created:
```bash
aws dynamodb describe-table \
    --table-name mock-data-table \
    --region us-west-2 \
    --endpoint-url http://localhost:32331
```

Wait until the table status is "ACTIVE" before proceeding.

## Creating the Cloud Spanner Table

Create the target Cloud Spanner table using the following DDL:

```sql
CREATE TABLE mock_data_table (
  id STRING(36) NOT NULL,
  stringField STRING(MAX),
  floatField FLOAT64,
  integerField INT64,
  booleanField BOOL,
  timestampField TIMESTAMP,
  jsonField JSON,  -- For nested JSON objects
  arrayField JSON, -- For complex arrays
  tags JSON,       -- For simple arrays
) PRIMARY KEY(id);
```

You can execute this DDL using the gcloud CLI:

```bash
# Create the DDL file
cat > create_table.sql << EOL
CREATE TABLE mock_data_table (
  id STRING(36) NOT NULL,
  stringField STRING(MAX),
  floatField FLOAT64,
  integerField INT64,
  booleanField BOOL,
  timestampField TIMESTAMP,
  jsonField JSON,
  arrayField JSON,
  tags JSON,
) PRIMARY KEY(id);
EOL

# Execute the DDL
gcloud spanner databases ddl update your-database-id \
    --instance=your-instance-id \
    --project=your-project-id \
    --ddl-file=create_table.sql

# Verify the table was created
gcloud spanner databases ddl describe your-database-id \
    --instance=your-instance-id \
    --project=your-project-id
```

Note on data type mappings from DynamoDB to Spanner:
- String → STRING(MAX)
- Number (integer) → INT64
- Number (float) → FLOAT64
- Boolean → BOOL
- Timestamp → TIMESTAMP
- JSON objects → JSON
- Arrays → JSON
- Complex objects → JSON

## Usage

1. Run the script:

   ```bash
   node generate-mock-data.js
   ```

The script will:
- Generate the specified number of mock data items
- Write them to DynamoDB in batches of 25 items (DynamoDB batch write limit)
- Log progress and any errors

## Notes

- The script uses batch writing to optimize performance
- Each batch write operation processes up to 25 items (DynamoDB limit)
- Progress is logged for each batch
- The script includes error handling for batch write operations

## Verifying Generated Data

To view the generated data, use the AWS CLI scan command:

```bash
# Basic scan to view all items
aws dynamodb scan \
    --table-name mock-data-table \
    --region us-west-2 \
    --endpoint-url http://localhost:32331

# Scan with specific attributes
aws dynamodb scan \
    --table-name mock-data-table \
    --attributes-to-get id stringField arrayField \
    --region us-west-2 \
    --endpoint-url http://localhost:32331

# Scan with limit
aws dynamodb scan \
    --table-name mock-data-table \
    --limit 5 \
    --region us-west-2 \
    --endpoint-url http://localhost:32331

# Scan with filter expression
aws dynamodb scan \
    --table-name mock-data-table \
    --filter-expression "integerField > :val" \
    --expression-attribute-values '{":val": {"N":"50"}}' \
    --region us-west-2 \
    --endpoint-url http://localhost:32331
```

These commands allow you to:
- View all generated items
- Select specific fields to view
- Limit the number of returned items
- Filter items based on field values

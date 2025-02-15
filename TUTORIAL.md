# DynamoDB to Cloud Spanner Replication Tool Tutorial

This tutorial provides a step-by-step guide for setting up and using the DynamoDB to Cloud Spanner replication tool. The tool supports both one-time migration and continuous live replication of data.

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Step-by-Step Setup](#step-by-step-setup)
5. [Testing with Mock Data](#testing-with-mock-data)
6. [Running the Replication](#running-the-replication)
7. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## System Architecture

The replication tool consists of two modes:

1. **One-time Migration**: Handles one-time migration of existing data
2. **Live Migration**: Manages continuous replication using DynamoDB Streams

![Architecture Overview]
```
DynamoDB ─── One-time migration ─┐
    │                            ↓
    └── Live Migration  ─── Cloud Spanner
```

## Prerequisites

1. **Local Development Setup**:
   
   Option 1 - Using Homebrew (macOS):
   ```bash
   # Install DynamoDB Local
   brew install dynamodb-local
   
   # Start DynamoDB Local
   dynamodb-local -sharedDb
   
   # Or start with a specific port
   dynamodb-local -port 32331 -sharedDb
   ```
   
   Option 2 - Using Docker:
   ```bash
   # Pull and run DynamoDB Local
   docker pull amazon/dynamodb-local
   docker run -p 8000:8000 amazon/dynamodb-local
   
   # Or use a specific port (e.g., 32331 as shown in the example config)
   docker run -p 32331:8000 amazon/dynamodb-local
   ```

   AWS CLI Setup:
   ```bash
   # Install AWS CLI

   # For macOS (using Homebrew)
   brew install awscli

   # For Linux (using package manager)
   # Ubuntu/Debian
   sudo apt-get update && sudo apt-get install awscli
   # RHEL/CentOS/Fedora
   sudo yum install awscli

   # For Windows
   # Download and run the official AWS CLI MSI installer:
   # https://awscli.amazonaws.com/AWSCLIV2.msi

   # Verify installation
   aws --version
   ```

   AWS Configuration for Local Development:
   ```bash
   # Configure AWS CLI with dummy credentials for local development
   aws configure set aws_access_key_id "dummy" --profile dynamodb-local
   aws configure set aws_secret_access_key "dummy" --profile dynamodb-local
   aws configure set region "us-west-2" --profile dynamodb-local

   # Use these credentials when running commands
   export AWS_PROFILE=dynamodb-local
   
   # Or specify the profile in each command
   aws dynamodb list-tables --profile dynamodb-local --endpoint-url http://localhost:32331
   ```

2. **AWS Setup** (for production):
   - AWS account with DynamoDB access
   - AWS credentials configured locally
   ```bash
   aws configure
   # Enter your AWS Access Key ID
   # Enter your AWS Secret Access Key
   # Enter your default region (e.g., us-west-2)
   ```

2. **Google Cloud Setup**:
   - Google Cloud project with Cloud Spanner enabled
   - Service account with Spanner access
   ```bash
   # Download your service account key file
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

3. **Node.js Environment**:
   - Node.js version 14 or later
   ```bash
   node --version  # Should be ≥ 14.0.0
   ```

## Installation

1. Clone or create your project directory:
   ```bash
   git clone https://github.com/gsbingo17/ddb-to-spanner.git
   mkdir ddb-to-spanner
   cd ddb-to-spanner
   ```

2. Install required dependencies:
   ```bash
   npm install 
   ```

## Step-by-Step Setup

### 1. Create DynamoDB Table

For local development:
```bash
# Create table in DynamoDB Local (using dynamodb-local profile)
aws dynamodb create-table \
    --table-name your-table-name \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --endpoint-url http://localhost:32331 \
    --region us-west-2 \
    --profile dynamodb-local

# Verify table creation
aws dynamodb describe-table \
    --table-name your-table-name \
    --endpoint-url http://localhost:32331 \
    --region us-west-2 \
    --profile dynamodb-local
```

For production:
```bash
# Create table in AWS DynamoDB
aws dynamodb create-table \
    --table-name your-table-name \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --region us-west-2
```

### 2. Create Cloud Spanner Table

```sql
CREATE TABLE your_table_name (
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
```

Execute using gcloud:
```bash
gcloud spanner databases ddl update your-database \
    --instance=your-instance \
    --project=your-project \
    --ddl-file=create_table.sql
```

### 3. Configure Replication

Create `replication_config.json`:

For local development:
```json
{
  "databasePairs": [
    {
      "source": {
        "region": "us-west-2",
        "tableName": "your-table-name",
        "endpoint": "http://localhost:32331"  // DynamoDB Local endpoint
      },
      "target": {
        "projectId": "your-project-id",
        "instanceId": "your-instance-id",
        "databaseId": "your-database",
        "tableName": "your_table_name"
      }
    }
  ],
  "checkpointFrequency": 10,
  "batchSize": 1000,
  "maxRetries": 3,
  "retryDelayMs": 1000
}
```

For production:
```json
{
  "databasePairs": [
    {
      "source": {
        "region": "us-west-2",
        "tableName": "your-table-name"
      },
      "target": {
        "projectId": "your-project-id",
        "instanceId": "your-instance-id",
        "databaseId": "your-database",
        "tableName": "your_table_name"
      }
    }
  ],
  "checkpointFrequency": 10,
  "batchSize": 1000,
  "maxRetries": 3,
  "retryDelayMs": 1000
}
```

## Testing with Mock Data

### 1. Generate Test Data

The included mock data generator creates sample data with various field types:

1. Configure generator settings in `generate-mock-data.js`:
   ```javascript
   const config = {
     tableName: 'your-table-name',
     rowCount: 100,
     region: 'us-west-2'
   };
   ```

2. Run the generator:
   ```bash
   node generate-mock-data.js
   ```

### 2. Verify and Test Data

#### Option 1: Using NoSQL Workbench

1. Install NoSQL Workbench:
   - Download from [AWS NoSQL Workbench](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/workbench.settingup.html)
   - Available for Windows, macOS, and Linux

2. Connect to DynamoDB Local:
   - Open NoSQL Workbench
   - Click "Operation Builder"
   - Add Connection → DynamoDB Local
   - Connection settings:
     ```
     Port: 32331 (or your configured port)
     Region: us-west-2
     ```

3. Test CRUD Operations:
   - Create: Add new items using the visual builder
   - Read: Query data using various conditions
   - Update: Modify existing items
   - Delete: Remove items from the table

#### Option 2: Using AWS CLI

Check DynamoDB Local:
```bash
# For local development (using dynamodb-local profile)
aws dynamodb scan \
    --table-name your-table-name \
    --limit 5 \
    --endpoint-url http://localhost:32331 \
    --region us-west-2 \
    --profile dynamodb-local

# Additional useful commands for local development
aws dynamodb list-tables \
    --endpoint-url http://localhost:32331 \
    --region us-west-2 \
    --profile dynamodb-local

aws dynamodb describe-table \
    --table-name your-table-name \
    --endpoint-url http://localhost:32331 \
    --region us-west-2 \
    --profile dynamodb-local
```

For production:
```bash
aws dynamodb scan \
    --table-name your-table-name \
    --limit 5 \
    --region us-west-2
```

## Running the Replication

### 1. One-Time Migration

For a one-time data migration:
```bash
node index.js migrate
or
node index.js
```

### 2. Live Replication

For continuous replication:
```bash
node index.js live
```

### 3. Parallel Processing (Optional)

For large tables, use parallel processing:
```bash
# Terminal 1
node index.js migrate --segment 0 --totalSegments 4

# Terminal 2
node index.js migrate --segment 1 --totalSegments 4

# Terminal 3
node index.js migrate --segment 2 --totalSegments 4

# Terminal 4
node index.js migrate --segment 3 --totalSegments 4
```

## Monitoring and Troubleshooting

### 1. Monitor Progress

The tool provides progress information through logs:
- Batch processing status
- Current checkpoint timestamps
- Error details if any

### 2. Check Checkpoints

View the current checkpoint:
```bash
cat checkpoint.json
```

Example checkpoint file:
```json
{
  "checkpoints": {
    "your-table-name": 1708020000000
  }
}
```

### 3. Common Issues and Solutions

1. **Connection Issues**:
   - Verify AWS credentials
   - Check Google Cloud service account permissions
   - Ensure network connectivity to both services

2. **Stream Processing Errors**:
   - Verify DynamoDB Streams is enabled
   - Check shard iterator expiration
   - Monitor AWS rate limits

3. **Data Type Mismatches**:
   - Ensure Spanner schema matches DynamoDB data types
   - Check JSON field handling
   - Verify timestamp formats

4. **Performance Issues**:
   - Adjust batchSize in config
   - Consider parallel processing
   - Monitor resource utilization

## Best Practices

1. **Development Workflow**:
   - Use DynamoDB Local for development and testing
   - Test replication with small datasets locally first
   - Verify all operations work locally before moving to production

2. **Before Production Deployment**:
   - Backup source and target databases
   - Test with small data subset in production
   - Verify schema compatibility

2. **During Replication**:
   - Monitor system resources
   - Check error logs regularly
   - Verify data consistency

3. **After Completion**:
   - Validate record counts
   - Check data integrity
   - Monitor for any lag in live replication

## Data Type Mapping Reference

| DynamoDB Type | Cloud Spanner Type |
|---------------|--------------------|
| String        | STRING(MAX)        |
| Number (int)  | INT64              |
| Number (float)| FLOAT64            |
| Boolean       | BOOL               |
| Map/Object    | JSON               |
| List/Array    | JSON               |
| Null          | NULL               |

This mapping ensures proper data type conversion during replication while maintaining data integrity.

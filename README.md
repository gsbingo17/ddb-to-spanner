# DynamoDB to Cloud Spanner Replication

This script replicates data from a DynamoDB table to a Cloud Spanner table. It supports two modes of operation:

- **Migrate:** Performs a one-time migration of data from DynamoDB to Cloud Spanner.
- **Live:** Sets up a live replication using DynamoDB Streams to continuously synchronize data between the two databases. This includes:
    - **Initial Migration:** A one-time migration of existing data from DynamoDB to Cloud Spanner.
    - **Incremental Replication:** Uses DynamoDB Streams to continuously synchronize data between the two databases, replicating any new changes made after the initial migration.

## Prerequisites

- Node.js (version 14 or later recommended)
- AWS account with access to DynamoDB
- DynamoDB table with Streams enabled (for live replication)
- Google Cloud project with Cloud Spanner enabled
- AWS credentials configured (see [AWS Credentials Configuration](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/setting-credentials-node.html))
- Google Cloud service account with permissions to access Cloud Spanner (see [Creating Service Accounts](https://cloud.google.com/iam/docs/creating-managing-service-accounts))

## Installation

1. Install dependencies:

   ```bash
   npm install @aws-sdk/client-dynamodb @aws-sdk/client-dynamodb-streams @aws-sdk/lib-dynamodb @google-cloud/spanner
   ```

## Configuration

Create a replication_config.json file: This file defines the replication settings, including the DynamoDB and Cloud Spanner connection details. Here's an example:

```json
{
  "databasePairs": [
    {
      "source": {
        "region": "us-west-2",
        "tableName": "sourceTable"
      },
      "target": {
        "projectId": "your-project-id",
        "instanceId": "your-instance-id",
        "databaseId": "your-database",
        "tableName": "targetTable"
      }
    }
  ],
  "checkpointFrequency": 10,
  "batchSize": 1000,
  "maxRetries": 3,
  "retryDelayMs": 1000
}
```

Configuration parameters:
- **Global Settings**
  - `batchSize`: Number of items to process in each batch write operation (default: 1000)
  - `checkpointFrequency`: Number of records to process before saving a checkpoint (default: 100)
  - `maxRetries`: Maximum number of retry attempts for failed transactions (default: 3)
  - `retryDelayMs`: Base delay in milliseconds between retries (default: 1000)

- **source**: DynamoDB configuration
  - `region`: AWS region where your DynamoDB table is located
  - `tableName`: Name of the source DynamoDB table
- **target**: Cloud Spanner configuration
  - `projectId`: Your Google Cloud project ID
  - `instanceId`: Your Cloud Spanner instance ID
  - `databaseId`: Your Cloud Spanner database ID
  - `tableName`: Name of the target Cloud Spanner table

## Checkpoint System

The script uses a timestamp-based checkpoint system to track replication progress:

```json
{
  "checkpoints": {
    "sourceTableName": 1708020000000  // Timestamp in milliseconds
  }
}
```

- Each table's checkpoint is stored as a Unix timestamp in milliseconds
- The checkpoint represents the point in time up to which records have been processed
- When restarting replication, records are filtered based on their ApproximateCreationDateTime
- Records with timestamps older than the checkpoint are skipped
- Checkpoints are updated periodically based on the checkpointFrequency setting

## Live Replication Process

1. **Initial Setup**
   - Gets current timestamp as the starting point
   - Performs initial data migration
   - Starts stream processing from the saved timestamp

2. **Stream Processing**
   - Uses TRIM_HORIZON to read all available records
   - Filters records based on their ApproximateCreationDateTime
   - Only processes records newer than the checkpoint timestamp
   - Handles INSERT, MODIFY, and REMOVE operations

3. **Checkpoint Management**
   - Saves checkpoints after processing batches of records
   - Uses atomic file operations to ensure checkpoint integrity
   - Automatically resumes from last checkpoint after interruption

## Data Type Conversions

The script handles the following data type conversions from DynamoDB to Cloud Spanner:

1. **Complex Types**
   - Objects and arrays are converted to JSON strings:
     ```javascript
     // DynamoDB
     { 
       "data": { "key": "value" },
       "tags": ["tag1", "tag2"]
     }
     // Spanner
     data: '{"key":"value"}',
     tags: '["tag1","tag2"]'
     ```

2. **Basic Types**
   - Strings/Numbers/Booleans are preserved as-is
   - Null/undefined values are properly handled

## Error Handling and Retries

The script includes robust error handling:

1. **Transaction Retries**
   - Failed transactions are retried up to maxRetries times
   - Exponential backoff between retries (retryDelayMs * 2^attempt)
   - Special handling for ALREADY_EXISTS errors

2. **Stream Processing**
   - Handles expired shard iterators
   - Continues processing other shards if one fails
   - Rate limiting to avoid hitting DynamoDB Streams limits

3. **Checkpoint Safety**
   - Uses temporary files for atomic checkpoint updates
   - Cleans up temporary files on error
   - Validates checkpoint data on load

## Usage

1. Migrate Mode:

   To perform a one-time migration of data from DynamoDB to Cloud Spanner:

   ```bash
   node index.js migrate
   ```

2. Live Mode:

   To set up live replication using DynamoDB Streams:

   ```bash
   node index.js live
   ```

   The script will continuously listen for changes in the specified DynamoDB table and replicate them to Cloud Spanner.

## Notes

* For live replication, ensure DynamoDB Streams is enabled on your source table
* The script uses DynamoDB's DocumentClient for simplified interaction with DynamoDB
* Cloud Spanner operations use writeAtLeastOnce for efficient batch processing
* Progress logging shows batch processing status and timestamp filtering details
* For large tables:
  - The migration process uses pagination to handle the data in chunks
  - Optional parallel scanning for improved performance:
    ```bash
    # Run multiple processes with different segment values
    node index.js migrate --segment 0 --totalSegments 4  # Process 1
    node index.js migrate --segment 1 --totalSegments 4  # Process 2
    node index.js migrate --segment 2 --totalSegments 4  # Process 3
    node index.js migrate --segment 3 --totalSegments 4  # Process 4
    ```
  - Parallel scanning is disabled by default
  - Enable parallel scanning by providing --segment and --totalSegments arguments
  - Each process scans a different segment of the table in parallel

For more information about Cloud Spanner operations, see:
https://googleapis.dev/nodejs/spanner/latest/index.html

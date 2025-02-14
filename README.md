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

## Data Type Conversions

The script handles the following data type conversions from DynamoDB to Cloud Spanner:

1. **Numbers**
   - Integers within INT64 range (-9223372036854775808 to 9223372036854775807):
     ```javascript
     // DynamoDB
     { "count": 42 }
     // Spanner
     count: "42" // INT64 value in Spanner after type detection
     ```
   - Integers outside INT64 range or floating-point numbers:
     ```javascript
     // DynamoDB
     { "price": 99.99, "bigNum": 9223372036854775808 }
     // Spanner
     price: "99.99", // FLOAT64 value in Spanner after type detection
     bigNum: "9.223372036854776e+18" // FLOAT64 value in Spanner after overflow detection
     ```
   - Numbers stored as strings in DynamoDB are properly parsed and converted
   - Automatic overflow detection and conversion to FLOAT64 when needed

2. **Complex Types**
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

3. **Other Types**
   - Booleans are preserved as-is
   - Strings are preserved as-is
   - Null/undefined values are properly handled

The script includes robust type checking and validation to ensure data integrity during conversion.

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
* Checkpoint frequency can be adjusted in replication_config.json:
  ```json
  {
    "checkpointFrequency": 100,  // Save checkpoint every 100 records
    "batchSize": 1000,           // Process records in batches of 1000
    "maxRetries": 3,             // Retry failed transactions up to 3 times
    "retryDelayMs": 1000         // Wait 1 second between retries (doubles on each retry)
  }
  ```
* Checkpoints are stored in checkpoint.json and are used to resume replication from the last saved position
* If the replication process is interrupted, it will automatically resume from the last checkpoint
* The script uses DynamoDB's DocumentClient for simplified interaction with DynamoDB
* Cloud Spanner operations use Table.insert() and Table.delete() methods for efficient batch processing (controlled by batchSize in config)
* Numbers are automatically detected as INT64 or FLOAT64 and converted appropriately
* Complex data types (arrays, objects) are automatically converted to JSON strings
* Progress logging shows batch processing status
* The script includes rate limiting to avoid hitting DynamoDB Streams read limits
* Error handling is implemented for both DynamoDB and Cloud Spanner operations
* For large tables, the migration process uses pagination to handle the data in chunks

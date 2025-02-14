import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDBStreams, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand } from "@aws-sdk/client-dynamodb-streams";
import { Spanner } from '@google-cloud/spanner';
import fs from 'fs/promises';
import path from 'path';

// Checkpoint file path
const CHECKPOINT_FILE = 'checkpoint.json';

// Function to load checkpoint data
async function loadCheckpoint(tableName) {
  try {
    const checkpointData = await fs.readFile(CHECKPOINT_FILE, 'utf8');
    const checkpoint = JSON.parse(checkpointData);
    return checkpoint.checkpoints[tableName] || {};
  } catch (error) {
    if (error.code === 'ENOENT') {
      // Create checkpoint file if it doesn't exist
      await fs.writeFile(CHECKPOINT_FILE, JSON.stringify({ checkpoints: {} }, null, 2));
      return {};
    }
    throw error;
  }
}

// Function to save checkpoint data
async function saveCheckpoint(tableName, shardId, sequenceNumber) {
  const tempFile = `${CHECKPOINT_FILE}.tmp`;
  try {
    let checkpoint = { checkpoints: {} };
    
    try {
      const checkpointData = await fs.readFile(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(checkpointData);
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
      // File doesn't exist, use default empty checkpoint
    }
    
    if (!checkpoint.checkpoints[tableName]) {
      checkpoint.checkpoints[tableName] = {};
    }
    
    checkpoint.checkpoints[tableName][shardId] = sequenceNumber;
    
    // Write to temporary file first
    await fs.writeFile(tempFile, JSON.stringify(checkpoint, null, 2));
    
    // Atomically rename temp file to actual checkpoint file
    await fs.rename(tempFile, CHECKPOINT_FILE);
    
    console.log(`Saved checkpoint for table ${tableName}, shard ${shardId}: ${sequenceNumber}`);
  } catch (error) {
    // Clean up temp file if it exists
    try {
      await fs.unlink(tempFile);
    } catch (unlinkError) {
      // Ignore error if temp file doesn't exist
    }
    console.error('Error saving checkpoint:', error);
    throw error;
  }
}

// Default configuration values
const DEFAULT_CONFIG = {
  batchSize: 1000,
  checkpointFrequency: 100, // Save checkpoint every N records
  maxRetries: 3,
  retryDelayMs: 1000
};

// Load replication configuration from JSON file
const userConfig = JSON.parse(await fs.readFile('replication_config.json', 'utf8'));
const config = { ...DEFAULT_CONFIG, ...userConfig };

// Function to convert DynamoDB number to Spanner type
function convertDynamoDBNumberToSpanner(dynamoDBNumber) {
  // 1. Check if the input is already null or undefined
  if (dynamoDBNumber == null) {
    return null;
  }

  // 2. Type Check
  if (typeof dynamoDBNumber === 'number') { // Already a JavaScript number
    if (Number.isInteger(dynamoDBNumber)) {
      // 3a. Integer: Convert to INT64 if within range
      if (dynamoDBNumber >= -9223372036854775808 && dynamoDBNumber <= 9223372036854775807) {
        return { value: String(dynamoDBNumber), type: 'INT64' };
      } else {
        // Handle overflow: Convert to FLOAT64
        console.warn(`Integer overflow: ${dynamoDBNumber}. Converting to FLOAT64.`);
        return { value: dynamoDBNumber.toString(), type: 'FLOAT64' };
      }
    } else {
      // 3b. Floating-point: Convert to FLOAT64
      return { value: dynamoDBNumber.toString(), type: 'FLOAT64' };
    }
  } else if (typeof dynamoDBNumber === 'string') { // DynamoDB might store numbers as strings
    const num = Number(dynamoDBNumber);
    if (isNaN(num)) {
      throw new Error(`Invalid number string: ${dynamoDBNumber}`);
    }
    return convertDynamoDBNumberToSpanner(num);
  } else {
    throw new Error(`Unexpected DynamoDB Number type: ${typeof dynamoDBNumber}.`);
  }
}

// Function to convert DynamoDB types to Spanner types
function convertToSpannerType(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'number' || (typeof value === 'string' && !isNaN(Number(value)))) {
    return convertDynamoDBNumberToSpanner(value);
  }

  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value);
  }

  if (typeof value === 'boolean') {
    return value;
  }

  return String(value);
}

// Function to convert a DynamoDB item to Spanner row
function convertItemToRow(item) {
  const row = {};
  for (const [key, value] of Object.entries(item)) {
    row[key] = convertToSpannerType(value);
  }
  return row;
}

// Function to perform one-time migration for a table
async function migrateTable(ddbClient, spannerTable, tableConfig) {
  try {
    console.log(`Migrating table: ${tableConfig.source.tableName} to ${tableConfig.target.tableName}`);

    let lastEvaluatedKey;
    do {
      const params = {
        TableName: tableConfig.source.tableName,
        ...(lastEvaluatedKey && { ExclusiveStartKey: lastEvaluatedKey })
      };

      const { Items, LastEvaluatedKey } = await ddbClient.scan(params);
      lastEvaluatedKey = LastEvaluatedKey;

      if (Items && Items.length > 0) {
        // Process items in batches
        const batchSize = config.batchSize || 1000; // Use config value or default to 1000
        for (let i = 0; i < Items.length; i += batchSize) {
          const batch = Items.slice(i, i + batchSize);
          
          // console.log('Original DynamoDB items:', JSON.stringify(batch, null, 2));

          // Convert DynamoDB items to Spanner rows
          const rows = batch.map(convertItemToRow);
          // console.log('Converted rows with types:', JSON.stringify(rows, null, 2));

          try {
            // Insert batch of rows into Spanner with proper types
            const formattedRows = rows.map(row => {
              const formattedRow = {};
              for (const [key, value] of Object.entries(row)) {
                if (value && value.type && value.value) {
                  // Handle typed values (numbers)
                  if (value.type === 'FLOAT64') {
                    formattedRow[key] = Number(value.value);
                  } else {
                    formattedRow[key] = value.value;
                  }
                } else {
                  // Handle other values
                  formattedRow[key] = value;
                }
              }
              return formattedRow;
            });

            // console.log('Final formatted rows for Spanner:', JSON.stringify(formattedRows, null, 2));
            await spannerTable.insert(formattedRows);
            console.log(`Processed batch of ${rows.length} items`);
          } catch (error) {
            console.error(`Error inserting batch starting at item ${i}:`, error);
            throw error;
          }
        }
      }
    } while (lastEvaluatedKey);

    console.log(`Migration for ${tableConfig.source.tableName} completed successfully!`);
  } catch (error) {
    console.error(`Error migrating table ${tableConfig.source.tableName}:`, error);
    throw error;
  }
}

// Function to get initial sequence number for live replication
async function getInitialSequenceNumber(region, tableName, endpoint) {
  try {
    const ddb = new DynamoDB({ 
      region,
      endpoint
    });
    const { Table } = await ddb.describeTable({ TableName: tableName });
    
    if (!Table.LatestStreamArn) {
      throw new Error('DynamoDB Streams is not enabled on the source table');
    }
    
    const streamClient = new DynamoDBStreams({ 
      region,
      endpoint
    });
    const { Shards } = await streamClient.describeStream({ StreamArn: Table.LatestStreamArn });
    
    if (!Shards || Shards.length === 0) {
      return null;
    }
    
    // Get the oldest shard's iterator
    const { ShardIterator } = await streamClient.getShardIterator({
      StreamArn: Table.LatestStreamArn,
      ShardId: Shards[0].ShardId,
      ShardIteratorType: 'TRIM_HORIZON'
    });
    
    // Get the first record's sequence number
    const { Records } = await streamClient.getRecords({
      ShardIterator: ShardIterator,
      Limit: 1
    });
    
    return Records && Records.length > 0 ? Records[0].dynamodb.SequenceNumber : null;
  } catch (error) {
    console.error('Error getting initial sequence number:', error);
    throw error;
  }
}

// Function to start live replication using DynamoDB Streams
async function startLiveReplication(ddbClient, spannerTable, tableConfig, startSequenceNumber = null) {
  try {
    console.log(`Starting live replication for: ${tableConfig.source.tableName} to ${tableConfig.target.tableName}`);

    // Get the stream ARN
    const ddb = new DynamoDB({ 
      region: tableConfig.source.region,
      endpoint: tableConfig.source.endpoint
    });
    const { Table } = await ddb.describeTable({ TableName: tableConfig.source.tableName });
    
    if (!Table.LatestStreamArn) {
      throw new Error('DynamoDB Streams is not enabled on the source table');
    }

    const streamArn = Table.LatestStreamArn;
    const streamClient = new DynamoDBStreams({ 
      region: tableConfig.source.region,
      endpoint: tableConfig.source.endpoint
    });

    // Get stream shards
    const { Shards } = await streamClient.describeStream({ StreamArn: streamArn });

    // Load checkpoints for this table
    const checkpoints = await loadCheckpoint(tableConfig.source.tableName);
    console.log('Loaded checkpoints:', checkpoints);

    // Process each shard
    for (const shard of Shards) {
      // Use checkpoint if available, otherwise use startSequenceNumber
      const checkpointSequenceNumber = checkpoints[shard.ShardId];
      const sequenceNumber = checkpointSequenceNumber || startSequenceNumber;
      
      if (checkpointSequenceNumber) {
        console.log(`Resuming shard ${shard.ShardId} from checkpoint: ${checkpointSequenceNumber}`);
      }
      
      processStreamRecords(
        streamClient,
        spannerTable,
        tableConfig,
        streamArn,
        shard.ShardId,
        sequenceNumber
      );
    }
  } catch (error) {
    console.error(`Error starting live replication for ${tableConfig.source.tableName}:`, error);
    throw error;
  }
}

async function processStreamRecords(streamClient, spannerTable, tableConfig, streamArn, shardId, startSequenceNumber = null) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 1000;

  try {
    // Determine iterator type and sequence number
    let iteratorType = 'TRIM_HORIZON';
    let sequenceNumber = null;

    if (startSequenceNumber) {
      iteratorType = 'AT_SEQUENCE_NUMBER';
      sequenceNumber = startSequenceNumber;
    }

    const iteratorParams = {
      StreamArn: streamArn,
      ShardId: shardId,
      ShardIteratorType: iteratorType,
      ...(sequenceNumber && { SequenceNumber: sequenceNumber })
    };

    console.log(`Starting stream processing for shard ${shardId} with iterator type ${iteratorType}${sequenceNumber ? ` and sequence number ${sequenceNumber}` : ''}`);

    const { ShardIterator } = await streamClient.getShardIterator(iteratorParams);
    let currentShardIterator = ShardIterator;

    while (currentShardIterator) {
      const { Records, NextShardIterator } = await streamClient.getRecords({
        ShardIterator: currentShardIterator
      });

      if (Records && Records.length > 0) {
        let processedRecords = 0;
        const totalRecords = Records.length;
        const lastSequenceNumber = Records[Records.length - 1].dynamodb.SequenceNumber;
        // Process records in transaction batches
        const batchSize = config.batchSize || 1000;
        const recordBatches = [];
        
        for (let i = 0; i < Records.length; i += batchSize) {
          recordBatches.push(Records.slice(i, i + batchSize));
        }

        for (const batch of recordBatches) {
          let retries = 0;
          let success = false;

          while (!success && retries < MAX_RETRIES) {
            try {
              await spannerTable.database.runTransaction(async (transaction) => {
                for (const record of batch) {
                  switch (record.eventName) {
                    case 'INSERT':
                    case 'MODIFY': {
                      const row = convertItemToRow(record.dynamodb.NewImage);
                      const formattedRow = {};
                      for (const [key, value] of Object.entries(row)) {
                        if (value && value.type && value.value) {
                          if (value.type === 'FLOAT64') {
                            formattedRow[key] = Number(value.value);
                          } else if (value.type === 'INT64') {
                            formattedRow[key] = value.value;
                          } else {
                            formattedRow[key] = value.value;
                          }
                        } else {
                          formattedRow[key] = value;
                        }
                      }
                      transaction.upsert(tableConfig.target.tableName, formattedRow);
                      break;
                    }
                    case 'REMOVE': {
                      const keys = convertItemToRow(record.dynamodb.Keys);
                      const formattedKeys = {};
                      for (const [key, value] of Object.entries(keys)) {
                        if (value && value.type && value.value) {
                          formattedKeys[key] = value.type === 'FLOAT64' ? Number(value.value) : value.value;
                        } else {
                          formattedKeys[key] = value;
                        }
                      }
                      transaction.delete(tableConfig.target.tableName, formattedKeys);
                      break;
                    }
                  }
                }
                await transaction.commit();
                console.log(`Processed batch of ${batch.length} records in transaction`);
              });

              // Update processed records count
              processedRecords += batch.length;

              // Save checkpoint based on frequency configuration
              if (processedRecords >= config.checkpointFrequency || processedRecords >= totalRecords) {
                try {
                  await saveCheckpoint(tableConfig.source.tableName, shardId, lastSequenceNumber);
                  processedRecords = 0; // Reset counter after successful checkpoint
                } catch (checkpointError) {
                  console.warn('Warning: Failed to save checkpoint but transaction was successful:', checkpointError);
                  // Don't throw the error as the transaction was successful
                }
              }
              success = true;
            } catch (error) {
              retries++;
              if (error.code === 6) { // ALREADY_EXISTS
                console.log('Handling duplicate record, continuing...');
                success = true;
              } else if (retries < MAX_RETRIES) {
                console.warn(`Retrying transaction (attempt ${retries}/${MAX_RETRIES}) after error:`, error);
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * Math.pow(2, retries)));
              } else {
                console.error('Max retries reached, failing transaction:', error);
                throw error;
              }
            }
          }
        }
      }

      // Wait before getting more records to avoid hitting rate limits
      await new Promise(resolve => setTimeout(resolve, 1000));
      currentShardIterator = NextShardIterator;
    }
  } catch (error) {
    console.error(`Error processing stream records for shard ${shardId}:`, error);
    // Implement retry logic here
  }
}

// Function to handle database operations based on the chosen mode
async function processDatabase(sourceConfig, targetConfig, mode) {
  try {
    // Initialize DynamoDB DocumentClient
    const ddbClient = DynamoDBDocument.from(new DynamoDB({
      region: sourceConfig.region,
      endpoint: sourceConfig.endpoint,
    }));

    // Initialize Spanner
    const spanner = new Spanner({
      projectId: targetConfig.projectId
    });

    const instance = spanner.instance(targetConfig.instanceId);
    const database = instance.database(targetConfig.databaseId);
    const table = database.table(targetConfig.tableName);

    if (mode === 'migrate') {
      // Only migrate data
      await migrateTable(ddbClient, table, { source: sourceConfig, target: targetConfig });
    } else if (mode === 'live') {
      // Get initial sequence number before migration
      const startSequenceNumber = await getInitialSequenceNumber(
        sourceConfig.region, 
        sourceConfig.tableName,
        sourceConfig.endpoint
      );
      
      // Perform migration
      await migrateTable(ddbClient, table, { source: sourceConfig, target: targetConfig });
      
      // Start live replication from the saved sequence number
      await startLiveReplication(ddbClient, table, { source: sourceConfig, target: targetConfig }, startSequenceNumber);
    } else {
      console.error(`Invalid mode: ${mode}. Please choose either 'migrate' or 'live'.`);
      process.exit(1);
    }
  } catch (error) {
    console.error(`Error processing database:`, error);
    throw error;
  }
}

// Function to start the replication/migration process
async function startProcess(mode) {
  try {
    if (!['migrate', 'live'].includes(mode)) {
      console.error(`Invalid mode: ${mode}. Please choose either 'migrate' or 'live'.`);
      process.exit(1);
    }

    console.log(`Starting process in ${mode} mode for ${config.databasePairs.length} database pairs`);
    
    for (let i = 0; i < config.databasePairs.length; i++) {
      const databasePair = config.databasePairs[i];
      console.log(`\nProcessing database pair ${i + 1} of ${config.databasePairs.length}`);
      console.log(`Source: ${databasePair.source.tableName} in region ${databasePair.source.region}`);
      console.log(`Target: ${databasePair.target.tableName} in database ${databasePair.target.databaseId}`);
      
      await processDatabase(databasePair.source, databasePair.target, mode);
      console.log(`Completed processing database pair ${i + 1}\n`);
    }
    
    console.log('All database pairs have been processed successfully');
  } catch (error) {
    console.error('Error during process:', error);
    process.exit(1);
  }
}

// Get the desired mode from command line arguments
const mode = process.argv[2] || 'migrate'; // Default to 'migrate' if not provided

startProcess(mode);

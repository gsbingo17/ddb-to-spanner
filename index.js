import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDBStreams, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand } from "@aws-sdk/client-dynamodb-streams";
import { DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { Spanner, MutationSet } from '@google-cloud/spanner';
import fs from 'fs/promises';
import path from 'path';

// Checkpoint file path
const CHECKPOINT_FILE = 'checkpoint.json';

// Function to load checkpoint data
async function loadCheckpoint(tableName) {
  try {
    const checkpointData = await fs.readFile(CHECKPOINT_FILE, 'utf8');
    const checkpoint = JSON.parse(checkpointData);
    return checkpoint.checkpoints[tableName] || null;
  } catch (error) {
    if (error.code === 'ENOENT') {
      // Create checkpoint file if it doesn't exist
      await fs.writeFile(CHECKPOINT_FILE, JSON.stringify({ checkpoints: {} }, null, 2));
      return null;
    }
    throw error;
  }
}

// Function to save checkpoint data
async function saveCheckpoint(tableName, timestamp) {
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
    
    checkpoint.checkpoints[tableName] = timestamp;
    
    // Write to temporary file first
    await fs.writeFile(tempFile, JSON.stringify(checkpoint, null, 2));
    
    // Atomically rename temp file to actual checkpoint file
    await fs.rename(tempFile, CHECKPOINT_FILE);
    
    console.log(`Saved checkpoint for table ${tableName}: ${new Date(timestamp).toISOString()}`);
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

// Function to convert DynamoDB types to Spanner types
function convertToSpannerType(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (Array.isArray(value) || typeof value === 'object') {
    return JSON.stringify(value);
  }

  return value;
}

// Function to convert a DynamoDB item to Spanner row
function convertItemToRow(item) {
  const row = {};
  for (const [key, value] of Object.entries(item)) {
    // console.log(`Processing key: ${key}, value: ${JSON.stringify(value, null, 2)}`);
    row[key] = convertToSpannerType(value);
  }
  // console.log(`Converted row: ${JSON.stringify(row)}`);
  return row;
}

// Function to convert a DynamoDB stream event to DynamoDB item
function convertDynamoDBObject(dynamoObject) {
  return unmarshall(dynamoObject);
}

// Function to perform one-time migration for a table
async function migrateTable(ddbClient, spannerTable, tableConfig) {
  try {
    console.log(`Migrating table: ${tableConfig.source.tableName} to ${tableConfig.target.tableName}`);
    if (config.totalSegments) {
      console.log(`Using parallel scan: Segment ${config.segment} of ${config.totalSegments}`);
    }

    let lastEvaluatedKey;
    do {
      const params = {
        TableName: tableConfig.source.tableName,
        ...(config.totalSegments && {
          TotalSegments: config.totalSegments,
          Segment: config.segment
        }),
        ...(lastEvaluatedKey && { ExclusiveStartKey: lastEvaluatedKey })
      };

      const { Items, LastEvaluatedKey } = await ddbClient.scan(params);
      lastEvaluatedKey = LastEvaluatedKey;

      if (Items && Items.length > 0) {
        // Process items in batches
        const batchSize = config.batchSize || 1000; // Use config value or default to 1000
        for (let i = 0; i < Items.length; i += batchSize) {
          const batch = Items.slice(i, i + batchSize);
          
          // Convert DynamoDB items to Spanner rows
          const rows = batch.map(convertItemToRow);
          
          try {
            // Create a new MutationSet for the batch
            const mutations = new MutationSet();
            
            // Add rows to mutation set
            rows.forEach(row => {
              mutations.insert(tableConfig.target.tableName, row);
            });

            // Write mutations using writeAtLeastOnce
            await new Promise((resolve, reject) => {
              spannerTable.database.writeAtLeastOnce(mutations, (err, res) => {
                if (err) {
                  console.error(`Error writing batch starting at item ${i}:`, err);
                  reject(err);
                } else {
                  console.log(`Successfully wrote batch of ${rows.length} items.`);
                  resolve(res);
                }
              });
            });
            
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

// Function to get initial timestamp for live replication
async function getInitialTimestamp(region, tableName, endpoint) {
  try {
    const ddb = new DynamoDB({
      region,
      endpoint,
    });
    const describeTableCommand = new DescribeTableCommand({ TableName: tableName });
    const { Table } = await ddb.send(describeTableCommand);

    if (!Table.LatestStreamArn) {
      throw new Error("DynamoDB Streams is not enabled on the source table");
    }

    // Return current timestamp in milliseconds
    return Date.now();
  } catch (error) {
    console.error("Error getting initial timestamp:", error);
    throw error;
  }
}

// Function to start live replication using DynamoDB Streams
async function startLiveReplication(ddbClient, spannerTable, tableConfig, startTimestamp = null) {
  try {
    console.log(`Starting live replication for: ${tableConfig.source.tableName} to ${tableConfig.target.tableName}`);

    // Get the stream ARN
    const ddb = new DynamoDB({ 
      region: tableConfig.source.region,
      endpoint: tableConfig.source.endpoint
    });
    
    const describeTableCommand = new DescribeTableCommand({ 
      TableName: tableConfig.source.tableName 
    });
    const { Table } = await ddb.send(describeTableCommand);
    
    if (!Table.LatestStreamArn) {
      throw new Error('DynamoDB Streams is not enabled on the source table');
    }

    const streamArn = Table.LatestStreamArn;
    const streamClient = new DynamoDBStreams({ 
      region: tableConfig.source.region,
      endpoint: tableConfig.source.endpoint
    });

    // Get stream shards and process them
    const describeStreamCommand = new DescribeStreamCommand({ 
      StreamArn: streamArn 
    });
    const { StreamDescription } = await streamClient.send(describeStreamCommand);
    const shards = StreamDescription.Shards;

    if (!shards || shards.length === 0) {
      console.log('No shards found in the stream');
      return;
    }

    // Load checkpoint for this table
    const checkpointTimestamp = await loadCheckpoint(tableConfig.source.tableName);
    console.log('Loaded checkpoint:', checkpointTimestamp);

    // Use checkpoint if available, otherwise use startTimestamp
    const timestamp = checkpointTimestamp || startTimestamp || Date.now();
    console.log(`Using timestamp for replication: ${new Date(timestamp).toISOString()}`);

    // Process each shard
    for (const shard of shards) {
      try {
        
        await processStreamRecords(
          streamClient,
          spannerTable,
          tableConfig,
          streamArn,
          shard.ShardId,
          timestamp
        );
      } catch (error) {
        console.error(`Error processing shard ${shard.ShardId}:`, error);
        // Continue with next shard
        continue;
      }
    }
  } catch (error) {
    console.error(`Error starting live replication for ${tableConfig.source.tableName}:`, error);
    throw error;
  }
}

async function processStreamRecords(streamClient, spannerTable, tableConfig, streamArn, shardId, startTimestamp = null) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 1000;

  try {
    // Use TRIM_HORIZON to get all records and filter by timestamp
    const iteratorParams = {
      StreamArn: streamArn,
      ShardId: shardId,
      ShardIteratorType: 'TRIM_HORIZON'
    };

    console.log(`Starting stream processing for shard ${shardId} from beginning, will filter records after ${new Date(startTimestamp).toISOString()}`);

    const { ShardIterator } = await streamClient.getShardIterator(iteratorParams);
    let currentShardIterator = ShardIterator;

    while (currentShardIterator) {
      try {
        const { Records, NextShardIterator } = await streamClient.getRecords({
          ShardIterator: currentShardIterator
        });

        if (Records && Records.length > 0) {
          // Filter records based on timestamp
          const filteredRecords = startTimestamp 
            ? Records.filter(record => {
                const recordTimestamp = record.dynamodb.ApproximateCreationDateTime;
                const shouldInclude = recordTimestamp >= startTimestamp;
                // console.log(`Record timestamp: ${new Date(recordTimestamp).toISOString()} (${recordTimestamp}), Checkpoint: ${new Date(startTimestamp).toISOString()} (${startTimestamp}), Include: ${shouldInclude}`);
                return shouldInclude;
              })
            : Records;

          if (filteredRecords.length > 0) {
            console.log('Processing records:', JSON.stringify(filteredRecords, null, 2));
            let processedRecords = 0;
            const totalRecords = filteredRecords.length;
          
            // Process records in transaction batches
            const batchSize = config.batchSize || 1000;
            const recordBatches = [];
          
            for (let i = 0; i < filteredRecords.length; i += batchSize) {
              recordBatches.push(filteredRecords.slice(i, i + batchSize));
            }

            for (const batch of recordBatches) {
              let retries = 0;
              let success = false;

              while (!success && retries < MAX_RETRIES) {
                try {
                  const mutations = new MutationSet();

                  for (const record of batch) {
                    if (record.eventName === 'REMOVE') {
                      const keys = convertDynamoDBObject(record.dynamodb.Keys);
                      const keysArray = Object.values(keys);
                      mutations.deleteRows(tableConfig.target.tableName, keysArray);
                    } else {
                      const newImage = convertDynamoDBObject(record.dynamodb.NewImage);
                      const spannerRow = convertItemToRow(newImage);
                      mutations.upsert(tableConfig.target.tableName, spannerRow);
                    }
                  }

                  await new Promise((resolve, reject) => {
                    spannerTable.database.writeAtLeastOnce(mutations, (err, res) => {
                      if (err) {
                        console.error('Error writing mutations:', err);
                        reject(err);
                        return;
                      }
                      console.log(`Successfully processed batch of ${batch.length} records.`);
                      resolve(res);
                    });
                  });

                  // Update processed records count
                  processedRecords += batch.length;

                  // Save checkpoint with current timestamp
                  // if (processedRecords >= config.checkpointFrequency || processedRecords >= totalRecords) {
                    if (processedRecords >= config.checkpointFrequency) {
                    try {
                      const currentTimestamp = Date.now();
                      await saveCheckpoint(tableConfig.source.tableName, currentTimestamp);
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
        }

        // Wait before getting more records to avoid hitting rate limits
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        if (!NextShardIterator) {
          console.log(`Reached end of shard ${shardId}`);
          break;
        }
        
        currentShardIterator = NextShardIterator;
      } catch (error) {
        if (error.$metadata?.httpStatusCode === 400 && error.name === 'ResourceNotFoundException') {
          console.log(`Shard iterator expired or invalid for shard ${shardId}, stopping processing`);
          break;
        }
        throw error;
      }
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
      // Get initial timestamp before migration
      const startTimestamp = await getInitialTimestamp(
        sourceConfig.region, 
        sourceConfig.tableName,
        sourceConfig.endpoint
      );
      
      // Perform migration
      await migrateTable(ddbClient, table, { source: sourceConfig, target: targetConfig });
      
      // Start live replication from the saved timestamp
      await startLiveReplication(ddbClient, table, { source: sourceConfig, target: targetConfig }, startTimestamp);
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

// Parse command line arguments
const args = process.argv.slice(2);
const mode = args[0] || 'migrate'; // Default to 'migrate' if not provided

// Parse segment and totalSegments arguments
for (let i = 1; i < args.length; i += 2) {
  if (args[i] === '--segment' && args[i + 1]) {
    config.segment = parseInt(args[i + 1], 10);
  } else if (args[i] === '--totalSegments' && args[i + 1]) {
    config.totalSegments = parseInt(args[i + 1], 10);
  }
}

// Validate segment and totalSegments if provided
if (config.totalSegments && config.segment >= config.totalSegments) {
  console.error(`Invalid segment value: ${config.segment}. Must be less than totalSegments (${config.totalSegments})`);
  process.exit(1);
}

if (config.totalSegments) {
  console.log(`Starting with segment ${config.segment} of ${config.totalSegments}`);
}
startProcess(mode);

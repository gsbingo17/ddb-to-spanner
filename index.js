import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { DynamoDBStreams, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand } from "@aws-sdk/client-dynamodb-streams";
import { DescribeTableCommand } from "@aws-sdk/client-dynamodb";
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
    // return convertDynamoDBNumberToSpanner(value);
    return Number(value);
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
          
          // Convert DynamoDB items to Spanner rows
          const rows = batch.map(convertItemToRow);
          
          try {
            // Create a new MutationSet for the batch
            const mutations = new MutationSet();
            
            // Format rows and add to mutation set
            rows.forEach(row => {
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
              mutations.insert(tableConfig.target.tableName, formattedRow);
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

// Function to get initial sequence number for live replication
async function getInitialSequenceNumber(region, tableName, endpoint, retryCount = 0) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 1000;

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

    const streamClient = new DynamoDBStreams({
      region,
      endpoint,
    });

    // Get the latest stream description
    const describeStreamCommand = new DescribeStreamCommand({ StreamArn: Table.LatestStreamArn });
    const { StreamDescription } = await streamClient.send(describeStreamCommand);

    if (!StreamDescription.Shards || StreamDescription.Shards.length === 0) {
      console.log("No shards found in the stream");
      return null;
    }

    // Sort shards by sequence number to get the most recent ones first
    const sortedShards = [...StreamDescription.Shards].sort((a, b) => {
      const seqA = a.SequenceNumberRange.StartingSequenceNumber;
      const seqB = b.SequenceNumberRange.StartingSequenceNumber;
      return seqB.localeCompare(seqA);
    });

    // Try each shard until we find a valid one
    for (const shard of sortedShards) {
      try {
        // Start with AT_SEQUENCE_NUMBER at the shard's starting sequence number
        const startingSeqNum = shard.SequenceNumberRange.StartingSequenceNumber;
        const getShardIteratorCommand = new GetShardIteratorCommand({
          StreamArn: Table.LatestStreamArn,
          ShardId: shard.ShardId,
          ShardIteratorType: "AT_SEQUENCE_NUMBER",
          SequenceNumber: startingSeqNum
        });
        
        const { ShardIterator } = await streamClient.send(getShardIteratorCommand);
        
        // Verify the iterator is valid by getting records
        const getRecordsCommand = new GetRecordsCommand({
          ShardIterator: ShardIterator,
          Limit: 1
        });
        
        const { Records } = await streamClient.send(getRecordsCommand);
        
        // If we successfully got records or an empty result, the shard is valid
        return startingSeqNum;
      } catch (shardError) {
        if (shardError.name === 'ResourceNotFoundException') {
          console.warn(`Shard ${shard.ShardId} no longer exists, trying next shard...`);
          continue;
        }
        console.warn(`Warning: Failed to get iterator for shard ${shard.ShardId}:`, shardError);
        continue;
      }
    }
    
    // If we've exhausted all shards and retries are available, wait and try again
    if (retryCount < MAX_RETRIES) {
      console.log(`No valid shards found, retrying (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS * Math.pow(2, retryCount)));
      return getInitialSequenceNumber(region, tableName, endpoint, retryCount + 1);
    }
    
    console.log("No valid shards found after all retries, returning null");
    return null;
  } catch (error) {
    console.error("Error getting initial sequence number:", error);
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

    // Get stream shards
    const describeStreamCommand = new DescribeStreamCommand({ 
      StreamArn: streamArn 
    });
    const { StreamDescription } = await streamClient.send(describeStreamCommand);
    const shards = StreamDescription.Shards;

    if (!shards || shards.length === 0) {
      console.log('No shards found in the stream');
      return;
    }

    // Load checkpoints for this table
    const checkpoints = await loadCheckpoint(tableConfig.source.tableName);
    console.log('Loaded checkpoints:', checkpoints);

    // Process each shard
    for (const shard of shards) {
      try {
        // Get the shard's sequence number range
        const shardStartSeq = shard.SequenceNumberRange.StartingSequenceNumber;
        const shardEndSeq = shard.SequenceNumberRange.EndingSequenceNumber;
        
        // Use checkpoint if available
        const checkpointSequenceNumber = checkpoints[shard.ShardId];
        
        // Determine the appropriate sequence number to start from
        let sequenceNumber = null;
        if (checkpointSequenceNumber) {
          console.log(`Found checkpoint for shard ${shard.ShardId}: ${checkpointSequenceNumber}`);
          sequenceNumber = checkpointSequenceNumber;
        } else if (startSequenceNumber) {
          // Only use startSequenceNumber if it falls within this shard's range
          const startSeqBigInt = BigInt(startSequenceNumber);
          const shardStartBigInt = BigInt(shardStartSeq);
          const shardEndBigInt = shardEndSeq ? BigInt(shardEndSeq) : BigInt(Number.MAX_SAFE_INTEGER);
          
          if (startSeqBigInt >= shardStartBigInt && startSeqBigInt <= shardEndBigInt) {
            console.log(`Using initial sequence number for shard ${shard.ShardId}: ${startSequenceNumber}`);
            sequenceNumber = startSequenceNumber;
          }
        }
        
        // If no valid sequence number is found, use the shard's start sequence
        if (!sequenceNumber) {
          console.log(`Starting from beginning of shard ${shard.ShardId} with sequence number: ${shardStartSeq}`);
          sequenceNumber = shardStartSeq;
        }
        
        await processStreamRecords(
          streamClient,
          spannerTable,
          tableConfig,
          streamArn,
          shard.ShardId,
          sequenceNumber
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

async function processStreamRecords(streamClient, spannerTable, tableConfig, streamArn, shardId, startSequenceNumber = null) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 1000;

  try {
    // Determine iterator type and sequence number
    let iteratorType = 'AT_SEQUENCE_NUMBER';
    let sequenceNumber = startSequenceNumber;

    if (!sequenceNumber) {
      iteratorType = 'TRIM_HORIZON';
      sequenceNumber = null;
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
      try {
        const { Records, NextShardIterator } = await streamClient.getRecords({
          ShardIterator: currentShardIterator
        });

        if (Records && Records.length > 0) {
          console.log('Processing records:', JSON.stringify(Records, null, 2));
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
              const mutations = new MutationSet();

              for (const record of batch) {
                const formattedRow = {};
                switch (record.eventName) {
                  case 'INSERT':
                  case 'MODIFY': {
                    for (const [key, value] of Object.entries(record.dynamodb.NewImage)) {
                      if (value.N) {
                        formattedRow[key] = Number(value.N);
                      } else if (value.S) {
                        formattedRow[key] = value.S;
                      } else if (value.BOOL !== undefined) {
                        formattedRow[key] = value.BOOL;
                      } else if (value.M) {
                        const mapValue = {};
                        for (const [mapKey, mapVal] of Object.entries(value.M)) {
                          if (mapVal.N) {
                            mapValue[mapKey] = Number(mapVal.N);
                          } else if (mapVal.S) {
                            mapValue[mapKey] = mapVal.S;
                          } else if (mapVal.BOOL !== undefined) {
                            mapValue[mapKey] = mapVal.BOOL;
                          } else if (mapVal.M) {
                            // Handle nested maps recursively
                            const nestedMap = {};
                            for (const [nestedKey, nestedVal] of Object.entries(mapVal.M)) {
                              if (nestedVal.N) {
                                nestedMap[nestedKey] = Number(nestedVal.N);
                              } else if (nestedVal.S) {
                                nestedMap[nestedKey] = nestedVal.S;
                              } else if (nestedVal.BOOL !== undefined) {
                                nestedMap[nestedKey] = nestedVal.BOOL;
                              }
                            }
                            mapValue[mapKey] = nestedMap;
                          }
                        }
                        formattedRow[key] = mapValue;
                      } else {
                        formattedRow[key] = value;
                      }
                    }
                    mutations.insert(tableConfig.target.tableName, formattedRow);
                    break;
                  }
                  case 'REMOVE': {
                    for (const [key, value] of Object.entries(record.dynamodb.Keys)) {
                      if (value.N) {
                        formattedRow[key] = Number(value.N);
                      } else if (value.S) {
                        formattedRow[key] = value.S;
                      } else if (value.BOOL !== undefined) {
                        formattedRow[key] = value.BOOL;
                      } else if (value.M) {
                        const mapValue = {};
                        for (const [mapKey, mapVal] of Object.entries(value.M)) {
                          if (mapVal.N) {
                            mapValue[mapKey] = Number(mapVal.N);
                          } else if (mapVal.S) {
                            mapValue[mapKey] = mapVal.S;
                          } else if (mapVal.BOOL !== undefined) {
                            mapValue[mapKey] = mapVal.BOOL;
                          } else if (mapVal.M) {
                            // Handle nested maps recursively
                            const nestedMap = {};
                            for (const [nestedKey, nestedVal] of Object.entries(mapVal.M)) {
                              if (nestedVal.N) {
                                nestedMap[nestedKey] = Number(nestedVal.N);
                              } else if (nestedVal.S) {
                                nestedMap[nestedKey] = nestedVal.S;
                              } else if (nestedVal.BOOL !== undefined) {
                                nestedMap[nestedKey] = nestedVal.BOOL;
                              }
                            }
                            mapValue[mapKey] = nestedMap;
                          }
                        }
                        formattedRow[key] = mapValue;
                      } else {
                        formattedRow[key] = value;
                      }
                    }
                    mutations.delete(tableConfig.target.tableName, formattedRow);
                    break;
                  }
                }
              }

              await new Promise((resolve, reject) => {
                spannerTable.database.writeAtLeastOnce(mutations, (err, res) => {
                  if (err) {
                    console.error('Error writing mutations:', err);
                    reject(err);
                    return;
                  }
                  console.log(`Successfully processed batch of ${batch.length} records. Response:`, res);
                  resolve(res);
                });
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

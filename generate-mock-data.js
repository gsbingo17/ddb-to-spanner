import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

// Configuration
const config = {
  tableName: 'mock-data-table-2', // Change this to your table name
  rowCount: 200, // Change this to desired number of rows
  region: 'us-west-2' // Change this to your region
};

// Initialize DynamoDB client
const ddbClient = DynamoDBDocument.from(new DynamoDB({
  region: config.region,
  endpoint: 'http://localhost:32331',
}), {
  marshallOptions: {
    convertClassInstanceToMap: true,
    removeUndefinedValues: true,
    convertEmptyValues: false,
    numberAsString: true  // This will preserve float numbers by storing them as strings
  }
});

// Function to generate random string
function generateString(prefix = '') {
  return `${prefix}${Math.random().toString(36).substring(7)}`;
}

// Function to generate random float
function generateFloat(min = 0, max = 1000) {
  // Add a small random decimal (0.01 to 0.99) to ensure we never get whole numbers
  const randomDecimal = Math.random() * 0.99 + 0.01;
  return parseFloat((Math.random() * (max - min) + min + randomDecimal).toFixed(2));
}

// Function to generate random integer
function generateInteger(min = 0, max = 1000) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Function to generate random boolean
function generateBoolean() {
  return Math.random() < 0.5;
}

// Function to generate random timestamp
function generateTimestamp() {
  const start = new Date(2020, 0, 1).getTime();
  const end = new Date().getTime();
  return new Date(start + Math.random() * (end - start)).toISOString();
}

// Function to generate complex array
function generateComplexArray(size = 3) {
  return Array.from({ length: size }, (_, index) => ({
    a: generateString(`string${index + 1}_`),
    b: generateString(`value${index + 1}_`)
  }));
}

// Function to generate random JSON object
function generateJSON() {
  return {
    id: generateString('id_'),
    count: generateInteger(1, 100),
    enabled: generateBoolean(),
    metadata: {
      created: generateTimestamp(),
      modified: generateTimestamp(),
      version: generateFloat(1, 10)
    }
  };
}

// Function to generate a single mock item
function generateMockItem(id) {
  return {
    id: id.toString(), // Primary key
    stringField: generateString('test_'),
    floatField: generateFloat(),
    integerField: generateInteger(),
    booleanField: generateBoolean(),
    timestampField: generateTimestamp(),
    jsonField: generateJSON(),
    arrayField: generateComplexArray(generateInteger(2, 5)),
    tags: ['tag1', 'tag2', 'tag3'].slice(0, generateInteger(1, 3))
  };
}

// Function to batch write items to DynamoDB
async function batchWriteItems(items) {
  const batchSize = 25; // DynamoDB batch write limit
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const params = {
      RequestItems: {
        [config.tableName]: batch.map(item => ({
          PutRequest: {
            Item: item
          }
        }))
      }
    };

    try {
      await ddbClient.batchWrite(params);
      console.log(`Wrote batch of ${batch.length} items (${i + 1} to ${i + batch.length})`);
    } catch (error) {
      console.error(`Error writing batch starting at item ${i + 1}:`, error);
      throw error;
    }
  }
}

// Main function to generate and write mock data
async function generateMockData() {
  console.log(`Generating ${config.rowCount} mock items for table ${config.tableName}...`);
  
  const items = Array.from({ length: config.rowCount }, (_, i) => generateMockItem(i + 1));
  
  try {
    await batchWriteItems(items);
    console.log('Successfully generated all mock data!');
  } catch (error) {
    console.error('Error generating mock data:', error);
  }
}

// Run the generator
generateMockData();

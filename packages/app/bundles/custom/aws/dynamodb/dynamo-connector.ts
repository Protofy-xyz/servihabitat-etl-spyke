
import { DynamoDBClient, DescribeTableCommand, CreateTableCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { STSClient, AssumeRoleWithWebIdentityCommand } from "@aws-sdk/client-sts";
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import * as fs from 'fs';
import { BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";

const ENABLE_AWS_DYNAMODB_SANDBOX = process.env?.ENABLE_AWS_DYNAMODB_SANDBOX;
const AWS_DYNAMODB_ACCESS_KEY = process.env?.AWS_DYNAMODB_ACCESS_KEY ?? "";
const AWS_DYNAMODB_SECRET_KEY = process.env?.AWS_DYNAMODB_SECRET_KEY ?? "";
const AWS_DYNAMODB_REGION = process.env?.AWS_DYNAMODB_REGION ?? "eu-south-2";


export class DynamoConnector {
    dynamoDbClient
    constructor(client) {
        this.dynamoDbClient = client;
    }

    async getTableInfo(tableName: string) {
        try {
            // Check if the table exists
            const describeTableCommand = new DescribeTableCommand({ TableName: tableName });
            const response = await this.dynamoDbClient.send(describeTableCommand);
            return response;
        } catch (e) {
            console.log(`Error describing table ${tableName}. Error: ${e}`);
        }
    }
    async initTable(tableName: string) {
        try {
            // Check if the table exists
            const describeTableCommand = new DescribeTableCommand({ TableName: tableName });
            const response = await this.dynamoDbClient.send(describeTableCommand);
            await this.waitForTableToBecomeActive(tableName)
            console.log(`Table ${tableName} already exists.`);
        } catch (err) {
            if (err.name === 'ResourceNotFoundException') {
                // Table doesn't exist, so create it
                console.log(`Table ${tableName} does not exist. Creating...`);

                const createTableParams = {
                    TableName: tableName,
                    AttributeDefinitions: [
                        { AttributeName: 'id', AttributeType: 'S' }, // TODO replace key and bypass it, or add secondary indexes
                    ],
                    KeySchema: [
                        { AttributeName: 'id', KeyType: 'HASH' }, // TODO replace key and bypass it, or add secondary indexes
                    ],
                    BillingMode: "PAY_PER_REQUEST", // Switch to on-demand billing
                    // ProvisionedThroughput: {
                    //     ReadCapacityUnits: 10,
                    //     WriteCapacityUnits: 10,
                    // },
                    Tags: [
                        { Key: "app", Value: "bapis" } // needed for servihabitat control
                    ]
                };

                try {
                    const createTableCommand = new CreateTableCommand(createTableParams as any);
                    await this.dynamoDbClient.send(createTableCommand);
                    console.log(`Table ${tableName} created successfully. Waiting to become active...`);
                    await this.waitForTableToBecomeActive(tableName)
                } catch (createError) {
                    console.error("Error creating table:", createError);
                    throw new Error(createError)
                }
            } else {
                throw new Error(err)
            }
        }
    }

    async waitForTableToBecomeActive(tableName) {
        let isTableActive = false;
        while (!isTableActive) {
            const describeTableCommand = new DescribeTableCommand({ TableName: tableName });
            const response = await this.dynamoDbClient.send(describeTableCommand);
            const tableStatus = response.Table.TableStatus;
            if (tableStatus === 'ACTIVE') {
                console.log(`Table ${tableName} is now ACTIVE.`);
                isTableActive = true;
            } else {
                console.log(`Waiting for table ${tableName} to become ACTIVE. Current status: ${tableStatus}`);
                await new Promise(resolve => setTimeout(resolve, 5000)); // Wait for 5 seconds before checking again
            }
        }
    }

    async put(tableName: string, item: any) {
        // Loop through each item and upload to DynamoDB
        const params = {
            TableName: tableName,
            Item: item // Directly use the item as it's already in DynamoDB JSON format
        };

        try {
            const result = await this.dynamoDbClient.send(new PutItemCommand(params)); // Realizes an Upsert operation
            console.log('RESULT RESULT: ', result);
            return result;
            // console.log(`Successfully uploaded item with ID: ${item.Item.id.S}`);
        } catch (error) {
            console.error(`Error uploading item with ID: ${item.id.S}`, error);
            throw error;
        }
    }

    static async getClient(options?) {
        options = options ? { valueEncoding: 'json', ...options } : { valueEncoding: 'json' };
        const clientConfig = {
            region: AWS_DYNAMODB_REGION, // The aws region where is your dynamodb hosted
        };
        if (ENABLE_AWS_DYNAMODB_SANDBOX) {
            clientConfig["credentials"] = {
                accessKeyId: AWS_DYNAMODB_ACCESS_KEY,
                secretAccessKey: AWS_DYNAMODB_SECRET_KEY,
                // sessionToken: "YOUR_SESSION_TOKEN"  // only if needed
            }
        } else {
            // console.log('DEV: DB getClient, creating dynamodb client using service account sa-bapis')
            const webIdentityToken = process.env.AWS_WEB_IDENTITY_TOKEN_FILE;  // Token de identidad web proporcionado por EKS
            const roleArn = process.env.AWS_ROLE_ARN;
            // console.log('DEV: Credentials:', { roleArn, webIdentityToken, AWS_DYNAMODB_REGION })

            try {
                console.log('DEV: getting temporary credentials')
                const temporaryCredentials = await getTemporaryCredentials(roleArn, webIdentityToken);
                console.log('DEV: temporary credentials', temporaryCredentials)
                clientConfig["credentials"] = {
                    accessKeyId: temporaryCredentials.AccessKeyId,
                    secretAccessKey: temporaryCredentials.SecretAccessKey,
                    sessionToken: temporaryCredentials.SessionToken,
                }
            } catch (e) {
                console.log('[AWS - Error] Error getting temporary credentials. Error: ' + e);
                throw new Error('Error getting temporary AWS credentials (via service account)...')
            }
        }
        const dynamodbClient = new DynamoDBClient({ ...clientConfig });
        const client = DynamoDBDocumentClient.from(dynamodbClient);
        return client;
    }

    async batchWriteItems(tableName, items) {
        const MAX_BATCH_SIZE = 25; // max batch size allowed by dynamodb
        const MAX_RETRIES = 5; // Set the maximum number of retries

        for (let i = 0; i < items.length; i += MAX_BATCH_SIZE) {
            const batch = items.slice(i, i + MAX_BATCH_SIZE).map(item => ({
                PutRequest: { Item: item }
            }));

            const params = {
                RequestItems: {
                    [tableName]: batch
                }
            };

            let attempt = 0;
            let success = false;

            while (attempt < MAX_RETRIES && !success) {
                try {
                    const result = await this.dynamoDbClient.send(new BatchWriteItemCommand(params));
                    console.log(`Batch write successful:`, result);
                    success = true; // Exit loop if successful
                } catch (error) {
                    if (error.name === 'ProvisionedThroughputExceededException') {
                        const waitTime = Math.pow(2, attempt) * 1000; // Exponential backoff: wait 2^n seconds
                        console.error(`ProvisionedThroughputExceededException: Retrying batch in ${waitTime / 1000} seconds...`);
                        await new Promise(resolve => setTimeout(resolve, waitTime)); // Wait before retrying
                    } else {
                        console.error(`Error with batch write:`, error);
                        // throw error; // Rethrow non-retryable errors
                    }
                    attempt++;
                }
            }

            if (!success) {
                console.log(`Failed to write batch after ${MAX_RETRIES} retries.`);
                // throw new Error(`Failed to write batch after ${MAX_RETRIES} retries.`);
            }
        }
    }
}

async function getTemporaryCredentials(roleArn, webIdentityTokenPath) {
    console.log('DEV: inside getTemporaryCredentials')
    const stsClient = new STSClient({ region: AWS_DYNAMODB_REGION });
    console.log('DEV: getTemporaryCredentials : stsClient')
    const token = await fs.readFileSync(webIdentityTokenPath, { encoding: 'utf-8' });
    console.log('DEV: getTemporaryCredentials : token: ', token)
    console.log('DEV: getTemporaryCredentials : about to assumeRoleCommand: ')
    const assumeRoleCommand = new AssumeRoleWithWebIdentityCommand({
        RoleArn: roleArn,
        RoleSessionName: 'temporaly-bapis-session',
        WebIdentityToken: token,
        DurationSeconds: 900,  // Duración de las credenciales, en segundos (15 min)
    });
    console.log('DEV: getTemporaryCredentials : assumeRoleCommand: ', assumeRoleCommand)
    const response = await stsClient.send(assumeRoleCommand);
    console.log('DEV: getTemporaryCredentials : recieved response: ', response)
    return response?.Credentials;
}
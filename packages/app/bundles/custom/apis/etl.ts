import { Protofy } from "protobase";
import { APIContext } from "protolib/bundles/apiContext"
import { Application } from 'express';
import { handler } from 'protonode'
import { getBucket } from '../aws/s3/s3-connector';
import { DynamoConnector } from "../aws/dynamodb/dynamo-connector";

Protofy("type", "CustomAPI")

export default Protofy("code", async (app: Application, context: typeof APIContext) => {
    context.automations.automation({
        name: 'etl',
        responseMode: 'wait',
        app: app
    })
    app.get('/api/v1/etl/load_from_s3_to_dynamodb', handler(async (req, res) => {
        // 1. Read the file from S3
        const bucket = await getBucket();
        const filename = "etl_test/PROMOTIONS_20240917.JSON";
        // Transform bucket s3 data 
        const content = await bucket.readResource(filename); //  gets file content as string (but following DynamoDB JSON)
        const data = content.split('\n').filter(Boolean).map((line: string) => JSON.parse(line)?.Item); // converts it to JSON array (the elements have DynamoDBJSON fromat)
        // Create Dynamodb table
        console.log('Getting AWS client for dynamodb...')
        const awsClient = await DynamoConnector.getClient();
        const tableName = "promotions_test";
        const dynamodb = new DynamoConnector(awsClient); // Gets database
        // Create table if no exist
        await dynamodb.initTable(tableName); // FIX? -> add custom indexes for each of models | question: can be created from here or should be previously done by bapis-monorepo
        console.log('Successfully created table:', tableName);
        // Upload each element to dynamodb
        await dynamodb.batchWriteItems(tableName, data);
        res.send('Successfully ETL data from S3 to DynamoDB');
    }));
});

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
        const raw_data = content.split('\n').filter(Boolean).map((line: string) => JSON.parse(line)?.Item); // converts it to JSON array (the elements have DynamoDBJSON fromat)
        // console.log('DATA 1: length :: ' + data.length);
        // console.log('DATA 2 (missing ids): length :: ' + data.filter((i) => i.hasOwnProperty('id'))?.length);
        // FIX: Promotions NFQ Data problems
        const data = raw_data.reduce((total, item) => {
            const itemFoundIndex = total.findIndex((i) => i.id.S === item.id.S); // -1 when not found
            if (item.hasOwnProperty('id')) {
                if (itemFoundIndex == -1) { // Add item bc is new item
                    return total.concat(item);
                }
                else { // Modify products with existing items and incoming item
                    total[itemFoundIndex] = { ...total[itemFoundIndex], products: { SS: total[itemFoundIndex].products.SS.concat(item.products.SS) } }
                    return total;
                }
            }
            return total
        }, [])
        // console.log('DATA 3 (No duplicateds ids): length :: ' + data2.length);
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
    app.get('/api/v1/etl/get_tableInfo/:tableName', handler(async (req, res) => {
        const tableName = req.params.tableName;
        const awsClient = await DynamoConnector.getClient();
        const dynamodb = new DynamoConnector(awsClient); // Gets database
        const tableInfo = await dynamodb.getTableInfo(tableName);
        res.send(tableInfo)
    }));
});

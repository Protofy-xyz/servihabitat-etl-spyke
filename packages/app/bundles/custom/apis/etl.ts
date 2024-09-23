import { Protofy } from "protobase";
import { APIContext } from "protolib/bundles/apiContext"
import { Application } from 'express';
import { handler } from 'protonode'
import { getBucket } from '../aws/s3/s3-connector';
import { DynamoConnector } from "../aws/dynamodb/dynamo-connector";
import { ETL } from "../models/ETL";

Protofy("type", "CustomAPI")

export default Protofy("code", async (app: Application, context: typeof APIContext) => {
    context.automations.automation({
        name: 'etl',
        responseMode: 'wait',
        app: app
    })

    app.get('/api/v1/etl/:enititymodel/load_from_s3_to_dynamodb', handler(async (req, res) => {
        type DomainEntities = "promotions" | "products" | "activitys" | "clients" | "managements" | "checklists";
        const enititymodel: DomainEntities = req.params.enititymodel as DomainEntities;

        const getFileName = (entity: string) => {
            const fileNames = {
                promotions: "PROMOTIONS_20240923030914.JSON",
                products: "PRODUCTS_20240923030914.JSON",
                activitys: "",
                clients: "CLIENTS_20240923030914.JSON",
                managements: "MANAGEMENTS_20240923030914.JSON",
                checklists: "CHECKLISTS_20240923030914.JSON"
            }
            const filename = fileNames[entity];
            if (!filename) throw new Error("Error getting filename for entity model: " + entity);
            const prefix = "etl_test";
            return `${prefix}/${filename}`;
        }

        // 1. Read the file from S3
        const bucket = await getBucket();
        // Transform bucket s3 data 
        const filename = getFileName(enititymodel);
        const content = await bucket.readResource(filename); //  gets file content as string (but following DynamoDB JSON)
        const raw_data = content.split('\n').filter(Boolean).map((line: string) => JSON.parse(line)?.Item); // converts it to JSON array (the elements have DynamoDBJSON fromat)
        // console.log('RAW DATA 1: length :: ' + raw_data.length);
        // console.log('RAW DATA 2: (missing ids): length :: ' + raw_data.filter((i) => i.hasOwnProperty('id'))?.length);
        const data = (new ETL(enititymodel, raw_data)).transform();
        // console.log('DATA 3 (No duplicateds ids): length :: ' + data.length);
        // Create Dynamodb table
        const awsClient = await DynamoConnector.getClient();
        const tableName = `${enititymodel}_test`; // TABLE NAME --> TODO: remove "_test" sufix

        const dynamodb = new DynamoConnector(awsClient); // Gets database
        // Create table if no exist
        await dynamodb.initTable(tableName); // FIX? -> add custom indexes for each of models | question: can be created from here or should be previously done by bapis-monorepo
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

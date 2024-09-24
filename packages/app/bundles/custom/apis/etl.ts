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
        const awsClient = await DynamoConnector.getClient();
        const tableName = `${enititymodel}_test`; // TABLE NAME --> TODO: remove "_test" sufix
        const dynamodb = new DynamoConnector(awsClient); // Gets database
        // Create table if no exist
        await dynamodb.initTable(tableName);
        await bucket.readResource(filename, onProcessBatchLines); //  gets file content as string (but following DynamoDB JSON)
        async function onProcessBatchLines (batchLines: string[]) {
            const raw_data= batchLines.filter(Boolean).map((line: string) => JSON.parse(line)?.Item);
            const data = (new ETL(enititymodel, raw_data)).transform();
            await dynamodb.batchWriteItems(tableName, data);
        }
        // Upload each element to dynamodb
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

import { Readable, Duplex, Transform } from 'stream';
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
    S3Client, ListBucketsCommand,
    PutObjectCommand, DeleteObjectCommand,
    GetObjectCommand, ListObjectsV2Command,
    ListObjectsV2CommandInput,
    HeadObjectCommand
} from "@aws-sdk/client-s3";
import { STSClient, AssumeRoleWithWebIdentityCommand } from "@aws-sdk/client-sts";
import * as fs from 'fs';
const readline = require('readline');

const ENABLE_AWS_S3_SANDBOX = process.env?.ENABLE_AWS_S3_SANDBOX;
const AWS_S3_BUCKET = process.env?.AWS_S3_BUCKET;
const AWS_S3_ACCESS_KEY = process.env?.AWS_S3_ACCESS_KEY ?? "";
const AWS_S3_SECRET_KEY = process.env?.AWS_S3_SECRET_KEY ?? "";
const AWS_S3_REGION = process.env?.AWS_S3_REGION ?? "eu-south-2";

async function getTemporaryCredentials(roleArn, webIdentityTokenPath) {
    const stsClient = new STSClient({ region: AWS_S3_REGION });
    const token = await fs.readFileSync(webIdentityTokenPath, { encoding: 'utf-8' });
    const assumeRoleCommand = new AssumeRoleWithWebIdentityCommand({
        RoleArn: roleArn,
        RoleSessionName: 'temporaly-bapis-session',
        WebIdentityToken: token,
        DurationSeconds: 3600,  // Duración de las credenciales, en segundos (15 min)
    });
    const response = await stsClient.send(assumeRoleCommand);
    return response?.Credentials;
}

export class S3Bucket {
    client;
    bucket; // bucket name
    constructor(client, bucketName?: string) {
        this.client = client;
        this.bucket = bucketName ?? AWS_S3_BUCKET;
    }

    static async getClient() {
        const clientConfig = {
            region: AWS_S3_REGION, // The aws region where is your dynamodb hosted
        };
        if (ENABLE_AWS_S3_SANDBOX) { // Allow you to load Access keys from envs instead of using service account
            console.log('Using Sandbox credentials for s3...')
            clientConfig["credentials"] = {
                accessKeyId: AWS_S3_ACCESS_KEY,
                secretAccessKey: AWS_S3_SECRET_KEY,
                //sessionToken: "YOUR_SESSION_TOKEN"  // only if needed
            }
        } else { // Try service account
            console.log('DEV: S3BUcket using service account.');
            const webIdentityToken = process.env.AWS_WEB_IDENTITY_TOKEN_FILE;  // Token de identidad web proporcionado por EKS
            const roleArn = process.env.AWS_ROLE_ARN;
            try {
                console.log('DEV: S3BUcket :: getTemporaryCredentials');
                const temporaryCredentials = await getTemporaryCredentials(roleArn, webIdentityToken);
                console.log('DEV: S3BUcket :: temporaryCredentials', temporaryCredentials);
                clientConfig["credentials"] = {
                    accessKeyId: temporaryCredentials.AccessKeyId,
                    secretAccessKey: temporaryCredentials.SecretAccessKey,
                    sessionToken: temporaryCredentials.SessionToken,
                }
            } catch (e) {
                console.log('[AWS - Error] Error getting temporary credentials. Error: ' + e);
            }
        }
        return new S3Client({ ...clientConfig });
    }

    async listBuckets() {
        try {
            const data = await this.client.send(new ListBucketsCommand({}));
            return data.Buckets;
        } catch (err) {
            console.log("Error listing buckets: ", err);
        }
    }

    async put(filename: string, content: Buffer | string | Readable | Duplex | Transform | Blob, config = {}) {
        /*
        Content can be:
            - Buffer:  If you have binary data already loaded into memory, you can pass it as a Buffer
                example:
                    // Reading a file into a buffer
                    const fileContent = fs.readFileSync('path-to-your-file');
            - string: for uploading text
            - Stream (Readable, Duplex, Transform): For larger files or for data that you do not want to fully load into memory
                exemple:
                    // Creating a read stream
                    const fileStream = fs.createReadStream('path-to-your-file');
            - Blob
        */
        const putCmd = {
            Bucket: this.bucket,
            Key: filename, // File name you want to save as in S3
            Body: content,
            ...config
        };

        try {
            const data = await this.client.send(new PutObjectCommand(putCmd));
            console.log("File uploaded successfully.", data);
        } catch (err) {
            console.error("Error putting item:", err);
        }
    }

    async del(filename: string) {
        const delCmd = {
            Bucket: this.bucket,
            Key: filename,
        };

        try {
            const data = await this.client.send(new DeleteObjectCommand(delCmd));
            console.log("File deleted successfully", data);
        } catch (err) {
            console.error("Error", err);
        }
    }

    async resourceExists(filename) {
        try {
            const headCommand = new HeadObjectCommand({
                Bucket: this.bucket,
                Key: filename
            });
            await this.client.send(headCommand);
            return true
        } catch (e) {
            console.error('ERROR: resource does not exist. ', e)
            return false
        }
    }

    async getPresignedUrl(filename: string, expiration: number = 3600) {
        // Returns an URL that expires after 'expiration' time of specific resource
        // default URL expiration time 1 hour
        const exists = await this.resourceExists(filename)
        if (!exists) {
            throw new Error('Resource does not exist')
        }
        const command = new GetObjectCommand({
            Bucket: this.bucket,
            Key: filename
        });

        const url = await getSignedUrl(this.client, command, {
            expiresIn: expiration
        });
        return url;
    }

    async getBase64Image(filename: string) {
        const exists = await this.resourceExists(filename)
        if (!exists) {
            throw new Error('Resource does not exist')
        }
        const command = new GetObjectCommand({
            Bucket: this.bucket,
            Key: filename
        });
        const data = await this.client.send(command);
        // Buffer the data
        const chunks = [];
        for await (const chunk of data.Body) {
            chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        // Convert to base64
        return buffer.toString('base64');
    }

    async getPublicUrl(filename: string) {
        /* example:
            if "filename" is "path/to/image.jpg" then,
            https://your-bucket-name.s3.amazonaws.com/path/to/image.jpg
        */
        return `https://${this.bucket}.s3.amazonaws.com/${filename}`
    }

    async list(prefix?: string, delimiter?: string) {
        // list objects inside bucket
        /*
            Prefix which simulates a directory path (e.g. photos/2021/).
            Uses a delimiter (/), which helps list objects as if they were in directories by grouping them based on the prefix.
        */
        const params: ListObjectsV2CommandInput = { Bucket: this.bucket }; // This returns all objects by default
        if (prefix && delimiter) {
            /* 
                If 'prefix' and 'delimiter' are set you can 
                list specific objects emulating a common file 
                directory structure. Because s3 it's a bucket,
                and it has no directories.
            */
            params.Prefix = prefix;
            params.Delimiter = delimiter;
        }
        try {
            const data = await this.client.send(new ListObjectsV2Command(params));
            if (data.CommonPrefixes) {
                return data.CommonPrefixes;
                // return data.CommonPrefixes.map(item => item.Prefix);
            }
            return data.Contents;
            // return data.Contents.map(item => item.Key);
        } catch (err) {
            console.error("Error listing objects:", err);
        }
    }


    async readResource(filename: string, cb = (batchLines: string[]) => {}, _batchSize = 25) {
        const exists = await this.resourceExists(filename);
        if (!exists) {
            throw new Error('Resource does not exist')
        }
        const command = new GetObjectCommand({
            Bucket: this.bucket,
            Key: filename
        });

        try {
            const data = await this.client.send(command);
            if (!data.Body) {
                throw new Error("No content in the S3 resource");
            }
            const s3Stream = data.Body;
            const rl = readline.createInterface({
                input: s3Stream,
                crlfDelay: Infinity, // Recognizes both CRLF and LF as newline characters
            });
            let linesBatch = [];
            const batchSize = _batchSize ?? 25;
            for await (const line of rl) {
                linesBatch.push(line);
                if (linesBatch.length === batchSize) {
                    await cb(linesBatch);  // Process the batch
                    linesBatch = [];  // Reset the batch
                }
            }
            // Process any remaining lines if they don't fill the last batch
            if (linesBatch.length > 0) {
                await cb(linesBatch);
            }
            console.log('File processing completed.');
        } catch (e) {
            throw new Error("Error reading S3 resource. Error: " + e.message);
        }
    }
}

export const getBucket = async () => {
    const client = await S3Bucket.getClient();
    return new S3Bucket(client);
}
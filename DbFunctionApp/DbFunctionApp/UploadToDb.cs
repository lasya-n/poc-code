using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;

namespace DbFunctionApp
{
    public static class UploadToDb
    {
        private static readonly string cosmosConnectionString = "mongodb://tdppoccosmosdb:5tF17rQfU1FH68adPO2QqCbf1ZhRtdCXSC72wxY11lhT7QgziA1eqHB1490m1Llu7gklMbeM3wPLACDbWc45Og==@tdppoccosmosdb.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@tdppoccosmosdb@";
        private static readonly string cosmosDatabaseName = "tdppocdb";
        private static readonly string cosmosCollectionName = "tdppoccollection";

        [FunctionName("UploadToDb")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log)
        {
            try
            {
                // Read CSV data from the HTTP request
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

                // Parse and transform CSV data
                List<YourMongoDataModel> dataToInsert = ParseAndTransformCsvData(requestBody);

                // Write data to Azure Cosmos MongoDB
                await InsertDataIntoCosmosDb(dataToInsert);

                return new OkObjectResult("Data transferred successfully to Cosmos MongoDB.");
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error transferring data to Cosmos MongoDB.");
                return new StatusCodeResult(500);
            }
        }
        private static List<YourMongoDataModel> ParseAndTransformCsvData(string csvData)
        {
            
            List<YourMongoDataModel> dataList = new List<YourMongoDataModel>();

            var rows = csvData.Split('\n');
            foreach (var row in rows)
            {
                var columns = row.Split(',');
                if (columns.Length >= 1)
                {
                    //mapping content in blobs with fields in CosmosDB
                    dataList.Add(new YourMongoDataModel
                    {
                        id = columns[0],
                        date = columns[1],
                        amount = columns[2],
                        name = columns[3],
                        transactionId = columns[4]
                        
                    });
                }
            }
            return dataList;
        }

        private static async Task InsertDataIntoCosmosDb(List<YourMongoDataModel> dataToInsert)
        {
            var client = new MongoClient(cosmosConnectionString);
            var database = client.GetDatabase(cosmosDatabaseName);
            var collection = database.GetCollection<YourMongoDataModel>(cosmosCollectionName);

            // Insert data into MongoDB
            await collection.InsertManyAsync(dataToInsert);
        }
    }

    public class YourMongoDataModel
    {
        //setting DB schema
        public string id { get; set; }
        public string date { get; set; }
        public string amount { get; set; }
        public string name { get; set; }
        public string transactionId { get; set; }
        
    }
}
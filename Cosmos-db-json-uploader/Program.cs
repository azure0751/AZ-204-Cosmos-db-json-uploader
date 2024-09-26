using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System.Linq.Expressions;

namespace Cosmos_db_json_uploader
{
    internal class Program
    {
        private static readonly string endpointUri = "https://localhost:8081";
        private static readonly string primaryKey = "";
        private static readonly string databaseId = "demo";
        private static readonly string containerId = "democont";

        private static async Task Main(string[] args)
        {
            // Initialize Cosmos client
            CosmosClient cosmosClient = new CosmosClient(endpointUri, primaryKey);

            // Get database and container references
            Database database = cosmosClient.GetDatabase(databaseId);
            Container container = database.GetContainer(containerId);

            // Load JSON file from the local file system


            /// Path to the folder containing the JSON files
            string folderPath = "D:\\Github-Repos-Code\\azure0751\\AZ-204-Random-Json-Generator\\AZ-204-Random-Json-Generator\\bin\\Debug\\net8.0\\GeneratedJsonFiles";

            // Get all JSON files in the folder
            string[] jsonFiles = Directory.GetFiles(folderPath, "*.json");

            try
            {



                // Loop through each JSON file and upload to Cosmos DB
                foreach (var jsonFile in jsonFiles)
                {
                    Console.WriteLine($"Processing file: {Path.GetFileName(jsonFile)}");

                    // Read the content of the JSON file
                    string jsonContent = File.ReadAllText(jsonFile);

                    // Parse the JSON content
                    var parsedJson = JToken.Parse(jsonContent);

                    // Check if the JSON is an array or a single object
                    if (parsedJson is JArray items)
                    {
                        // If it's an array, iterate through each item
                        foreach (var item in items)
                        {
                            await UploadItemAsync(container, item);
                        }
                    }
                    else
                    {
                        // If it's a single object, upload directly
                        await UploadItemAsync(container, parsedJson);
                    }

                    Console.WriteLine($"Uploaded data from {Path.GetFileName(jsonFile)}");
                }

                Console.WriteLine("All files processed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }



            static async Task UploadItemAsync(Container container, JToken document)
            {
                // Generate a unique ID if the document doesn't have one
                if (document["id"] == null)
                {
                    document["id"] = Guid.NewGuid().ToString();
                }

                // Upload the document (schema-less)
                await container.CreateItemAsync(document, new PartitionKey(document["id"].ToString()));
            }
        }
    }
}
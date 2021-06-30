using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.WebJobs.Extensions.Http;

namespace SynapseFunctionApp
{
    public static class Function1
    {
        [FunctionName("Function1")]
        // Add retry capabilties to the function:
        // //https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-error-pages?tabs=csharp
        //[ExponentialBackoffRetry(3, "00:00:03", "00:12:00")] // Expontentail back retry
        public static async void Run([BlobTrigger("dataingest/{name}", Connection = "blobConnectionString")]  Stream myBlob,
          [Blob("samplefedfile/{name}", FileAccess.ReadWrite, Connection = "synapseConnectionString")]CloudBlobContainer blobContainer,
          string name, ILogger log)
        {
            // Log metadata
            log.LogInformation($"Blob trigger invoked: Name:{name}");

            // Capture blob name (name of csv file)
            var blobName = name;

            // Create target container if not exist

            // Ensure that target Synapse DataLake blob container exists
            if (!await blobContainer.ExistsAsync())
            {
                log.LogError($"Data Lake container does not exist");

                // Data Lake container must exist, or exit function
                throw new FunctionException($"Data Lake container does not exist");
                //await blobContainer.CreateIfNotExistsAsync();

                // Container does not exist -- throw
                //return new NotFoundObjectResult("Azure Synapse DataLake container does not exist");
            }

            try
            {
                var cloudBlockBlob = blobContainer.GetBlockBlobReference(blobName);
                //var photoBytes = Convert.FromBase64String(request.Photo);
                //await cloudBlockBlob.UploadFromByteArrayAsync(photoBytes, 0, photoBytes.Length);
                await cloudBlockBlob.UploadFromStreamAsync(myBlob);
            }
            catch (Exception ex)
            {
                log.LogError($"Exception encountered uploading stream: {ex.Message}");
                throw new FunctionException($"Exception encountered: {ex.Message}");
            }

            log.LogInformation($"Blob {name} successfully written");
        }
    }
}

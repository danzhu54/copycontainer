using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;

class Program
{
    static async Task Main(string[] args)
    {
        string jsonFilePath = "copycontainer/source_containers.json";
        string jsonString = await File.ReadAllTextAsync(jsonFilePath);
        var storageAccount = JsonSerializer.Deserialize<StorageAccount>(jsonString);

        if (storageAccount != null)
        {
            string destinationContainerName = storageAccount.StorageContainers.DestinationContainerName;
            string destinationToken = storageAccount.StorageContainers.Token;

            await DeleteDestinationContainerData(destinationContainerName, destinationToken);
            await CopyBlobs(storageAccount);
        }
    }

    static async Task CopyBlobs(StorageAccount storageAccount)
    {
        string destinationContainerName = storageAccount.StorageContainers.DestinationContainerName;
        string destinationToken = storageAccount.StorageContainers.Token;

        BlobServiceClient destinationServiceClient = new BlobServiceClient(new Uri(destinationContainerName), new AzureSasCredential(destinationToken));
        BlobContainerClient destinationContainerClient = destinationServiceClient.GetBlobContainerClient(destinationContainerName);

        Console.WriteLine($"Starting copy operation to {destinationContainerName} container...");
        foreach (var sourceContainer in storageAccount.StorageContainers.SourceContainers)
        {
            string sourceContainerName = sourceContainer.SourceContainerName;
            string sourceToken = sourceContainer.Token;

            BlobServiceClient sourceServiceClient = new BlobServiceClient(new Uri(sourceContainerName), new AzureSasCredential(sourceToken));
            BlobContainerClient sourceContainerClient = sourceServiceClient.GetBlobContainerClient(sourceContainerName);

            Console.WriteLine($"Copying blobs from {sourceContainerName} to {destinationContainerName} container ({storageAccount.StorageContainers.SourceContainers.IndexOf(sourceContainer) + 1}/{storageAccount.StorageContainers.SourceContainers.Count})...");
            await foreach (var blobItem in sourceContainerClient.GetBlobsAsync())
            {
                string destinationBlobName = storageAccount.StorageContainers.SourceContainers.Count > 1
                    ? $"{sourceContainerName}/{blobItem.Name}"
                    : blobItem.Name;

                BlobClient sourceBlobClient = sourceContainerClient.GetBlobClient(blobItem.Name);
                BlobClient destinationBlobClient = destinationContainerClient.GetBlobClient(destinationBlobName);

                Console.WriteLine($"Copying {blobItem.Name} blob from {sourceContainerName} to {destinationContainerName} container...");
                if (!await destinationBlobClient.ExistsAsync())
                {
                    await destinationBlobClient.StartCopyFromUriAsync(sourceBlobClient.Uri);
                }
            }
        }
        Console.WriteLine($"Copy operation to {destinationContainerName} container completed successfully.");
    }

    static async Task DeleteDestinationContainerData(string destinationContainerName, string destinationToken)
    {
        BlobServiceClient destinationServiceClient = new BlobServiceClient(new Uri(destinationContainerName), new AzureSasCredential(destinationToken));
        BlobContainerClient destinationContainerClient = destinationServiceClient.GetBlobContainerClient(destinationContainerName);

        Console.WriteLine($"Deleting data from {destinationContainerName} container...");
        await foreach (var blobItem in destinationContainerClient.GetBlobsAsync())
        {
            BlobClient blobClient = destinationContainerClient.GetBlobClient(blobItem.Name);
            await blobClient.DeleteIfExistsAsync();
        }
        Console.WriteLine($"Data from {destinationContainerName} container deleted successfully.");
    }
}

public class StorageAccount
{
    public StorageContainers StorageContainers { get; set; }
}

public class StorageContainers
{
    public string DestinationContainerName { get; set; }
    public string Token { get; set; }
    public List<SourceContainer> SourceContainers { get; set; }
}

public class SourceContainer
{
    public string SourceContainerName { get; set; }
    public string Token { get; set; }
}

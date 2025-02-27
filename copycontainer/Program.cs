using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;

class Program
{
    static async Task Main(string[] args)
    {
        string jsonFilePath = "source_containers.json";
        string jsonString = await File.ReadAllTextAsync(jsonFilePath);
        var storageAccounts = JsonSerializer.Deserialize<StorageAccounts>(jsonString);

        if (storageAccounts?.StorageAccountList != null)
        {
            foreach (var storageAccount in storageAccounts.StorageAccountList)
            {
                //await DeleteDestinationContainerData(storageAccount);
                await CopyBlobs(storageAccount);
            }
        }
        else
        {
            Console.WriteLine("Invalid JSON structure or missing required fields.");
        }
    }

    static async Task CopyBlobs(StorageAccount storageAccount)
    {
        string destinationContainerName = storageAccount.StorageContainers.DestinationContainerName;
        string destinationToken = storageAccount.StorageContainers.Token;
        string destinationUri = $"https://{storageAccount.StorageAccountName}.blob.core.windows.net/{destinationContainerName}?{destinationToken}";

        BlobServiceClient destinationServiceClient = new BlobServiceClient(new Uri(destinationUri));
        BlobContainerClient destinationContainerClient = destinationServiceClient.GetBlobContainerClient(destinationContainerName);

        Console.WriteLine($"Starting copy operation to {destinationContainerName} container...");
        foreach (var sourceContainer in storageAccount.StorageContainers.SourceContainers)
        {
            string sourceContainerName = sourceContainer.SourceContainerName;
            string sourceToken = sourceContainer.Token;
            string sourceUri = $"https://{storageAccount.StorageAccountName}.blob.core.windows.net/{sourceContainerName}?{sourceToken}";

            BlobContainerClient sourceContainerClient = new BlobContainerClient(new Uri(sourceUri));

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

    static async Task DeleteDestinationContainerData(StorageAccount storageAccount)
    {
        string destinationContainerName = storageAccount.StorageContainers.DestinationContainerName;
        string destinationToken = storageAccount.StorageContainers.Token;
        string destinationUri = $"https://{storageAccount.StorageAccountName}.blob.core.windows.net/{destinationContainerName}?{destinationToken}";
        BlobServiceClient destinationServiceClient = new BlobServiceClient(new Uri(destinationUri), new AzureSasCredential(destinationToken));
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

public class StorageAccounts
{
    public List<StorageAccount> StorageAccountList { get; set; }
}

public class StorageAccount
{
    public StorageContainers StorageContainers { get; set; }
    public string StorageAccountName { get; set; }
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

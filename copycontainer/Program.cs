using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

// Define your source and destination container details
string storageAccountName = "";

string sourceContainerName = "";
string sourceSasToken = "";

string destinationContainerName = "";
string destinationSasToken = "";

// Construct the source and destination container URIs with SAS tokens
string sourceContainerUri = $"https://{storageAccountName}.blob.core.windows.net/{sourceContainerName}?{sourceSasToken}";
string destinationContainerUri = $"https://{storageAccountName}.blob.core.windows.net/{destinationContainerName}?{destinationSasToken}";

// Create BlobContainerClients for source and destination
BlobContainerClient sourceContainerClient = new BlobContainerClient(new Uri(sourceContainerUri));
BlobContainerClient destinationContainerClient = new BlobContainerClient(new Uri(destinationContainerUri));

try
{
    // List all blobs in the source container
    Console.WriteLine($"Listing blobs in source container '{sourceContainerName}':");
    await foreach (BlobItem blobItem in sourceContainerClient.GetBlobsAsync())
    {
        // Log the blob name and size
        Console.WriteLine($"Blob Name: {blobItem.Name}, Size: {blobItem.Properties.ContentLength} bytes");

        // Get the source blob client
        BlobClient sourceBlobClient = sourceContainerClient.GetBlobClient(blobItem.Name);

        // Get the destination blob client
        BlobClient destinationBlobClient = destinationContainerClient.GetBlobClient(blobItem.Name);

        // Copy the blob from the source to the destination
        Console.WriteLine($"Copying blob '{blobItem.Name}' to destination container...");
        await destinationBlobClient.StartCopyFromUriAsync(sourceBlobClient.Uri);

        // Wait for the copy operation to complete
        while (true)
        {
            BlobProperties properties = await destinationBlobClient.GetPropertiesAsync();
            if (properties.CopyStatus != CopyStatus.Pending)
            {
                if (properties.CopyStatus == CopyStatus.Success)
                {
                    Console.WriteLine($"Blob '{blobItem.Name}' copied successfully.");
                }
                else
                {
                    Console.WriteLine($"Blob '{blobItem.Name}' copy failed. Status: {properties.CopyStatus}");
                }
                break;
            }
            await Task.Delay(500); // Wait before checking the status again
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}

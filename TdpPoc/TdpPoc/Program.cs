using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Text;
using Newtonsoft.Json;
using ClosedXML.Excel;


namespace TdpPoc
{
    class Program
    {
        private static string connectionString = "DefaultEndpointsProtocol=https;AccountName=tdppocstacc;AccountKey=LTh/zR32WGiDdjEpzHVDou5czh9gUYuHozbzQTpStq2Fx7N9XqwJbULYe3HNVRStdR/IjkuhID8R+AStNqHOsQ==;EndpointSuffix=core.windows.net";
        private static string containerName = "poc-root-container";
        private static string sourceDirectoryName = "Input-folder";
        static string functionAppUrl = "https://tdppocfunapp.azurewebsites.net/api/Function1?code=2pixl5kuOMylISSpFT7U-OtYeunT5yzuF-c3qfPo0E-CAzFuVsgeBA==";
        static string functionKey = "2pixl5kuOMylISSpFT7U-OtYeunT5yzuF-c3qfPo0E-CAzFuVsgeBA==";
        static string logicAppUrl = "https://prod-10.centralus.logic.azure.com:443/workflows/8f5d598186e841eba7c9bdd86061cc37/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=G0qhiEcImAP_obgjm04kF9W1luVyhEv7t7VYWeckz4k";

        public static void Main(string[] args)
        {
            int valid_count = 0;
            int invalid_count = 0;
            string body;
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);
            Boolean flag = false;

            //Loops over all blobs in Input-folder
            foreach (BlobItem blobItem in blobContainerClient.GetBlobs(prefix: sourceDirectoryName))
            {
                flag = true;
                break;
            }
            if (flag == false)  //checks if Input-folder is empty
            {
                body = "The Input-folder is empty!!!<br>Please upload the files to get the status";
                SendEmail(body).Wait();
            }
            else
            {
                foreach (BlobItem blobItem in blobContainerClient.GetBlobs(prefix: sourceDirectoryName))
                {
                    BlobClient blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                    string filename = Path.GetFileName(blobItem.Name);

                    string destDirectoryName = "Output-folder";
                    string destinationPath;

                    string fileExtension = Path.GetExtension(filename); //gets blob file extension

                    //check the file extension and move accordingly
                    if (fileExtension.Equals(".csv", StringComparison.OrdinalIgnoreCase) || fileExtension.Equals(".xlsx", StringComparison.OrdinalIgnoreCase))
                    {
                        if (fileExtension.Equals(".csv", StringComparison.OrdinalIgnoreCase))
                        {
                            writeCsv(blobClient).Wait();    //insert csv data in Cosmos MongoDB
                        }
                        else
                        {
                            writeXlsx(blobClient).Wait();   //insert xlsx data in Cosmos MongoDB
                        }
                        valid_count++;
                        Console.WriteLine($"{filename}/is a Valid file");
                        destinationPath = $"{destDirectoryName}/{"Valid"}/{filename}";
                    }

                    else
                    {
                        invalid_count++;
                        Console.WriteLine($"{filename}/is an Invalid file");
                        destinationPath = $"{destDirectoryName}/{"Invalid"}/{filename}";
                    }
                    BlobClient destBlobClient = blobContainerClient.GetBlobClient(destinationPath);
                    destBlobClient.StartCopyFromUri(blobClient.Uri);    //moves file to Valid or Invalid folders respectively
                    blobClient.Delete();
                }
                int total_count = valid_count + invalid_count;
                body = "The total number of files are " + total_count + ".<br> In that, valid files count = " + valid_count + "<br> Invalid files count = " + invalid_count;
                SendEmail(body).Wait(); //send mail
            }
        }


        public static string convertExcelToCsv(byte[] excelData)
        {
            using (var stream = new MemoryStream(excelData))
            using (var workbook = new XLWorkbook(stream))
            {
                var worksheet = workbook.Worksheet(1); // Assuming data is in the first worksheet
                StringBuilder csvData = new StringBuilder();

                var range = worksheet.RangeUsed();
                var rows = range.RowsUsed();
                foreach (var row in rows)   //converting excel to csv cell wise
                {
                    var columns = row.Cells();
                    foreach (var cell in columns)
                    {
                        var cellValue = cell.Value;
                        cellValue = cell.IsEmpty() ? string.Empty : cellValue.ToString();
                        csvData.Append(cellValue);
                        csvData.Append(",");
                    }
                    csvData.AppendLine();
                }
                return csvData.ToString();
            }
        }


        public static async Task writeXlsx(BlobClient blobClient)
        {

            using (MemoryStream memoryStream = new MemoryStream())
            {
                blobClient.DownloadTo(memoryStream);    //getting data of a xlsx blob
                byte[] excelData = memoryStream.ToArray();
                string csvData = convertExcelToCsv(excelData);  //converting excel data to csv
                Console.WriteLine("Xlsx data: \n" + csvData);
                string[] csvLines = csvData.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                // Send the POST request to the Azure Function
                HttpResponseMessage response;
                using (HttpClient httpClient = new HttpClient())
                {
                    // Send the POST request to the Azure Function
                    httpClient.DefaultRequestHeaders.Add("x-functions-key", functionKey);
                    foreach (string line in csvLines)
                    {
                        string jsonContent = Newtonsoft.Json.JsonConvert.SerializeObject(line);
                        StringContent content = new StringContent(jsonContent, Encoding.UTF8, "application/json");  //POST request

                        response = await httpClient.PostAsync(functionAppUrl, content); //upload content in DB
                        if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine("Xlsx data uploaded successfully!");
                        }
                        else
                        {
                            Console.WriteLine($"Failed to upload CSV data. Status code: {response.StatusCode}");
                        }
                    }
                }
            }
        }


        public static async Task writeCsv(BlobClient blobClient)
        {
            string csvData;
            HttpResponseMessage response;
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    // Prepare the request content
                    using (MemoryStream stream = new MemoryStream())
                    {
                        blobClient.DownloadTo(stream);  //getting data of a csv blob
                        stream.Position = 0;
                        using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                        {
                            csvData = reader.ReadToEnd();
                        }
                        Console.WriteLine("CSV data:\n" + csvData);
                    }
                    string[] csvLines = csvData.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

                    // Send the POST request to the Azure Function
                    httpClient.DefaultRequestHeaders.Add("x-functions-key", functionKey);
                    foreach (string line in csvLines)
                    {
                        string jsonContent = JsonConvert.SerializeObject(line);
                        StringContent content = new StringContent(jsonContent, Encoding.UTF8, "application/json");  //POST request

                        response = await httpClient.PostAsync(functionAppUrl, content); //upload content in DB
                        if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine("CSV data uploaded successfully!");
                        }
                        else
                        {
                            Console.WriteLine($"Failed to upload CSV data. Status code: {response.StatusCode}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }


        public static async Task SendEmail(string body)
        {
            try
            {
                var emailContent = new
                {
                    email_body = body
                };
                var httpClient = new HttpClient();

                // Send the POST request to the Azure Logic app
                var jsonContent = new StringContent(JsonConvert.SerializeObject(emailContent), Encoding.UTF8, "application/json");
                var response = await httpClient.PostAsync(logicAppUrl, jsonContent);
                if (response.IsSuccessStatusCode)   //if email sent successfully
                {
                    Console.WriteLine("Email sent successfully!");
                }
                else
                {
                    Console.WriteLine("Failed to send email.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while sending the email: {ex.Message}");
            }
        }
    }
}
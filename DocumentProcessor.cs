namespace BG.Processor.Processors
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using BG.Processor.Settings;
    using Dapper;
    using Microsoft.Data.SqlClient;
    using Microsoft.EntityFrameworkCore.Metadata.Internal;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Renci.SshNet;

    public class DocumentProcessor : IProcessor
    {
        private readonly AppSettings appSettings;
        private readonly ApiSettings apiSettings;
        private readonly ILogger<DocumentProcessor> logger;
        private readonly IServiceRunRepository serviceRunRepository;
        private readonly HttpClient httpClient;
        private readonly string connectionStringOne;
        private readonly string ordersApi;
        private readonly string documentManagementConnection;
        private readonly IEmailService emailService;

        public DocumentProcessor (IOptions<AppSettings> appSettings, IOptions<ApiSettings> apiSettings, ILogger<DocumentProcessor> logger, IServiceRunRepository serviceRunRepository, IHttpClientFactory httpClient, IOptions<ConnectionStrings> connectionStrings, IEmailService emailService)
        {
            this.appSettings = appSettings.Value;
            this.apiSettings = apiSettings.Value;
            this.logger = logger;
            this.serviceRunRepository = serviceRunRepository;
            this.httpClient = httpClient.CreateClient();
            this.connectionStringOne = connectionStrings.Value.LimsConnection;   
            this.ordersApi = this.apiSettings.OrderManagement;
            this.documentManagementConnection = connectionStrings.Value.DocumentManagementConnection;
            this.emailService = emailService;
        }

        public string ProcessorName => this.GetType().Name;

        public void Process (params string[] env)
        {
            try
            {
                Task.Run(() => this.ProcessAsync()).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex.ToString());
            }
        }

        public async Task ProcessAsync()
        {
            var process = this.serviceRunRepository.Filter(p => p.ApplicationName == this.ProcessorName).FirstOrDefault();
            DateTime lastRunTime = process.LastRunDate.Value;
            DateTime currentRunTime = DateTime.Now;
            this.logger.LogInformation($"Starting {this.ProcessorName} -- Last run time: {lastRunTime}");
            
            string errorMessage = null;
            var errorEmailSubject = "DocumentProcessor error";
            var errorEmailBody = this.CreateEmailForFailedProcessing(currentRunTime, lastRunTime, errorMessage);

            try
            {
                var isReqLookUpSuccessful = await this.GetRecentlyUploadedReqs(lastRunTime).ConfigureAwait(false);
                var isReportLookUpSuccessful = await this.GetRecentlyUploadedReports(lastRunTime).ConfigureAwait(false);
                var isSampleDocLookUpSuccessful = await this.GetRecentSampleDocUploads(lastRunTime).ConfigureAwait(false);

                var reqStatusMessage = isReqLookUpSuccessful ? "Recent requisition uploads have been processed" : "There was an error processing recent requisition uploads.";
                var reportStatusMessage = isReportLookUpSuccessful ? "Recent report uploads have been processed" : "There was an error processing recent report uploads.";
                var sampleDocStatusMessage = isSampleDocLookUpSuccessful ? "Recent sample document uploads have been processed" : "There was an error processing recent sample document uploads.";

                if (isReqLookUpSuccessful && isReportLookUpSuccessful && isSampleDocLookUpSuccessful)
                {
                    this.UpdateProcessRunTime(process, currentRunTime);
                }
                else
                {
                    errorMessage = $"{reqStatusMessage} \n{reportStatusMessage} \n{sampleDocStatusMessage}";

                    this.emailService.SendEmail(this.appSettings.AppSupportEmail, this.appSettings.EmailSettings.From, errorEmailSubject, errorEmailBody, isBodyHtml: true);
                }
            }
            catch (Exception ex )
            {
                errorMessage = ex.Message;
                this.emailService.SendEmail(this.appSettings.AppSupportEmail, this.appSettings.EmailSettings.From, errorEmailSubject, errorEmailBody, isBodyHtml: true);
            }
        }

        public void UpdateProcessRunTime(ServiceRun serviceRun, DateTime currentRunDateTime)
        {
            serviceRun.LastRunDate = currentRunDateTime;
            this.serviceRunRepository.Update(serviceRun);
            this.serviceRunRepository.SaveChanges();
        }

        //retrieves the requisitions/files uploaded through GLCDE
        private async Task<bool> GetRecentlyUploadedReqs(DateTime lastRunTime)
        {
            var reqLocation = this.appSettings.QuadaxDocumentSettings.RequisitionsLocation;
            var labIdFolders = new[] { "DNALab", "MitoLab", "CytoLab", "BiochemLab" };
            var isTaskSuccessful = true;

            try
            {
                var labNumbersToSearchWith = new List<string>();

                foreach ( var labId in labIdFolders )
                {
                    var recentlyUpdatedLabNumFolders = Directory.GetDirectories(@$"{reqLocation}\{labId}")
                        .Where(dir => Directory.GetLastWriteTime(dir) >= lastRunTime);

                    if (recentlyUpdatedLabNumFolders.Any())
                    {
                        foreach ( var labNumFolder in recentlyUpdatedLabNumFolders )
                        {
                            var files = Directory.GetFiles(labNumFolder)
                                .Where(f => File.GetLastWriteTime(f) >= lastRunTime);

                            foreach ( var file in files )
                            {
                                var fileName = Path.GetFileNameWithoutExtension(file);
                            
                                if (char.IsNumber(fileName[0]))
                                {
                                    if (char.IsLetter(fileName[fileName.Length - 1]))
                                    {
                                        labNumbersToSearchWith.Add(fileName.Substring(0, fileName.Length - 1));
                                    }
                                    else
                                    {
                                        labNumbersToSearchWith.Add(fileName);
                                    }
                                }
                                else
                                {
                                    this.logger.LogError($"Unable to associate a lab number with this file: {file}");
                                    isTaskSuccessful = false;
                                }
                            }
                        }
                    }
                }

                if (labNumbersToSearchWith.Any())
                {
                    var samplesWithNewFiles = await this.GetLabNumbersThatHaveNewFiles(labNumbersToSearchWith, lastRunTime).ConfigureAwait(false);

                    if ( samplesWithNewFiles.Any())
                    {
                        foreach ( var sample in samplesWithNewFiles )
                        {
                            var labId = sample.LabId;
                            var sampleLabIdFolder = string.Empty;
                            
                            if (!string.IsNullOrWhiteSpace(labId))
                            {
                                switch (labId)
                                {
                                    case "BIO":
                                        sampleLabIdFolder = "BiochemLab";
                                        break;

                                    case "DNA":
                                        sampleLabIdFolder = "DNALab";
                                        break;

                                    case "CYT":
                                        sampleLabIdFolder = "CytoLab";
                                        break;

                                    case "MIT":
                                        sampleLabIdFolder = "MitoLab";
                                        break;
                                }
                                
                                var labNumber = sample.LabNumber;
                                var labNumFolder = (labNumber / 1000) * 1000;

                                var filepath = Path.Combine(reqLocation, sampleLabIdFolder, labNumFolder.ToString());
                                var newFiles = Directory.GetFiles(filepath, labNumber + "*")
                                    .Where(f => Directory.GetLastWriteTime(f) >= lastRunTime);
                                var index = 0;

                                foreach ( var file in newFiles )
                                {                              
                                    var dateSuffix = DateTime.Now.ToString("yyyyMMddHHmmss"); 
                                    var fileExtension = Path.GetExtension(file);

                                    var newFileName = index > 0 ?
                                        $"{sample.LabGroupId}_REQ_{dateSuffix}-{index}{fileExtension}" :
                                        $"{sample.LabGroupId}_REQ_{dateSuffix}{fileExtension}";
                                    var isSuccessfulUpload = this.ConfigureSftp(newFileName, file, sample.Id);

                                    if (isSuccessfulUpload)
                                    {
                                        var isLogUpdateSuccessful = await this.LogTransaction(sample.LabGroupId, sample.Id, DateTime.Now).ConfigureAwait(false);

                                        if (isLogUpdateSuccessful)
                                        {
                                            index++;
                                            continue;
                                        }
                                        else
                                        {
                                            isTaskSuccessful = false;
                                        }
                                    }
                                    else
                                    {
                                        isTaskSuccessful = false;
                                    }
                                }
                            }
                            else
                            {
                                this.logger.LogError($"Sample Id {sample.Id} does not have a Lab Id associated with it in vw_BillableSamples");
                                isTaskSuccessful = false;
                            }
                        }
                    }
                }
                else
                {
                    this.logger.LogInformation("Did not find any recent requisition uploads");
                }
                return isTaskSuccessful;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Error retrieving recently uploaded requisitions: {ex.Message}");
                return false;
            }
        }
        
        //retrieves reports/results uploaded through GLCDE
        private async Task<bool> GetRecentlyUploadedReports(DateTime lastRunTime)
        {
            var reportLocation = this.appSettings.DocumentSettings.ReportsLocation;
            var isTaskSuccessful = true;

            try
            {
                var sampleIdsToSearchWith = new List<string>();
                var recentlyUpdatedReportFolders = Directory.GetDirectories(reportLocation).Where(dir => Directory.GetLastWriteTime(dir) >= lastRunTime);

                if (recentlyUpdatedReportFolders.Any())
                {
                    foreach ( var folder in recentlyUpdatedReportFolders )
                    {
                        var folderFiles = Directory.GetFiles(folder).Where(f => File.GetLastWriteTime(f) >= lastRunTime);

                        foreach (var file in folderFiles )
                        {
                            var fileName = Path.GetFileNameWithoutExtension(file);

                            if (char.IsNumber(fileName[0]))
                            {
                                if (char.IsLetter(fileName[fileName.Length - 1]))
                                {
                                    sampleIdsToSearchWith.Add(fileName.Substring(0, fileName.Length - 1));
                                }
                                else
                                {
                                    sampleIdsToSearchWith.Add(fileName);
                                }
                            }
                            else
                            {
                                this.logger.LogError($"Unable to associate a sample Id with this file: {file}");
                                isTaskSuccessful = false;
                            }
                        }
                    }
                    
                    if (sampleIdsToSearchWith.Any())
                    {
                        var samplesWithNewReports = await this.GetSamplesThatHaveNewFiles(sampleIdsToSearchWith, true, lastRunTime).ConfigureAwait(false);

                        if (samplesWithNewReports.Any())
                        {
                            foreach ( var sample in samplesWithNewReports )
                            {
                                var sampleId = sample.Id;
                                var sampleFolder = (sampleId / 1000) * 1000;

                                var filePath = Path.Combine(reportLocation, sampleFolder.ToString());
                                var newReportFiles = Directory.GetFiles(filePath, sampleId + "*")
                                    .Where(samp => Directory.GetLastWriteTime(samp) >= lastRunTime);
                                var index = 0;

                                foreach ( var file in newReportFiles )
                                {
                                    var dateSuffix = DateTime.Now.ToString("yyyyMMddHHmmss");
                                    var fileExtension = Path.GetExtension(file);

                                    var newFileName = index > 0 ?
                                        $"{sample.LabGroupId}_TRF_{dateSuffix}-{index}{fileExtension}" :
                                        $"{sample.LabGroupId}_TRF_{dateSuffix}{fileExtension}";
                                    var isSuccessfulUpload = this.ConfigureSftp(newFileName, file, sampleId);

                                    if (isSuccessfulUpload)
                                    {
                                        var isLogUpdateSuccessful = await this.LogTransaction(sample.LabGroupId, sample.Id, DateTime.Now).ConfigureAwait(false);

                                        if (isLogUpdateSuccessful)
                                        {
                                            index++;
                                            continue;
                                        }
                                        else
                                        {
                                            isTaskSuccessful = false;
                                        }
                                    }
                                    else
                                    {
                                        isTaskSuccessful = false;
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        this.logger.LogInformation("Did not find any recent report uploads");
                    }
                }
                return isTaskSuccessful;                  
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Error retrieving recently uploaded reports: {ex.Message}");
                return false;
            }
        }
        
        //retrieves files uploaded through RARE
        private async Task<bool> GetRecentSampleDocUploads(DateTime lastRunTime)
        {
            var isTaskSuccessful = true;

            var queryRecentSampleDocUploads = @$"Select d.* 
                                                from DocumentUpload d with(nolock)
                                                    join TransactionLog t with(nolock)
                                                        on t.SampleId = d.SampleId
                                                where (CASE 
		                                                    WHEN t.AccessionUpdateStatus = 'OK' THEN t.AccessionUpdateDateTime 
		                                                    ELSE t.AccessionCreateDateTime
		                                                    END) < '{lastRunTime}'
		                                            AND d.CreatedDate > (CASE 
								                                            WHEN t.AccessionUpdateStatus = 'OK' THEN t.AccessionUpdateDateTime 
								                                            ELSE t.AccessionCreateDateTime
								                                            END)";

            try
            {
                using SqlConnection conn = new SqlConnection(this.connectionStringOne);
                var recentlyUploadedDocs = await conn.QueryAsync(queryRecentSampleDocUploads).ConfigureAwait(false);

                if (recentlyUploadedDocs.Any())
                {
                    var result = recentlyUploadedDocs.Select(s => new SampleDocumentUpload
                    {
                        Id = s.Id,
                        SampleId = s.SampleId,
                        DocumentId = s.DocumentId,
                        CreatedBy = s.CreatedBy,
                        CreatedDate = s.CreatedDate
                    });

                    var sampleIds = result.Select(s => s.SampleId.ToString()).ToList();
                
                    var samplesWithNewFiles = await this.GetSamplesThatHaveNewFiles(sampleIds, false, lastRunTime).ConfigureAwait(false);              

                    if (samplesWithNewFiles.Any())
                    {
                        foreach ( var sample in samplesWithNewFiles )
                        {
                            var sampleDocIds = result.Where(r => r.SampleId == sample.Id).Select(s => s.DocumentId);
                            var formatDocIds = string.Join(",", sampleDocIds);

                            var queryDocuments = @$"select d.Id, 
                                                            d.FileName, 
                                                            fp.FullPath as FilePath, 
                                                            d.CreatedOnDateTime as CreatedDate
                                                    from Documents d with(nolock) 
                                                        join Files f with(nolock) 
                                                            on d.FileId = f.Id
                                                        join FilePath fp with(nolock) 
                                                            on f.PathId = fp.Id
                                                    where d.Id in ({formatDocIds})";
                            using SqlConnection docConn = new SqlConnection(this.documentManagementConnection);
                            var documents = await docConn.QueryAsync(queryDocuments).ConfigureAwait(false);

                            if (documents.Any())
                            {
                                var docResult = documents.Select(d => new FileInfoDTO
                                {
                                    Id = d.Id,
                                    FileName = d.FileName,
                                    FilePath = d.FilePath,
                                    CreatedDate = d.CreatedDate
                                });
                                var eligIndex = 0;
                                var medRecIndex = 0;

                                foreach ( var doc in docResult )
                                {
                                    var dateSuffix = DateTime.Now.ToString("yyyyMMddHHmmss");
                                    var newFileName = string.Empty;
                                    var isSuccessfulUpload = false;
                                    var fileExtension = doc.FileName.Split('.').Last();

                                    if (doc.FileName.Contains("ELIG", StringComparison.OrdinalIgnoreCase))
                                    {
                                        newFileName = eligIndex > 0 ?
                                            $"{sample.LabGroupId}_ELIG_{dateSuffix}-{eligIndex}.{fileExtension}" :
                                            $"{sample.LabGroupId}_ELIG_{dateSuffix}.{fileExtension}";
                                        isSuccessfulUpload = this.ConfigureSftp(newFileName, doc.FilePath, sample.Id);
                                        eligIndex++;
                                    }
                                    else
                                    {
                                        newFileName = medRecIndex > 0 ?
                                            $"{sample.LabGroupId}_MEDREC_{dateSuffix}-{medRecIndex}.{fileExtension}" :
                                            $"{sample.LabGroupId}_MEDREC_{dateSuffix}.{fileExtension}";
                                        isSuccessfulUpload = this.ConfigureSftp(newFileName, doc.FilePath, sample.Id);
                                        medRecIndex++;
                                    }

                                    if (isSuccessfulUpload)
                                    {
                                        var isLogUpdateSuccessful = await this.LogTransaction(sample.LabGroupId, sample.Id, DateTime.Now).ConfigureAwait(false);

                                        if (isLogUpdateSuccessful)
                                        {
                                            continue;
                                        }
                                        else
                                        {
                                            isTaskSuccessful = false;
                                        }
                                    }
                                    else
                                    {
                                        isTaskSuccessful = false;
                                    }
                                }
                            }
                            else
                            {
                                this.logger.LogError($"Unable to find documents with these Ids: {formatDocIds}, Sample Id: {sample.Id}");
                                isTaskSuccessful = false;
                            }
                        }
                    }
                }
                return isTaskSuccessful;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Error retrieving data from DocumentUpload: {ex.Message}");
                return false;
            }
        }
   
        //GetRecentSampleDocUploads and GetRecentlyUploadedReports use this method
        //returns samples with new files using the sample Id
        private async Task<IEnumerable<BillableSampleDTO>> GetSamplesThatHaveNewFiles(List<string> sampleIds, bool isReport, DateTime lastRunTime)
        {
            var formatSampleIdList = string.Join(",", sampleIds.Distinct());

            var querySamplesInQ = @$"Select qb.* 
                                        from vw_BillableSamples b with(nolock) 
                                            join TransactionLog t 
                                                on t.SampleId = b.Id
                                        where b.Id in ({formatSampleIdList})
                                            and (CASE 
		                                            WHEN t.AccessionUpdateStatus = 'OK' THEN t.AccessionUpdateDateTime 
		                                            ELSE t.AccessionCreateDateTime
		                                            END) < '{lastRunTime}'";

            try
            {
                using SqlConnection conn = new SqlConnection(this.connectionStringOne);
                var samplesWithNewFiles = await conn.QueryAsync<BillableSampleDTO>(querySamplesInQ).ConfigureAwait(false);
                var logMessage = isReport ?
                    "Samples in vw_BillableSamples with recent report uploads: " + string.Join(", ", samplesWithNewFiles.Select(s => s.Id)) :
                    "Samples in vw_BillableSamples with recent sample document uploads: " + string.Join(", ", samplesWithNewFiles.Select(s => s.Id));

                if (samplesWithNewFiles.Any())
                {
                    this.logger.LogInformation(logMessage);
                }
                else
                {
                    this.logger.LogInformation("Unable to find any samples in vw_BillableSamples with recent " + (isReport ? "report uploads" : "sample document uploads"));
                }

                return samplesWithNewFiles;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Error retrieving data from vw_BillableSamples by sample Id: {ex.Message}");
                return null;
            }
        }
        
        //GetRecentlyUploadedReqs uses this method
        //returns samples with new files using the lab number
        private async Task<IEnumerable<BillableSampleDTO>> GetLabNumbersThatHaveNewFiles(List<string> labnumbers, DateTime lastRunTime)
        {
            var formatLabNumberList = string.Join(",", labnumbers.Distinct());

            var queryLabNumbers = @$"SELECT b.* 
                                             FROM vw_BillableSamples b with(nolock) 
                                                join TransactionLog t 
                                                    on t.SampleId = b.Id 
                                            WHERE b.LabNumber in ({formatLabNumberList})
                                                and (CASE 
		                                                WHEN t.AccessionUpdateStatus = 'OK' THEN t.AccessionUpdateDateTime 
		                                                ELSE t.AccessionCreateDateTime
		                                                END) < '{lastRunTime}'";

            try
            {
                using SqlConnection conn = new SqlConnection(this.connectionStringOne);
                var result = await conn.QueryAsync<BillableSampleDTO>(queryLabNumbers).ConfigureAwait(false);

                var logMessage = !result.Any() ?
                    "Unable to find any lab numbers in vw_BillableSamples with recently uploaded requisitions" :
                    "Lab numbers in vw_BillableSamples with recently uploaded requisitions: " + string.Join(", ", result.Select(r => r.LabNumber));

                this.logger.LogInformation(logMessage);

                return result;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Error retrieving data from vw_BillableSamples by lab number: {ex.Message}");
                return null;
            }
        }

        private async Task<Stream> GetFileFromFilePath(string filePath)
        {
            using HttpClient httpClient = new HttpClient();
            try
            {
                var requestUrl = $"{this.appSettings.ApiSettings.PlaceHolderApi}/Documents/download/external?filePath={filePath}";
                HttpResponseMessage response = await this.httpClient.GetAsync(requestUrl).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var responseBody = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                return responseBody;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Issue retrieving FileStream for file {filePath}: {ex.Message}");
                return null;
            }
        }

        private bool ConfigureSftp(string newFileName, string filePath, int sampleId)
        {
            try
            {
                var host = this.appSettings.DocumentSettings.Host;
                var username = this.appSettings.DocumentSettings.Username;
                var password = this.appSettings.DocumentSettings.Password;

                using SftpClient sftpClient = new SftpClient(host, 22, username, password);
                sftpClient.Connect();

                if (!sftpClient.IsConnected)
                {
                    throw new Exception($"Unable to connect to {host}");
                }

                sftpClient.ChangeDirectory("Test_Files");
                sftpClient.ChangeDirectory("UAT Test Files");             
                var directory = sftpClient.WorkingDirectory;

                var fileStream = this.GetFileFromFilePath(filePath);
                sftpClient.UploadFile(fileStream.Result, directory + $"/{newFileName}");              
 
                if (sftpClient.Exists($"{directory}/{newFileName}"))
                {
                    this.logger.LogInformation($"file {newFileName} has been uploaded to sftp in directory {directory}");
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Issue uploading file {filePath} to sftp for sample Id {sampleId}: {ex.Message}");
                return false;
            }
        }
    
        private async Task<bool> LogTransaction(int labGroupId, int sampleId, DateTime accessionUpdateDateTime)
        {

            var queryUpdateTransactionLog = @$"Update TransactionLog 
                                        Set AccessionUpdateDateTime = '{accessionUpdateDateTime}',
                                            AccessionUpdateStatus = 'OK'
                                        Where SampleId = {sampleId} and LabGroupId = {labGroupId}";

            try
            {
                using SqlConnection conn = new SqlConnection(this.connectionStringOne);

                await conn.OpenAsync().ConfigureAwait(false);
                var rowsAffected = await conn.ExecuteAsync(queryUpdateTransactionLog).ConfigureAwait(false); 
                
                return rowsAffected > 0 ? true : false;
            }
            catch (Exception ex)
            {
                this.logger.LogError($"Issue updating Transaction Log for sample Id {sampleId}: {ex.Message}");
                return false;
            }
        }

        private string CreateEmailForFailedProcessing(DateTime currentRunTime, DateTime lastRunTime, string errorMessage)
        {
            var builder = new StringBuilder();
            var header = "Error in document processor. Please check the Processor logs for more information.";

            builder.Append($"<h2 style='text-align:center'>{header}</h2>");
            builder.Append($"<div style='text-align:center'>{errorMessage}</div>");
            builder.Append($"<div style='text-align:center'> Last Run Time: {lastRunTime} \n Current Run Time: {currentRunTime}</div>");
            return builder.ToString();
        }
    }
}

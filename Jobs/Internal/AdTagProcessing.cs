using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Internal.AdTagProcessing;
using Greenhouse.Data;
using Greenhouse.Data.Model.AdTag;
using Greenhouse.Data.Model.AdTag.APIAdServer;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Mail;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Text.RegularExpressions;
using APIAdServerRequest = Greenhouse.Data.Model.AdTag.APIAdServer.APIAdServerRequest;

namespace Greenhouse.Jobs.Internal;

[Export("AdTagProcessingJob", typeof(IDragoJob))]
public partial class AdTagProcessing : Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private static readonly AdTagBaseRepository<JobRun> jobRunRepo = new AdTagBaseRepository<JobRun>();
    private static readonly BaseRepository<Lookup> lookupRepo = new BaseRepository<Lookup>();
    private static readonly PlacementRepository placementRepository = new PlacementRepository();
    private static readonly AdTagBaseRepository<APIAdServerRequest> apiAdServerRequestRepo = new AdTagBaseRepository<APIAdServerRequest>();


    private static OAuthAuthenticator oAuthAuthenticator;
    private IHttpClientProvider _httpClientProvider;
    private ApiClient _apiClient;

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;

        Lookup adTagConnectionString = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_CONNECTIONSTRING);
        string baseUri = Data.Services.SetupService.GetById<Data.Model.Setup.Lookup>(Constants.ADTAG_BASE_URI).Value;

        var credentials = new Credential(adTagConnectionString.Value);

        oAuthAuthenticator = new OAuthAuthenticator(HttpClientProvider,
            credentials.CredentialSet.username,
            credentials.CredentialSet.clientId,
            credentials.CredentialSet.clientSecret,
            credentials.CredentialSet.refreshToken
        );
        _apiClient = new(_httpClientProvider, oAuthAuthenticator, baseUri);
    }

    public void Execute()
    {
        Lookup keepJobRunDuration = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_KEEP_JOBRUN_FOR_N_DAYS);

        //Cleanup Job Log
        AdTagUtilitiesRepository.CleanupJobRun(keepJobRunDuration.Value);

        //Create Temp Directory and Clean Up old report files
        CleanUpFolder();

        //Start the AgTag Process
        List<string> reports = ProcessAdTag();

        SendNotificationEmail(reports);

        //NLogger Log
        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, "Completed"));
    }

    public void PostExecute()
    {
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {

        }
    }

    ~AdTagProcessing()
    {
        Dispose(false);
    }

    private List<string> ProcessAdTag()
    {
        List<string> reports = new List<string>();

        //Get All Active API Ad Server Requests from DB
        List<APIAdServerRequestMapping> adServerRequestMappings = APIAdServerRequestRepository.GetAllAPIAdServerRequestMappings();
        if (adServerRequestMappings == null || adServerRequestMappings.Count == 0) return reports;

        int adServerCounter = 0;
        var jobRun = new JobRun();
        string advertiserId = "", advertiserName = "";

        try
        {
            foreach (APIAdServerRequestMapping adServerRequestMapping in adServerRequestMappings)
            {
                jobRun = new JobRun();
                adServerCounter++;

                advertiserId = adServerRequestMapping.AdvertiserId;
                advertiserName = adServerRequestMapping.AdvertiserName;

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - Processing API Ad Server Request {adServerCounter.ToString()} of {adServerRequestMappings.Count.ToString()}." +
                    $" Username: {adServerRequestMapping.UserName}, Profile ID: {adServerRequestMapping.ProfileId}, Account ID: {adServerRequestMapping.AccountId}, " +
                    $"Advertiser ID: {adServerRequestMapping.AdvertiserId}, IsOutputToReport: {adServerRequestMapping.IsOutputToReport}"));

                //If Profile ID or Account ID is null in the database, log the error and go to the next record.
                if (string.IsNullOrEmpty(adServerRequestMapping.ProfileId) ||
                    string.IsNullOrEmpty(adServerRequestMapping.AccountId) ||
                    string.IsNullOrEmpty(adServerRequestMapping.AdvertiserId))
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        $"{this.JED.JobGUID.ToString()} - Unable to process API Ad Server Request. Missing Profile ID or Account ID or Advertiser ID. Username: {adServerRequestMapping.UserName}, " +
                        $"Profile ID: {adServerRequestMapping.ProfileId}, Account ID: {adServerRequestMapping.AccountId}, " +
                        $"Advertiser ID: {adServerRequestMapping.AdvertiserId}, IsOutputToReport: {adServerRequestMapping.IsOutputToReport}"));
                    continue;
                }

                string reportName = ProcessPlacements(adServerRequestMapping, jobRun);

                if (!string.IsNullOrEmpty(reportName)) reports.Add(reportName);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - Processed API Ad Server Request. Username: {adServerRequestMapping.UserName}, Profile ID: {adServerRequestMapping.ProfileId}, " +
                    $"Account ID: {adServerRequestMapping.AccountId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, IsOutputToReport: {adServerRequestMapping.IsOutputToReport}"));

                //If it is to output to report and write to report status is true, reset the WriteToReportStatus to false.
                if (adServerRequestMapping.IsOutputToReport && adServerRequestMapping.WriteToReportStatus)
                {
                    //Update the WriteToReportStatus to "False"
                    APIAdServerRequest adServerRequest = new APIAdServerRequest();
                    adServerRequest.APIAdServerRequestID = adServerRequestMapping.APIAdServerRequestID;
                    adServerRequest.LastUpdated = DateTime.Now;
                    adServerRequest.LastImportDate = DateTime.Now;
                    adServerRequest.WriteToReportStatus = false;
                    apiAdServerRequestRepo.Update(adServerRequest);

                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                        $"{this.JED.JobGUID.ToString()} - Updated APIAdServerRequest. Set WriteToReportStatus to False. APIAdServerRequestID: {adServerRequest.APIAdServerRequestID.ToString()}, WriteToReportStatus: {adServerRequest.WriteToReportStatus.ToString()}"));
                }
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            HandleException(jobRun, advertiserId, advertiserName, ex);
        }
        catch (Exception ex)
        {
            HandleException(jobRun, advertiserId, advertiserName, ex);
        }

        return reports;
    }

    private void HandleException<TException>(JobRun jobRun, string advertiserId, string advertiserName, TException ex) where TException : Exception
    {
        //Check if Job Log has been created. 
        //Job Log is created only if Placements were found.
        //If Placements were not found and an error occurs at Advertiser level, create Job Log
        UpdateJobRun(jobRun, ex, advertiserId, advertiserName);
        var logMessage = BuildLogErrorMessage(ex);
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, logMessage));
    }

    private string BuildLogErrorMessage<TException>(TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"{this.JED.JobGUID} - Exception details : {httpEx}",
            _ => $"{JED.JobGUID} - ERROR: {exception.Message}{System.Environment.NewLine}Stack Trace:{System.Environment.NewLine}{exception.StackTrace}"
        };
    }
    private void UpdateJobRun(JobRun jobRun, Exception ex, string advertiserId, string advertiserName)
    {
        if (string.IsNullOrEmpty(jobRun.Status))
        {
            jobRun.Status = Constants.JobRunStatus.Error.ToString();
            jobRun.Message = $"{this.JED.JobGUID} - Error Message: {ex.Message}{System.Environment.NewLine}Stack Trace:{System.Environment.NewLine}{ex.StackTrace}";
            jobRun.AdvertiserId = Convert.ToInt64(advertiserId);
            jobRun.AdvertiserName = advertiserName;
            jobRunRepo.Add(jobRun);
        }
        else
        {
            jobRun.Status = Constants.JobRunStatus.Error.ToString();
            jobRun.Message = $"{this.JED.JobGUID} - Error Message: {ex.Message}{System.Environment.NewLine}Stack Trace:{System.Environment.NewLine}{ex.StackTrace}";
            jobRun.AdvertiserId = Convert.ToInt64(advertiserId);
            jobRun.AdvertiserName = advertiserName;
            UpdateJobRun(jobRun);
        }
    }

    private string ProcessPlacements(APIAdServerRequestMapping adServerRequestMapping, JobRun jobRun)
    {
        string reportName = null;
        //Get the last processed Placement ID                        
        var advertiserJobDetailRepo = new AdTagBaseRepository<AdvertiserJobDetail>();
        AdvertiserJobDetail advertiserJobDetail = advertiserJobDetailRepo.GetById(Convert.ToInt64(adServerRequestMapping.AdvertiserId));

        //If Last Processed Placement not found, create a new one and set ID to 0.
        if (advertiserJobDetail == null)
        {
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Last Processed Placement ID was not found for Advertiser ID: {adServerRequestMapping.AdvertiserId}"));
            advertiserJobDetail = new AdvertiserJobDetail();
            advertiserJobDetail.AdvertiserID = Convert.ToInt64(adServerRequestMapping.AdvertiserId);
            advertiserJobDetail.LastProcessedPlacementID = 0;
            advertiserJobDetailRepo.Add(advertiserJobDetail);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Created Last Processed Placement ID for Advertiser ID: {adServerRequestMapping.AdvertiserId}"));
        }

        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Advertiser ID: {adServerRequestMapping.AdvertiserId}, Last Processed Placement ID: {advertiserJobDetail.LastProcessedPlacementID.ToString()}"));

        //Get a list of placements 

        DateTime startDate;

        if (adServerRequestMapping.LastImportDate.HasValue)
        {
            int adTagOffsetDays = Convert.ToInt16(lookupRepo.GetById(Constants.ADTAG_OFFSET_DAYS).Value);
            startDate = adServerRequestMapping.LastImportDate.Value.AddDays(-adTagOffsetDays);
        }
        else
        {
            int noOfMinStartDate = Convert.ToInt16(lookupRepo.GetById(Constants.ADTAG_NO_OF_MINSTARTDATE).Value);
            startDate = DateTime.Now.AddDays(-1 * noOfMinStartDate);
        }

        var options = new GetAllDCMPlacementsOptions()
        {
            ProfileId = adServerRequestMapping.ProfileId,
            AdvertiserID = adServerRequestMapping.AdvertiserId,
            StartDate = startDate
        };

        List<Placement> placements = _apiClient.GetAllDCMPlacementsAsync(options).GetAwaiter().GetResult();

        if (placements != null && placements.Count != 0)
        {
            //Create and Insert a Job Log Entry to the database                
            jobRun.Status = Constants.JobRunStatus.Running.ToString();
            jobRun.AdvertiserId = Convert.ToInt64(adServerRequestMapping.AdvertiserId);
            jobRun.AdvertiserName = adServerRequestMapping.AdvertiserName;

            Placement firstPlacement = placements.Last();
            jobRun.StartPlacementId = Convert.ToInt64(firstPlacement.Id);

            jobRun.HasuValueError = false;
            jobRun.PlacementsModified = 0;
            jobRunRepo.Add(jobRun);

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"{this.JED.JobGUID.ToString()} - Profile ID: {adServerRequestMapping.ProfileId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, " +
                $"Last Processed Placement ID: {advertiserJobDetail.LastProcessedPlacementID.ToString()}, No. of Unprocessed Placements: {placements.Count.ToString()}"));

            List<Placement> placementsWithNewKeyValue = RetrieveAdditionalKeyValues(placements, adServerRequestMapping);

            //Check if Output to Report or Call API to Update the AdditionalKeyValue
            if (adServerRequestMapping.IsOutputToReport)
            {
                reportName = OutputAdditionalKeyValues(adServerRequestMapping, placements);
            }
            else
            {
                UpdateAdditionalKeyValues(adServerRequestMapping, placementsWithNewKeyValue);
            }

            //Update the Last Processed Placement ID                         
            advertiserJobDetail.LastProcessedPlacementID = Convert.ToInt64(placements[0].Id);
            advertiserJobDetail.LastUpdated = DateTime.Now;
            advertiserJobDetailRepo.Update(advertiserJobDetail);

            //Update the Last Import Date
            APIAdServerRequest adServerRequest = new APIAdServerRequest();
            adServerRequest.APIAdServerRequestID = adServerRequestMapping.APIAdServerRequestID;
            adServerRequest.LastUpdated = DateTime.Now;
            adServerRequest.LastImportDate = DateTime.Now;
            apiAdServerRequestRepo.Update(adServerRequest);

            //Update Job Log Entry to mark it as complete                
            jobRun.PlacementsModified = placementsWithNewKeyValue.Count;
            jobRun.HasuValueError = placements.Any(p => string.IsNullOrEmpty(p.TagSetting.AdditionalKeyValues));
            jobRun.LastPlacementId = advertiserJobDetail.LastProcessedPlacementID;
            jobRun.Status = (jobRun.Status != Constants.JobRunStatus.Error.ToString() ? Constants.JobRunStatus.Complete.ToString() : jobRun.Status);
            UpdateJobRun(jobRun);

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"{this.JED.JobGUID.ToString()} - Updated Last Processed Placement ID. Advertiser ID: {adServerRequestMapping.AdvertiserId}, " +
                $"Last Processed Placement ID: {advertiserJobDetail.LastProcessedPlacementID.ToString()}"));
        }
        else
        {
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"{this.JED.JobGUID.ToString()} - No Unprocessed Placements Found. Profile ID: {adServerRequestMapping.ProfileId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, " +
                $"Last Processed Placement ID: {advertiserJobDetail.LastProcessedPlacementID.ToString()}"));

            jobRun.AdvertiserId = Convert.ToInt64(adServerRequestMapping.AdvertiserId);
            jobRun.AdvertiserName = adServerRequestMapping.AdvertiserName;
            jobRun.HasuValueError = false;
            jobRun.PlacementsModified = 0;
            jobRun.Status = Constants.JobRunStatus.Complete.ToString();
            jobRunRepo.Add(jobRun);
        }
        return reportName;
    }

    private List<Placement> RetrieveAdditionalKeyValues(List<Placement> placements, APIAdServerRequestMapping adServerRequestMapping)
    {
        var updatedPlacements = new List<Placement>();
        int placementCounter = 0, placementTotal = placements.Count;
        foreach (Placement placement in placements)
        {
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID} - Updating Placement {(++placementCounter)} of {placementTotal}. PlacementID: {placement.Id}, Campaign ID: {placement.CampaignId}."));

            if (string.IsNullOrEmpty(placement.TagSetting.AdditionalKeyValues))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - placement.TagSetting.AdditionalKeyValues is NULL or empty. CampaignID: {placement.CampaignId}, PlacementID: {placement.Id}, " +
                    $"PlacementName: {placement.Name}, AdditionalKeyValues: null"));

                placement.TagSetting.AdditionalKeyValues = placementRepository.GetAdditionalKeyValues(adServerRequestMapping.AccountId, adServerRequestMapping.AdvertiserId, placement.Name, placement.Id);

                if (!string.IsNullOrEmpty(placement.TagSetting.AdditionalKeyValues))
                {
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                        $"{this.JED.JobGUID.ToString()} - Retrieved AdditionalKeyValues from GenerateuValue. Campaign ID: {placement.CampaignId}, Placement ID: {placement.Id}, " +
                        $"Account ID: {adServerRequestMapping.AccountId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, Placement Name: {placement.Name}, " +
                        $"AdditionalKeyValues: {placement.TagSetting.AdditionalKeyValues}"));

                    updatedPlacements.Add(placement);
                }
            }
        }

        return updatedPlacements;
    }

    private string OutputAdditionalKeyValues(APIAdServerRequestMapping adServerRequestMapping, List<Placement> placements)
    {
        Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

        string recipients = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_REPORT_FOLDER_EMAIL).Value;
        string filepath = GetTempFileFolderPath();
        string filename = string.Format("{0}-{1}-{2}.xlsx", adServerRequestMapping.AdvertiserName, adServerRequestMapping.AdvertiserId, unixTimestamp.ToString());
        string additionalKeyValues = string.Empty;
        Regex specialChars = SpecialCharsRegex();
        Regex spaces = SpacesRegex();
        filename = specialChars.Replace(filename, "");
        filename = spaces.Replace(filename, " ");
        filename = filename.Replace(' ', '_');
        string fullPathFileName = string.Format("{0}{1}", filepath, filename);

        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Creating Excel Report. Advertiser ID: {adServerRequestMapping.AdvertiserId}, Filename: {fullPathFileName}"));

        GenerateExcelFile(adServerRequestMapping, placements, fullPathFileName);

        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Sent Report to BOX. Advertiser ID: {adServerRequestMapping.AdvertiserId}, Recipients: {recipients}, Filename: {fullPathFileName}"));

        //Upload Report to BOX through email.
        UploadReport(recipients, fullPathFileName);
        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Sent Report to BOX. Advertiser ID: {adServerRequestMapping.AdvertiserId}, Recipients: {recipients}, Filename: {fullPathFileName}"));

        //Build a List of Reports
        return filename;
    }

    private void GenerateExcelFile(APIAdServerRequestMapping adServerRequestMapping, List<Placement> placements,
        string fullPathFileName)
    {
        using (SpreadsheetDocument document = SpreadsheetDocument.Create(fullPathFileName, SpreadsheetDocumentType.Workbook))
        {
            var worksheetPart = InitializeExelSheetWorksheetPart(document, out SheetData sheetData);

            // Inserting each placement
            int placementCounter = 0;
            foreach (Placement placement in placements)
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - Outputting Placement {(++placementCounter).ToString()} of {placements.Count.ToString()}. PlacementID: {placement.Id}, Campaign ID: {placement.CampaignId}."));

                Row row = new Row();
                row.Append(
                    ConstructCell(placement.Id, CellValues.String),
                    ConstructCell(placement.Name, CellValues.String),
                    ConstructCell(placement.TagSetting.AdditionalKeyValues, CellValues.String));
                //ConstructCell(placement.TagSetting.AdditionalKeyValues, CellValues.String));

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - Retrieved AdditionalKeyValues from GenerateuValue. Campaign ID: {placement.CampaignId}, Placement ID: {placement.Id}, Account ID: {adServerRequestMapping.AccountId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, Placement Name: {placement.Name}, AdditionalKeyValues: {placement.TagSetting.AdditionalKeyValues}"));

                sheetData.AppendChild(row);
            }

            worksheetPart.Worksheet.Save();

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"{this.JED.JobGUID.ToString()}  Created Excel Report. Advertiser ID: {adServerRequestMapping.AdvertiserId}, Filename: {fullPathFileName}"));
        }
    }

    private static WorksheetPart InitializeExelSheetWorksheetPart(SpreadsheetDocument document, out SheetData sheetData)
    {
        WorkbookPart workbookPart = document.AddWorkbookPart();
        workbookPart.Workbook = new Workbook();

        WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
        worksheetPart.Worksheet = new Worksheet();

        Sheets sheets = workbookPart.Workbook.AppendChild(new Sheets());

        Sheet sheet = new Sheet() { Id = workbookPart.GetIdOfPart(worksheetPart), SheetId = 1, Name = "Placements" };

        sheets.Append(sheet);

        workbookPart.Workbook.Save();

        sheetData = worksheetPart.Worksheet.AppendChild(new SheetData());

        // Constructing header
        Row row = new Row();
        row.Append(
            ConstructCell("Placement ID", CellValues.String),
            ConstructCell("Placement Name", CellValues.String),
            ConstructCell("Ad Tag Modifier Key-Value Pairs", CellValues.String));
        //ConstructCell("Existing AdditionalKeyValues", CellValues.String));

        // Insert the header row to the Sheet Data
        sheetData.AppendChild(row);
        return worksheetPart;
    }

    public static void UploadReport(string emailRecipients, string attachmentFilename)
    {
        string senderEmail = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_SENDER_EMAIL).Value;

        StringBuilder htmlBuilder = new StringBuilder();
        string subject = "AdTag Advertiser Report";
        htmlBuilder.AppendLine("<div style='width:500px;'>");
        htmlBuilder.AppendFormat("<p>Date: {0}</p>", DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
        htmlBuilder.AppendLine("</div>");
        SendMailMessage(subject, senderEmail, htmlBuilder.ToString(), attachmentFilename, emailRecipients.Split(";".ToCharArray()).ToList());
    }

    private static void SendNotificationEmail(List<string> reports)
    {
        //Send Notification Email after going through all Advertiser to send one summary email with all the reports that were uploaded.
        if (reports != null && reports.Count > 0)
        {
            string reportURL = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_REPORT_FOLDER_URL).Value;
            string reportEmail = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_REPORT_EMAIL).Value;

            SendReportNotification(reportEmail, reportURL, reports);
        }
    }

    private static void UpdateJobRun(JobRun jobRun)
    {
        jobRun.LastUpdated = DateTime.Now;
        jobRunRepo.Update(jobRun);
    }

    private void UpdateAdditionalKeyValues(APIAdServerRequestMapping adServerRequestMapping, List<Placement> updatedPlacement)
    {
        string responseString = string.Empty;
        int placementCounter = 0;
        foreach (Placement placement in updatedPlacement)
        {
            placementCounter++;

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID.ToString()} - Updating Placement {placementCounter.ToString()} of {updatedPlacement.Count.ToString()}. PlacementID: {placement.Id}, Campaign ID: {placement.CampaignId}."));

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"{this.JED.JobGUID.ToString()} - Retrieved AdditionalKeyValues from GenerateuValue. Campaign ID: {placement.CampaignId}, Placement ID: {placement.Id}, " +
                $"Account ID: {adServerRequestMapping.AccountId}, Advertiser ID: {adServerRequestMapping.AdvertiserId}, Placement Name: {placement.Name}, " +
                $"AdditionalKeyValues: {placement.TagSetting.AdditionalKeyValues}"));

            var options = new UpdateDCMPlacementOptions()
            {
                ProfileId = adServerRequestMapping.ProfileId,
                Placement = placement,
            };

            responseString = _apiClient.UpdateDCMPlacementAsync(options).GetAwaiter().GetResult();

            var updatedDcmPlacement = ETLProvider.DeserializeType<Placement>($"{responseString}");

            if (!string.IsNullOrEmpty(updatedDcmPlacement?.TagSetting?.AdditionalKeyValues))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - SUCCESS: UpdateDCMPlacement. AdditionalKeyValues changed to: {updatedDcmPlacement?.TagSetting?.AdditionalKeyValues} for Placement ID: {updatedDcmPlacement?.Id}"));
            }
            else
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    $"{this.JED.JobGUID.ToString()} - WARNING - AdditionalKeyValues is null in DCM after patch request for Placement ID: {placement.Id}, Campaign ID: {placement.CampaignId}, " +
                    $"AdditionalKeyValues - Sent: {placement.TagSetting.AdditionalKeyValues}, AdditionalKeyValues - Received: {updatedDcmPlacement?.TagSetting?.AdditionalKeyValues}"));
            }
        }
    }

    private static string GetTempFileFolderPath()
    {
        Lookup adTagTempFileFolder = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_TEMP_FOLDER);

        return $"{Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseRawPath}\\{adTagTempFileFolder.Value}\\";
    }

    public static void CleanUpFolder()
    {
        Lookup keepFileDuration = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_KEEP_FILES_FOR_N_DAYS);

        string folderPath = GetTempFileFolderPath();

        int noOfDays = int.Parse(keepFileDuration.Value);

        //Create Directory if it doesn't exist
        Directory.CreateDirectory(folderPath);

        //Delete Old Files
        string[] files = Directory.GetFiles(folderPath);

        foreach (string file in files)
        {
            FileInfo fi = new FileInfo(file);
            if (fi.CreationTime < DateTime.Now.AddDays(-1 * noOfDays))
                fi.Delete();
        }
    }

    public static void SendReportNotification(string emailRecipients, string reportFolderURL, List<string> reports)
    {
        try
        {
            string senderEmail = Data.Services.SetupService.GetById<Lookup>(Constants.ADTAG_SENDER_EMAIL).Value;

            StringBuilder htmlBuilder = new StringBuilder();
            string subject = "AdTag Advertiser Reports";

            htmlBuilder.AppendLine("<div style='width:500px;'>");
            htmlBuilder.AppendFormat("The following Ad Tag Modification pre-launch reports have been uploaded to lionbox ({0}):", reportFolderURL);
            htmlBuilder.AppendLine("<ul>");
            foreach (string report in reports)
            {
                htmlBuilder.AppendFormat("<li>{0}</li>", report);
            }
            htmlBuilder.AppendLine("</ul></div>");
            SendMailMessage(subject, senderEmail, htmlBuilder.ToString(), null, emailRecipients.Split(";".ToCharArray()).ToList());
        }
        catch
        {
            throw;
        }
    }

    public static void SendMailMessage(string subject, string sender, string messageBody, string attachmentFilename, List<string> emailRecipients)
    {
        foreach (string recipient in emailRecipients)
        {
            MailMessage mailMessage = new MailMessage(sender, recipient, subject, messageBody);
            mailMessage.IsBodyHtml = true;

            if (!string.IsNullOrEmpty(attachmentFilename))
                mailMessage.Attachments.Add(new Attachment(attachmentFilename));
            Mail.Clients.SMTPMailClient client = new Mail.Clients.SMTPMailClient();
            var mailResult = client.SendMessage(mailMessage);
            if (mailResult == null || !MailResult.Success)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"Error Sending Email in AdTagProcessing.SendMailMessage - Exception: {mailResult?.Error?.Message} - Recipent(s): {recipient} - Email Body: {messageBody}"));
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Email sent to: {0}.", recipient)));
        }
    }

    private static Cell ConstructCell(string value, CellValues dataType)
    {
        return new Cell()
        {
            CellValue = new CellValue(value),
            DataType = new EnumValue<CellValues>(dataType)
        };
    }

    [GeneratedRegex("[\\\\/:*?\"'<>|]", RegexOptions.None)]
    private static partial Regex SpecialCharsRegex();

    [GeneratedRegex("[ ]{2,}", RegexOptions.None)]
    private static partial Regex SpacesRegex();
}

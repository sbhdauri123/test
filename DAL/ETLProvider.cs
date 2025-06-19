using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.DBM;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using ICSharpCode.SharpZipLib.Zip;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Greenhouse.DAL;

public partial class ETLProvider
{
    private static readonly Logger logger = LogManager.GetCurrentClassLogger();
    public string JobLogGUID { get; private set; }
    public void SetJobLogGUID(string guid)
    {
        JobLogGUID = guid;
    }

    private static Credential GreenhouseAWSCredential
    {
        get
        {
            return Credential.GetGreenhouseAWSCredential();
        }
    }

    private readonly HivePartitionRepository repo = new HivePartitionRepository();
    private readonly InstanceAdvertiserMappingRepository instanceAdvertiserMappingRepository = new InstanceAdvertiserMappingRepository();

    internal static string ConnectionString(bool useDimensionDB = false)
    {
        return useDimensionDB
            ? Configuration.Settings.Current.Greenhouse.GreenhouseDimDbConnectionString
            : Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
    }

    private static readonly string[] sourceArray = [".csv", ".gz"];

    [Obsolete]
    public void LoadLogFile(Uri sourcePath, int maxNbLogFileToImport, bool truncateStaging = false, int commandTimeout = 120)
    {
        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();
        var subList = UtilsText.GetSublistFromList(s3Dir.GetFiles(), maxNbLogFileToImport);
        try
        {
            foreach (var subListFiles in subList)
            {
                List<IFile> files = subListFiles.ToList();

                var dt = new DataTable("[dbo].[partition_stage]");
                dt.Columns.Add("DateTime");
                dt.Columns.Add("DataSource");
                dt.Columns.Add("FileType");
                dt.Columns.Add("AdvertiserID");
                dt.Columns.Add("Year");
                dt.Columns.Add("Month");
                dt.Columns.Add("Day");
                dt.Columns.Add("Hour");
                dt.Columns.Add("FileGUID", typeof(Guid));
                dt.Columns.Add("PartitionPath");

                files.ForEach(file =>
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start Downloading log file: {0}", file.FullName, JobLogGUID)));
                    using (var sr = new StreamReader(file.Get()))
                    {
                        var strContent = sr.ReadToEnd();
                        var content = strContent.Split(Constants.TAB_ARRAY, dt.Columns.Count);
                        dt.LoadDataRow(content, true);
                    }
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Finished Downloading log file: {0}", file.FullName, JobLogGUID)));
                });

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Downloaded total row count: {0}", dt.Rows.Count, JobLogGUID)));

                using (SqlConnection connection = new SqlConnection(ConnectionString(false)))
                {
                    try
                    {
                        connection.Open();
                        string tableName = "[dbo].[partition_stage]";
                        using (SqlBulkCopy sbc = new SqlBulkCopy(connection))
                        {
                            var cmd = connection.CreateCommand();
                            if (truncateStaging)
                            {
                                cmd.CommandText = string.Format("Truncate table {0}", tableName);
                                cmd.Connection = connection;
                                cmd.CommandType = CommandType.Text;
                                var result = cmd.ExecuteNonQuery();
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - truncated staging table: {0}", tableName, JobLogGUID)));
                            }

                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start inserting data", JobLogGUID)));
                            //TODO: reference sourcefile from S3.
                            sbc.BulkCopyTimeout = 6000;
                            sbc.DestinationTableName = tableName;
                            sbc.BatchSize = 100000;
                            sbc.WriteToServer(dt);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Finished inserting data", JobLogGUID)));
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start executing proc [dbo].[ProcessPartitions]", JobLogGUID)));

                            cmd.CommandText = "[dbo].[ProcessPartitions]";
                            cmd.CommandTimeout = commandTimeout;
                            cmd.ExecuteNonQuery();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Finished executing proc [dbo].[ProcessPartitions]", JobLogGUID)));
                        }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {						
                    }
                    catch (Exception exc)
                    {
                        logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID)));
                        throw;
                    }
                }//end using connection

                //Archive only after succuessfully loading the log file(s)
                files.ForEach(file =>
                {
                    //Archive file after loading			
                    var destUri = RemoteUri.CombineUri(s3Dir.Uri, "archive", file.Name);
                    var destFile = new S3File(destUri, GreenhouseAWSCredential);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start archiving file {0}", destFile.FullName, JobLogGUID)));
                    file.CopyTo(destFile, true);
                    file.Delete();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Finished archiving file {0}", destFile.FullName, JobLogGUID)));
                });
            }
        }
        catch (HttpClientProviderRequestException exception)
        {
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"{JobLogGUID} |ETLProvider | ${nameof(LoadLogFile)} | Exception details : {exception}"));
            throw;
        }
        catch (Exception)
        {
            throw;
        }
    }

    public void PuttySSH(string emrClusterID, string hiveCommandFile, bool isAdvertiser, HivePartition partition)
    {
        var puttyPath = @"c:\putty\putty.exe";
        var cmdArgs = string.Format(" -ssh -2 -C -v -l hadoop -load \"{0}\" -m {1}", emrClusterID, hiveCommandFile);

        #region SSH
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Start Executing putty command: {0} {1}", puttyPath, cmdArgs, JobLogGUID)));
        var cmd = new Process();
        var startInfo = new ProcessStartInfo(puttyPath, cmdArgs);
        startInfo.WindowStyle = ProcessWindowStyle.Minimized;
        cmd.StartInfo = startInfo;
        cmd.Start();
        cmd.WaitForExit();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - End Executing putty command: {0} {1}", puttyPath, cmdArgs, JobLogGUID)));
        #endregion

        int count = 0;
        #region Update IsInMetaStore flag in DB
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Updating IsInMetaStore flag to 1 in database: {0} {1}", puttyPath, cmdArgs, JobLogGUID)));
        if (isAdvertiser)
        {
            count = instanceAdvertiserMappingRepository.UpdateAdvertiserMapping(partition.InstanceID, partition.AdvertiserMappingID);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{3} - Updated {0} records for instanceID: {1}, advertisingmappingID: {2}", count, partition.InstanceID, partition.AdvertiserMappingID, JobLogGUID)));
        }
        else
        {
            var idsToUpdate = new List<Tuple<long, string, bool>>();
            idsToUpdate.Clear();
            idsToUpdate.Add(Tuple.Create(partition.PartitionID, partition.PartitionPath, false));
            count = repo.UpdatePartitions(idsToUpdate, false);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{3} - Updated {0} records for partitionpath: {1}, advertisingmappingID: {2}", count, partition.PartitionPath, partition.AdvertiserMappingID, JobLogGUID)));
        }
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Finished IsInMetaStore flag to 1 in database: {0} {1}", puttyPath, cmdArgs, JobLogGUID)));
        #endregion

        return;
    }

    #region DCM
    [Obsolete]
    public void LoadDCMMetadata(Guid guid, Uri sourcePath, int sourceID, int integrationID, int countryID, IList<FileCollectionItem> files)
    {
        if (files == null || files.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException(JobLogGUID + " - LoadDCMMetadata failed, because no files were passed in");
        }

        var strGruid = guid.ToString().Replace("-", string.Empty).ToString();

        string pattern = @"[\s\(\)\-\/\:\/]+";
        var regEx = new Regex(pattern);

        var guidParam = new SqlParameter(@"@FileGUID", strGruid);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        //Have to break temp table creations into two steps. Doing it in a proc creates them in a separate scope.
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        //TODO: reference sourcefile from S3.
                        sbc.BulkCopyTimeout = 12000;
                        foreach (var file in files)
                        {
                            sbc.ColumnMappings.Clear();
                            string tableName = string.Format("dcm.#dim{0}_{1}", file.SourceFileName, strGruid);
                            sbc.DestinationTableName = tableName;
                            sbc.BatchSize = 100000;

                            var s3FilePath = RemoteUri.CombineUri(s3Dir.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Start Processing Filename: {0}, s3Path: {1}", file.FilePath, s3FilePath, JobLogGUID)));

                            var s3FileStream = rac.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Retrieved S3 File Stream for file: {0}", file.FilePath, JobLogGUID)));

                            using (var zipStream = new GZipInputStream(s3FileStream))
                            {
                                using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(new StreamReader(zipStream), true))
                                {
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start Streaming CSV file: {0}", file.FilePath, JobLogGUID)));
                                    string[] headers = csvReader.GetFieldHeaders();

                                    foreach (string header in headers)
                                    {
                                        var sanitizedHeader = regEx.Replace(header, string.Empty);
                                        sbc.ColumnMappings.Add(header, sanitizedHeader);
                                    }

                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start BulkCopy Filename: {0}", file.FilePath, JobLogGUID)));
                                    sbc.WriteToServer(csvReader);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End BulkCopy Filename: {0}", file.FilePath, JobLogGUID)));
                                }
                            }
                        }//end loading each source file

                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"{JobLogGUID} |ETLProvider | {nameof(LoadDCMMetadata)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }
    #endregion

    #region [TTD]
    [Obsolete]
    public static void LoadTTDMetadata(Guid fileGuid, Uri sourceFilePath, int sourceID, int integrationID, int countryID, IList<FileCollectionItem> filesToImport)
    {
        if (filesToImport == null || filesToImport.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException("LoadTTDMetadata failed, because no files were passed in");
        }

        string fileGuidString = fileGuid.ToString().Replace("-", string.Empty);

        string pattern = @"[\s\(\)\-\/\:\/]+";
        var regEx = new Regex(pattern);

        var fileGuidParam = new SqlParameter(@"@FileGUID", fileGuidString);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        var RAC = new RemoteAccessClient(sourceFilePath, GreenhouseAWSCredential);
        var s3Directory = RAC.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, fileGuidString))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        sbc.BulkCopyTimeout = 12000;
                        foreach (var file in filesToImport)
                        {
                            sbc.ColumnMappings.Clear();
                            string tableName = string.Format("ttd.#dim{0}_{1}", file.SourceFileName, fileGuidString);
                            sbc.DestinationTableName = tableName;
                            sbc.BatchSize = 100000;

                            var s3FilePath = RemoteUri.CombineUri(s3Directory.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Start Processing Filename: {0}, s3Path: {1}", file.FilePath, s3FilePath)));

                            var s3FileStream = RAC.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Retrieved S3 File Stream for file: {0}", file.FilePath)));

                            using (var zipStream = new GZipInputStream(s3FileStream))
                            {
                                using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(new StreamReader(zipStream), true, '\t'))
                                {
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Start Streaming TSV file: {0}", file.FilePath)));
                                    string[] headers = csvReader.GetFieldHeaders();

                                    foreach (string header in headers)
                                    {
                                        var sanitizedHeader = regEx.Replace(header, string.Empty).ToLower();
                                        sbc.ColumnMappings.Add(header, sanitizedHeader);
                                    }

                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Start BulkCopy Filename: {0}", file.FilePath)));
                                    sbc.WriteToServer(csvReader);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("End BulkCopy Filename: {0}", file.FilePath)));
                                }
                            }
                        }//end loading each source file

                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadTTDMetadata)} | Exception details : {exception}"));
                    trans.Rollback(fileGuidString);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("Start BulkCopy failed: {0}", exc.Message)));
                    trans.Rollback(fileGuidString);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }

    [Obsolete]
    public void LoadTTDPartnerOverview(Guid fileGuid, Uri sourceFilePath, int sourceID, int integrationID, int countryID, string fileToImport)
    {
        if (string.IsNullOrEmpty(fileToImport))
        {
            throw new NullOrEmptyFileCollectionException("LoadTTDMetadata failed, because no file was passed in");
        }

        string fileGuidString = fileGuid.ToString().Replace("-", string.Empty);

        string pattern = @"[\s\(\)\-\/\:\/]+";
        var regEx = new Regex(pattern);

        var fileGuidParam = new SqlParameter(@"@FileGUID", fileGuidString);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        var RAC = new RemoteAccessClient(sourceFilePath, GreenhouseAWSCredential);
        var s3Directory = RAC.WithDirectory();

        var s3FilePath = RemoteUri.CombineUri(s3Directory.Uri, fileToImport);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing Filename: s3Path: {1}", JobLogGUID, s3FilePath.AbsoluteUri)));

        var s3FileStream = RAC.WithFile(s3FilePath).Get();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Retrieved S3 File Stream for file: {0}", fileToImport)));

        var overviewData = DeserializeJSON<Data.DataSource.TTD.PartnerOverview>(s3FileStream);
        if (overviewData.Advertisers == null || overviewData.Advertisers.Count == 0)
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Partner overview data has no advertiser data. Exiting method LoadTTDPartnerOverview")));
            return;
        }

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, fileGuidString))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        sbc.BulkCopyTimeout = 12000;
                        sbc.ColumnMappings.Clear();
                        sbc.BatchSize = 100000;

                        string fmt = "ttd.#dim{0}_" + fileGuidString;
                        string tableName = string.Format(fmt, "partner");
                        sbc.DestinationTableName = tableName;

                        #region partner
                        var partner = new List<dynamic>(){
                            new {  overviewData.partnerid,
                                     overviewData.partnername
                                }
                        };

                        BulkLoadData(partner, sbc, "metadata", tableName);
                        #endregion

                        var advertiserCampaign = overviewData.Advertisers.Where(x => x.Campaigns != null & x.Campaigns.Count != 0);
                        var campaignObject = advertiserCampaign.SelectMany(x => x.Campaigns
                                     .Select(c => new
                                     {
                                         x.advertiserid,
                                         campaigns = c
                                     }));

                        var campaignAdGroupObject = campaignObject.Where(x => x.campaigns.AdGroups != null && x.campaigns.AdGroups.Count != 0).Select(x => new
                        {
                            x.campaigns.campaignid,
                            x.campaigns.AdGroups
                        });

                        var advertiserCreatives = overviewData.Advertisers.Where(x => x.Creatives != null && x.Creatives.Count != 0).SelectMany(a => a.Creatives.Select(c => new
                        {
                            a.advertiserid,
                            c.creativeid,
                            c.creativename,
                            c.description
                        }));

                        BulkLoadData<dynamic>(advertiserCreatives, sbc, "metadata", string.Format(fmt, "creative"));

                        //overviewData.Advertisers.Select(x=>new { })
                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadTTDPartnerOverview)} | Exception details : {exception}"));
                    trans.Rollback(fileGuidString);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("Start BulkCopy failed: {0}", exc.Message)));
                    trans.Rollback(fileGuidString);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }

    [Obsolete]
    public void LoadTTDDeltaAPIData(IFileItem queueFile, Uri sourceFilePath, int sourceID, int integrationID, int countryID)
    {
        if (queueFile == null)
        {
            throw new NullOrEmptyFileCollectionException(JobLogGUID + " - LoadTTDDeltaAPIData failed, because no file was passed in");
        }

        var RAC = new RemoteAccessClient(sourceFilePath, GreenhouseAWSCredential);

        // loading the files 

        var campaigns = new List<Data.DataSource.TTD.Delta.Campaign>();
        var advertisers = new List<Data.DataSource.TTD.Delta.Advertiser>();
        var adgroups = new List<Data.DataSource.TTD.Delta.AdGroup>();

        var s3Files = RAC.WithDirectory(sourceFilePath).GetFiles();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("Retrieved S3 File Stream for path: {0}", queueFile.FileName)));

        foreach (IFile file in s3Files)
        {
            var s3FileStream = file.Get();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing Filename: s3Path: {1}", JobLogGUID, file.Uri)));

            if (file.Name.StartsWith("campaigns_advertiser_"))
            {
                var campaignsFromFile = DeserializeJSON<Data.DataSource.TTD.Delta.RootCampaign>(s3FileStream);
                campaigns.AddRange(campaignsFromFile.Campaigns);
                continue;
            }
            ;
            if (file.Name.StartsWith("adgroup_delta_advertiser_"))
            {
                var adgroupsFromFile = DeserializeJSON<Data.DataSource.TTD.Delta.RootAdGroup>(s3FileStream);
                adgroups.AddRange(adgroupsFromFile.AdGroups);
                continue;
            }

            if (file.Name.StartsWith("advertisers_"))
            {
                var advertisersFromFile = DeserializeJSON<Data.DataSource.TTD.Delta.RootAdvertiser>(s3FileStream);
                advertisers.AddRange(advertisersFromFile.advertisers);
                continue;
            }
        }

        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing Filename: s3Path: {1}", JobLogGUID, sourceFilePath.AbsoluteUri)));

        string fileGuidString = queueFile.FileGUID.ToString().Replace("-", string.Empty);

        var fileGuidParam = new SqlParameter(@"@FileGUID", fileGuidString);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, fileGuidString))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Creating Temp Tables", JobLogGUID)));

                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        sbc.BulkCopyTimeout = 12000;

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Creating Temp Tables", JobLogGUID)));

                        #region Advertiser
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing Advertisers", JobLogGUID)));

                        var advertiserObject = advertisers.Select(a => new
                        {
                            advertiserid = a.AdvertiserId,
                            advertisername = a.AdvertiserName,
                            attributionclicklookbackwindowinseconds = a.AttributionClickLookbackWindowInSeconds,
                            attributionimpressionlookbackwindowinseconds = a.AttributionImpressionLookbackWindowInSeconds,
                            availability = a.Availability,
                            clickdedupwindowinseconds = a.ClickDedupWindowInSeconds,
                            conversiondedupwindowinseconds = a.ConversionDedupWindowInSeconds,
                            currencycode = a.CurrencyCode,
                            defaultrightmediaoffertypeid = a.DefaultRightMediaOfferTypeId,
                            description = a.Description,
                            domainaddress = a.DomainAddress,
                            industrycategoryid = a.IndustryCategoryId,
                            keywords = string.Join(",", a.Keywords),
                            logourl = a.LogoURL,
                            partnerid = a.PartnerId
                        });

                        string dimAdvertiserTempTableName = "ttd.#dimadvertiser_" + fileGuidString;
                        BulkLoadData<dynamic>(advertiserObject, sbc, "metadata", dimAdvertiserTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing Advertisers", JobLogGUID)));

                        #endregion

                        #region Campaign
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing Campaigns", JobLogGUID)));

                        var campaignObject = campaigns.Select(c => new
                        {
                            campaignid = c.CampaignId,
                            campaignname = c.CampaignName,
                            description = c.Description,
                            startdateutc = c.StartDate,
                            enddateutc = c.EndDate,
                            availability = c.Availability,
                            advertiserid = c.AdvertiserId,
                            budgetamount = c.Budget.Amount,
                            budgetcurrencycode = c.Budget.CurrencyCode,
                            budgetinimpressions = c.BudgetInImpressions,
                            autoallocatorenabled = c.AutoAllocatorEnabled,
                            pacingmode = c.PacingMode,
                            partnercostpercentagefee = c.PartnerCostPercentageFee,
                            partnercpmfeeamount = c.PartnerCPMFee?.Amount ?? "NULL",
                            partnercpmfeecurrencycode = c.PartnerCPMFee?.CurrencyCode ?? "NULL",
                            partnercpcfeeamount = c.PartnerCPCFee.Amount.ToString(),
                            partnercpcfeecurrencycode = c.PartnerCPCFee.CurrencyCode,
                            purchaseordernumber = c.PurchaseOrderNumber?.ToString() ?? "NULL",
                            timezone = c.TimeZone
                        });

                        string dimCampaignTempTableName = "ttd.#dimcampaign_" + fileGuidString;
                        BulkLoadData<dynamic>(campaignObject, sbc, "metadata", dimCampaignTempTableName);

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing Campaigns", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing CampaignConversion", JobLogGUID)));
                        var campaignConversion = campaigns.SelectMany(x => x.CampaignConversionReportingColumns
                                                            .Select(c => new
                                                            {
                                                                campaignid = x.CampaignId,
                                                                campaignconversion = c
                                                            }));

                        var campaignConversionObject = campaignConversion.Select(c =>
                            new
                            {
                                trackingtagid = c.campaignconversion.TrackingTagId,
                                trackingtagname = c.campaignconversion.TrackingTagName,
                                reportingcolumnid = c.campaignconversion.ReportingColumnId,
                                crossdeviceattributionomodelid = c.campaignconversion.CrossDeviceAttributionModelId,
                                c.campaignid
                            }
                            );

                        string dimCampaignConversionTempTableName = "ttd.#dimcampaignconversion_" + fileGuidString;
                        BulkLoadData<dynamic>(campaignConversionObject, sbc, "metadata", dimCampaignConversionTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing CampaignConversion", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing CampaignFlight", JobLogGUID)));

                        var campaignflightObject = campaigns.SelectMany(x => x.CampaignFlights).Select(c => new
                        {
                            campaignflightid = c.CampaignFlightId,
                            campaignid = c.CampaignId,
                            startdateinclusiveutc = c.StartDateInclusiveUTC,
                            enddateexclusiveutc = c.EndDateExclusiveUTC,
                            budgetinadvertisercurrency = c.BudgetInAdvertiserCurrency,
                            budgetinimpressions = c.BudgetInImpressions,
                            dailytargetinadvertisercurrency = c.DailyTargetInAdvertiserCurrency,
                            dailytargetinimpressions = c.DailyTargetInImpressions
                        });

                        string dimCampaignFlightTempTableName = "ttd.#dimcampaignflight_" + fileGuidString;
                        BulkLoadData<dynamic>(campaignflightObject, sbc, "metadata", dimCampaignFlightTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing CampaignFlight", JobLogGUID)));

                        #endregion

                        #region AdGroup
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroups", JobLogGUID)));

                        var adGroupObject = adgroups.Select(a =>
                            new
                            {
                                adgroupid = a.AdGroupId,
                                adgroupname = a.AdGroupName,
                                description = a.Description,
                                isenabled = a.IsEnabled,
                                availability = a.Availability,
                                campaignid = a.CampaignId,
                                industrycategoryid = a.IndustryCategoryId,
                                createdatutc = a.CreatedAtUTC,
                                lastupdatedatutc = a.LastUpdatedAtUTC,
                                maxbidcpmamount = a.RTBAttributes?.MaxBidCPM?.Amount,
                                maxbidcpmcurrencycode = a.RTBAttributes?.MaxBidCPM?.CurrencyCode
                            });

                        string dimAdGroupTempTableName = "ttd.#dimadgroup_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupObject, sbc, "metadata", dimAdGroupTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroups", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing BudgetSettings", JobLogGUID)));

                        var adGroupBudgetSettings = adgroups.Where(a => a.RTBAttributes?.BudgetSettings != null).Select(a =>
                            new
                            {
                                adgroupid = a.AdGroupId,
                                budgetamount = a.RTBAttributes?.BudgetSettings?.Budget?.Amount,
                                budgetcurrencycode = a.RTBAttributes?.BudgetSettings?.Budget?.CurrencyCode,
                                budgetimpressions = a.RTBAttributes?.BudgetSettings?.BudgetInImpressions,
                                dailybudgetamount = a.RTBAttributes?.BudgetSettings?.DailyBudget?.Amount,
                                dailybudgetcurrencycode = a.RTBAttributes?.BudgetSettings?.DailyBudget?.CurrencyCode,
                                pacingenabled = a.RTBAttributes?.BudgetSettings?.PacingEnabled ?? null,
                                pacingmode = a.RTBAttributes?.BudgetSettings?.PacingMode,
                                autoallocatorpriority = a.RTBAttributes?.BudgetSettings?.AutoAllocatorPriority
                            }

                            );

                        string dimAdGroupBudgetSettingsTempTableName = "ttd.#dimadgroupbudgetsetting_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupBudgetSettings, sbc, "metadata", dimAdGroupBudgetSettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing BudgetSettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupFlight", JobLogGUID)));

                        var adGroupFlight = adgroups.Where(a => a.RTBAttributes?.BudgetSettings?.AdGroupFlights != null)
                            .SelectMany(a => a.RTBAttributes.BudgetSettings.AdGroupFlights).Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    campaignflightid = a.CampaignFlightId,
                                    budgetinadvertisercurrency = a.BudgetInAdvertiserCurrency,
                                    budgetinimpressions = a.BudgetInImpressions,
                                    dailytargetinadvertisercurrency = a.DailyTargetInAdvertiserCurrency,
                                    dailytargetinimpressions = a.DailyTargetInImpressions
                                }
                        );

                        string dimAdGroupFlightTempTableName = "ttd.#dimadgroupflight_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupFlight, sbc, "metadata", dimAdGroupFlightTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupFlight", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupFequencySettings", JobLogGUID)));

                        var adGroupFequencySettings = adgroups.Where(a => a.RTBAttributes?.FrequencySettings != null)
                            .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    frequencycap = a.RTBAttributes.FrequencySettings.FrequencyCap,
                                    frequencyperiodinminutes = a.RTBAttributes.FrequencySettings.FrequencyPeriodInMinutes,
                                    frequencypricingslopecpmamount = a.RTBAttributes.FrequencySettings.FrequencyPricingSlopeCPM?.Amount,
                                    frequencypricingslopecpmcurrencycode = a.RTBAttributes.FrequencySettings.FrequencyPricingSlopeCPM?.CurrencyCode
                                }
                            );

                        string dimAdGroupFreqencySettingsTempTableName = "ttd.#dimadgroupfrequencysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupFequencySettings, sbc, "metadata", dimAdGroupFreqencySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupFequencySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupIntegralContextualCategorySettings", JobLogGUID)));

                        var adGroupIntegralContextualCategorySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.IntegralSettings?.IntegralContextualCategorySettings != null)
                            .Select(a => new
                            {
                                a.AdGroupId,
                                Settings = a.RTBAttributes.SiteQualitySettings.IntegralSettings.IntegralContextualCategorySettings
                            })
                            .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforcontextualcategoriespercentofmediacostrate = a.Settings.FeeForContextualCategories?.PercentOfMediaCostRate,
                                    feeforcontextualcategoriespercentofdatacostrate = a.Settings.FeeForContextualCategories?.PercentOfDataCostRate,
                                    feeforcontextualcategoriescpmrateamount = a.Settings.FeeForContextualCategories?.CPMRate?.Amount,
                                    feeforcontextualcategoriescpmratecurrencycode = a.Settings.FeeForContextualCategories?.CPMRate?.CurrencyCode,
                                    feeforcontextualcategoriescpmrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforcontextualcategoriescpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforcontextualcategoriescpcrateamount = a.Settings.FeeForContextualCategories?.CPCRate?.Amount,
                                    feeforcontextualcategoriescpcratecurrencycode = a.Settings.FeeForContextualCategories?.CPCRate?.CurrencyCode,
                                    feeforcontextualcategoriescpcrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforcontextualcategoriescpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    contextualcategoriesenabled = a.Settings.ContextualCategoriesEnabled
                                }
                            );

                        string dimAdGroupIntegralContextualCategorySettingsTempTableName = "ttd.#dimadgroupintegralcontextualcategorysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupIntegralContextualCategorySettings, sbc, "metadata", dimAdGroupIntegralContextualCategorySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupIntegralContextualCategorySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupIntegralBrandSafetySettings", JobLogGUID)));

                        var adGroupIntegralBrandSafetySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.IntegralSettings?.IntegralBrandSafetySettings != null)
                            .Select(a => new
                            {
                                a.AdGroupId,
                                Settings = a.RTBAttributes.SiteQualitySettings.IntegralSettings.IntegralBrandSafetySettings
                            })
                            .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforbrandsafetypercentofmediacostrate = a.Settings.FeeForBrandSafety?.PercentOfMediaCostRate,
                                    feeforbrandsafetypercentofdatacostrate = a.Settings.FeeForBrandSafety?.PercentOfDataCostRate,
                                    feeforbrandsafetycpmrateamount = a.Settings.FeeForBrandSafety?.CPMRate?.Amount,
                                    feeforbrandsafetycpmratecurrencycode = a.Settings.FeeForBrandSafety?.CPMRate?.CurrencyCode,
                                    feeforbrandsafetycpmrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforbrandsafetycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforbrandsafetycpcrateamount = a.Settings.FeeForBrandSafety?.CPMRate?.Amount,
                                    feeforbrandsafetycpcratecurrencycode = a.Settings.FeeForBrandSafety?.CPMRate?.CurrencyCode,
                                    feeforbrandsafetycpcrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforbrandsafetycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideobrandsafetypercentofmediacostrate = a.Settings.FeeForVideoBrandSafety?.PercentOfMediaCostRate,
                                    feeforvideobrandsafetypercentofdatacostrate = a.Settings.FeeForVideoBrandSafety?.PercentOfDataCostRate,
                                    feeforvideobrandsafetycpmrateamount = a.Settings.FeeForVideoBrandSafety?.CPMRate?.Amount,
                                    feeforvideobrandsafetycpmratecurrencycode = a.Settings.FeeForVideoBrandSafety?.CPMRate?.CurrencyCode,
                                    feeforvideobrandsafetycpmrateinadvertisercurrencyamount = a.Settings.FeeForVideoBrandSafety?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforvideobrandsafetycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoBrandSafety?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideobrandsafetycpcrateamount = a.Settings.FeeForVideoBrandSafety?.CPCRate?.Amount,
                                    feeforvideobrandsafetycpcratecurrencycode = a.Settings.FeeForVideoBrandSafety?.CPCRate?.CurrencyCode,
                                    feeforvideobrandsafetycpcrateinadvertisercurrencyamount = a.Settings.FeeForVideoBrandSafety?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforvideobrandsafetycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoBrandSafety?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    brandsafetyenabled = a.Settings.BrandSafetyEnabled,
                                    adultcontent = a.Settings.AdultContent,
                                    alcoholcontent = a.Settings.AlcoholContent,
                                    drugcontent = a.Settings.DrugContent,
                                    hatespeechcontent = a.Settings.HateSpeechContent,
                                    gamblingcontent = a.Settings.GamblingContent,
                                    illegaldownloadcontent = a.Settings.IllegalDownloadContent,
                                    offensivelanguagecontent = a.Settings.OffensiveLanguageContent,
                                    violentcontent = a.Settings.ViolentContent
                                }
                            );

                        string dimAdGrouIntegralBrandSafetySettingsTempTableName = "ttd.#dimadgroupintegralbrandsafetysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupIntegralBrandSafetySettings, sbc, "metadata", dimAdGrouIntegralBrandSafetySettingsTempTableName);

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupIntegralBrandSafetySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupIntegralViewabilitySettings", JobLogGUID)));

                        var adGroupIntegralViewabilitySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.IntegralSettings?.IntegralViewabilitySettings != null)
                            .Select(a => new
                            {
                                a.AdGroupId,
                                Settings = a.RTBAttributes.SiteQualitySettings.IntegralSettings.IntegralViewabilitySettings
                            })
                            .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforviewabilityratingpercentofmediacostrate = a.Settings.FeeForViewabilityRating?.PercentOfMediaCostRate,
                                    feeforviewabilityratingpercentofdatacostrate = a.Settings.FeeForViewabilityRating?.PercentOfDataCostRate,
                                    feeforviewabilityratingcpmrateamount = a.Settings.FeeForViewabilityRating?.CPMRate?.Amount,
                                    feeforviewabilityratingcpmratecurrencycode = a.Settings.FeeForViewabilityRating?.CPMRate?.CurrencyCode,
                                    feeforviewabilityratingcpmrateinadvertisercurrencyamount = a.Settings.FeeForViewabilityRating?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforviewabilityratingcpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForViewabilityRating?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforviewabilityratingcpcrateamount = a.Settings.FeeForViewabilityRating?.CPCRate?.Amount,
                                    feeforviewabilityratingcpcratecurrencycode = a.Settings.FeeForViewabilityRating?.CPCRate?.CurrencyCode,
                                    feeforviewabilityratingcpcrateinadvertisercurrencyamount = a.Settings.FeeForViewabilityRating?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforviewabilityratingcpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForViewabilityRating?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    viewabilityrating = a.Settings.ViewabilityRating,
                                    feeforvideoviewabilityratingpercentofmediacostrate = a.Settings.FeeForVideoViewabilityRating?.PercentOfMediaCostRate,
                                    feeforvideoviewabilityratingpercentofdatacostrate = a.Settings.FeeForVideoViewabilityRating?.PercentOfDataCostRate,
                                    feeforvideoviewabilityratingcpmrateamount = a.Settings.FeeForVideoViewabilityRating?.CPMRate?.Amount,
                                    feeforvideoviewabilityratingcpmratecurrencycode = a.Settings.FeeForVideoViewabilityRating?.CPMRate?.CurrencyCode,
                                    feeforvideoviewabilityratingcpmrateinadvertisercurrencyamount = a.Settings.FeeForVideoViewabilityRating?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforvideoviewabilityratingcpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoViewabilityRating?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideoviewabilityratingcpcrateamount = a.Settings.FeeForVideoViewabilityRating?.CPCRate?.Amount,
                                    feeforvideoviewabilityratingcpcratecurrencycode = a.Settings.FeeForVideoViewabilityRating?.CPCRate?.CurrencyCode,
                                    feeforvideoviewabilityratingcpcrateinadvertisercurrencyamount = a.Settings.FeeForVideoViewabilityRating?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforvideoviewabilityratingcpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoViewabilityRating?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    videoviewabilityrating = a.Settings.ViewabilityRating,
                                }
                            );

                        string dimAdGroupIntegralViewabilitySettingsTempTableName = "ttd.#dimadgroupintegralviewabilitysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupIntegralViewabilitySettings, sbc, "metadata", dimAdGroupIntegralViewabilitySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupIntegralViewabilitySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupIntegralSuspiciousActivitySettings", JobLogGUID)));

                        var adGroupIntegralSuspiciousActivitySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.IntegralSettings?.IntegralSuspiciousActivitySettings != null)
                            .Select(a => new
                            {
                                a.AdGroupId,
                                Settings = a.RTBAttributes.SiteQualitySettings.IntegralSettings.IntegralSuspiciousActivitySettings
                            })
                            .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforsuspiciousactivityratingpercentofmediacostrate = a.Settings.FeeForSuspiciousActivityRating?.PercentOfMediaCostRate,
                                    feeforsuspiciousactivityratingpercentofdatacostrate = a.Settings.FeeForSuspiciousActivityRating?.PercentOfDataCostRate,
                                    feeforsuspiciousactivityratingcpmrateamount = a.Settings.FeeForSuspiciousActivityRating?.CPMRate?.Amount,
                                    feeforsuspiciousactivityratingcpmratecurrencycode = a.Settings.FeeForSuspiciousActivityRating?.CPMRate?.CurrencyCode,
                                    feeforsuspiciousactivityratingcpmrateinadvertisercurrencyamount = a.Settings.FeeForSuspiciousActivityRating?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforsuspiciousactivityratingcpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForSuspiciousActivityRating?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforsuspiciousactivityratingcpcrateamount = a.Settings.FeeForSuspiciousActivityRating?.CPCRate?.Amount,
                                    feeforsuspiciousactivityratingcpcratecurrencycode = a.Settings.FeeForSuspiciousActivityRating?.CPCRate?.CurrencyCode,
                                    feeforsuspiciousactivityratingcpcrateinadvertisercurrencyamount = a.Settings.FeeForSuspiciousActivityRating?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforsuspiciousactivityratingcpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForSuspiciousActivityRating?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideosuspiciousactivityratingpercentofmediacostrate = a.Settings.FeeForVideoSuspiciousActivityRating?.PercentOfMediaCostRate,
                                    feeforvideosuspiciousactivityratingpercentofdatacostrate = a.Settings.FeeForVideoSuspiciousActivityRating?.PercentOfDataCostRate,
                                    feeforvideosuspiciousactivityratingcpmrateamount = a.Settings.FeeForVideoSuspiciousActivityRating?.CPMRate?.Amount,
                                    feeforvideosuspiciousactivityratingcpmratecurrencycode = a.Settings.FeeForVideoSuspiciousActivityRating?.CPMRate?.CurrencyCode,
                                    feeforvideosuspiciousactivityratingcpmrateinadvertisercurrencyamount = a.Settings.FeeForVideoSuspiciousActivityRating?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforvideosuspiciousactivityratingcpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoSuspiciousActivityRating?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideosuspiciousactivityratingcpcrateamount = a.Settings.FeeForVideoSuspiciousActivityRating?.CPCRate?.Amount,
                                    feeforvideosuspiciousactivityratingcpcratecurrencycode = a.Settings.FeeForVideoSuspiciousActivityRating?.CPCRate?.CurrencyCode,
                                    feeforvideosuspiciousactivityratingcpcrateinadvertisercurrencyamount = a.Settings.FeeForVideoSuspiciousActivityRating?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforvideosuspiciousactivityratingcpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoSuspiciousActivityRating?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    suspiciousactivityrating = a.Settings.SuspiciousActivityRating
                                }
                            );

                        string dimAdGroupIntegralSuspiciousActivitySettingsTempTableName = "ttd.#dimadgroupintegralsuspiciousactivitysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupIntegralSuspiciousActivitySettings, sbc, "metadata", dimAdGroupIntegralSuspiciousActivitySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupIntegralSuspiciousActivitySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupGrapeshotContextualCategorySettings", JobLogGUID)));

                        var adGroupGrapeshotContextualCategorySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.GrapeshotSettings?.GrapeshotContextualCategorySettings != null)
                       .Select(a => new
                       {
                           a.AdGroupId,
                           Settings = a.RTBAttributes.SiteQualitySettings.GrapeshotSettings?.GrapeshotContextualCategorySettings
                       })
                       .Select(a =>
                           new
                           {
                               adgroupid = a.AdGroupId,
                               feeforcontextualcategoriespercentofmediacostrate = a.Settings.FeeForContextualCategories?.PercentOfMediaCostRate,
                               feeforcontextualcategoriespercentofdatacostrate = a.Settings.FeeForContextualCategories?.PercentOfDataCostRate,
                               feeforcontextualcategoriescpmrateamount = a.Settings.FeeForContextualCategories?.CPMRate?.Amount,
                               feeforcontextualcategoriescpmratecurrencycode = a.Settings.FeeForContextualCategories?.CPMRate?.CurrencyCode,
                               feeforcontextualcategoriescpmrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.Amount,
                               feeforcontextualcategoriescpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                               feeforcontextualcategoriescpcrateamount = a.Settings.FeeForContextualCategories?.CPCRate?.Amount,
                               feeforcontextualcategoriescpcratecurrencycode = a.Settings.FeeForContextualCategories?.CPCRate?.CurrencyCode,
                               feeforcontextualcategoriescpcrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPCRateInAdvertiserCurrency?.Amount,
                               feeforcontextualcategoriescpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                               contextualcategoriesenabled = a.Settings.ContextualCategoriesEnabled
                           }
                       );

                        string dimAdGroupGrapeshotContextualCategorySettingsTempTableName = "ttd.#dimadgroupgrapeshotcontextualcategorysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupGrapeshotContextualCategorySettings, sbc, "metadata", dimAdGroupGrapeshotContextualCategorySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupGrapeshotContextualCategorySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupGrapeshotBrandSafetySettings", JobLogGUID)));

                        var adGroupGrapeshotBrandSafetySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.GrapeshotSettings?.GrapeshotBrandSafetySettings != null)
                        .Select(a => new
                        {
                            a.AdGroupId,
                            Settings = a.RTBAttributes.SiteQualitySettings.GrapeshotSettings?.GrapeshotBrandSafetySettings
                        })
                        .Select(a =>
                           new
                           {
                               adgroupid = a.AdGroupId,
                               feeforbrandsafetypercentofmediacostrate = a.Settings.FeeForBrandSafety?.PercentOfMediaCostRate,
                               feeforbrandsafetypercentofdatacostrate = a.Settings.FeeForBrandSafety?.PercentOfDataCostRate,
                               feeforbrandsafetycpmrateamount = a.Settings.FeeForBrandSafety?.CPMRate?.Amount,
                               feeforbrandsafetycpmratecurrencycode = a.Settings.FeeForBrandSafety?.CPMRate?.Amount,
                               feeforbrandsafetycpmrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.Amount,
                               feeforbrandsafetycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                               feeforbrandsafetycpcrateamount = a.Settings.FeeForBrandSafety?.CPCRate?.Amount,
                               feeforbrandsafetycpcratecurrencycode = a.Settings.FeeForBrandSafety?.CPCRate?.CurrencyCode,
                               feeforbrandsafetycpcrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.Amount,
                               feeforbrandsafetycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                               brandsafetyenabled = a.Settings.BrandSafetyEnabled,
                               blockprofanityandhatespeech = a.Settings.BlockProfanityAndHateSpeech,
                               blockillegaldownload = a.Settings.BlockIllegalDownload,
                               blockmaturecontent = a.Settings.BlockMatureContent,
                               blockdrugs = a.Settings.BlockDrugs,
                               blocktobacco = a.Settings.BlockTobacco,
                               blockfirearms = a.Settings.BlockFirearms,
                               blockcrime = a.Settings.BlockCrime,
                               blockdeathmurder = a.Settings.BlockDeathMurder,
                               blockracist = a.Settings.BlockRacist,
                               blockmilitarycontent = a.Settings.BlockMilitaryContent,
                               blockterrorism = a.Settings.BlockTerrorism,
                           }
                        );

                        string dimadgroupgrapeshotbrandsafetysettingsTempTableName = "ttd.#dimadgroupgrapeshotbrandsafetysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupGrapeshotBrandSafetySettings, sbc, "metadata", dimadgroupgrapeshotbrandsafetysettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupGrapeshotBrandSafetySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupDoubleVerifyContextualCategorySettings", JobLogGUID)));

                        var adGroupDoubleVerifyContextualCategorySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.DoubleVerifySettings?.DoubleVerifyContextualCategorySettings != null)
                             .Select(a => new
                             {
                                 a.AdGroupId,
                                 Settings = a.RTBAttributes.SiteQualitySettings.DoubleVerifySettings?.DoubleVerifyContextualCategorySettings
                             })
                             .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforcontextualcategoriespercentofmediacostrate = a.Settings.FeeForContextualCategories?.PercentOfMediaCostRate,
                                    feeforcontextualcategoriespercentofdatacostrate = a.Settings.FeeForContextualCategories?.PercentOfDataCostRate,
                                    feeforcontextualcategoriescpmrateamount = a.Settings.FeeForContextualCategories?.CPMRate?.Amount,
                                    feeforcontextualcategoriescpmratecurrencycode = a.Settings.FeeForContextualCategories?.CPMRate?.CurrencyCode,
                                    feeforcontextualcategoriescpmrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforcontextualcategoriescpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforcontextualcategoriescpcrateamount = a.Settings.FeeForContextualCategories?.CPCRate?.Amount,
                                    feeforcontextualcategoriescpcratecurrencycode = a.Settings.FeeForContextualCategories?.CPCRate?.CurrencyCode,
                                    feeforcontextualcategoriescpcrateinadvertisercurrencyamount = a.Settings.FeeForContextualCategories?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforcontextualcategoriescpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForContextualCategories?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    contextualcategoriesenabled = a.Settings.ContextualCategoriesEnabled
                                }
                             );

                        string dimadgroupDoubleVerifyContextualCategorySettingsTempTableName = "ttd.#dimadgroupdoubleverifycontextualcategorysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupDoubleVerifyContextualCategorySettings, sbc, "metadata", dimadgroupDoubleVerifyContextualCategorySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupDoubleVerifyContextualCategorySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupDoubleVerifyViewabilitySettings", JobLogGUID)));

                        var adGroupDoubleVerifyViewabilitySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.DoubleVerifySettings?.DoubleVerifyViewabilitySettings != null)
                             .Select(a => new
                             {
                                 a.AdGroupId,
                                 Settings = a.RTBAttributes.SiteQualitySettings.DoubleVerifySettings?.DoubleVerifyViewabilitySettings
                             })
                             .Select(a =>
                                new
                                {
                                    adgroupid = a.AdGroupId,
                                    feeforviewabilitypercentofmediacostrate = a.Settings.FeeForViewability?.PercentOfMediaCostRate,
                                    feeforviewabilitypercentofdatacostrate = a.Settings.FeeForViewability?.PercentOfDataCostRate,
                                    feeforviewabilitycpmrateamount = a.Settings.FeeForViewability?.CPMRate?.Amount,
                                    feeforviewabilitycpmratecurrencycode = a.Settings.FeeForViewability?.CPMRate?.CurrencyCode,
                                    feeforviewabilitycpmrateinadvertisercurrencyamount = a.Settings.FeeForViewability?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforviewabilitycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForViewability?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforviewabilitycpcrateamount = a.Settings.FeeForViewability?.CPCRate?.Amount,
                                    feeforviewabilitycpcratecurrencycode = a.Settings.FeeForViewability?.CPCRate?.CurrencyCode,
                                    feeforviewabilitycpcrateinadvertisercurrencyamount = a.Settings.FeeForViewability?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforviewabilitycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForViewability?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideoviewabilitypercentofmediacostrate = a.Settings.FeeForVideoViewability?.PercentOfMediaCostRate,
                                    feeforvideoviewabilitypercentofdatacostrate = a.Settings.FeeForVideoViewability?.PercentOfDataCostRate,
                                    feeforvideoviewabilitycpmrateamount = a.Settings.FeeForVideoViewability?.CPMRate?.Amount,
                                    feeforvideoviewabilitycpmratecurrencycode = a.Settings.FeeForVideoViewability?.CPMRate?.CurrencyCode,
                                    feeforvideoviewabilitycpmrateinadvertisercurrencyamount = a.Settings.FeeForVideoViewability?.CPMRateInAdvertiserCurrency?.Amount,
                                    feeforvideoviewabilitycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoViewability?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                    feeforvideoviewabilitycpcrateamount = a.Settings.FeeForVideoViewability?.CPCRate?.Amount,
                                    feeforvideoviewabilitycpcratecurrencycode = a.Settings.FeeForVideoViewability?.CPCRate?.CurrencyCode,
                                    feeforvideoviewabilitycpcrateinadvertisercurrencyamount = a.Settings.FeeForVideoViewability?.CPCRateInAdvertiserCurrency?.Amount,
                                    feeforvideoviewabilitycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForVideoViewability?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                    viewabilityrating = a.Settings.ViewabilityRating,
                                    viewabilitytype = a.Settings.ViewabilityType
                                }
                             );

                        string dimadgroupDoubleVerifyViewabilitySettingsTempTableName = "ttd.#dimadgroupdoubleverifyviewabilitysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupDoubleVerifyViewabilitySettings, sbc, "metadata", dimadgroupDoubleVerifyViewabilitySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupDoubleVerifyViewabilitySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupDoubleVerifyBrandSafetySettings", JobLogGUID)));

                        var adGroupDoubleVerifyBrandSafetySettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.DoubleVerifySettings?.DoubleVerifyBrandSafetySettings != null)
                          .Select(a => new
                          {
                              a.AdGroupId,
                              Settings = a.RTBAttributes.SiteQualitySettings.DoubleVerifySettings?.DoubleVerifyBrandSafetySettings
                          })
                          .Select(a =>
                             new
                             {
                                 adgroupid = a.AdGroupId,
                                 feeforbrandsafetypercentofmediacostrate = a.Settings.FeeForBrandSafety?.PercentOfMediaCostRate,
                                 feeforbrandsafetypercentofdatacostrate = a.Settings.FeeForBrandSafety?.PercentOfDataCostRate,
                                 feeforbrandsafetycpmrateamount = a.Settings.FeeForBrandSafety?.CPMRate?.Amount,
                                 feeforbrandsafetycpmratecurrencycode = a.Settings.FeeForBrandSafety?.CPMRate?.CurrencyCode,
                                 feeforbrandsafetycpmrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.Amount,
                                 feeforbrandsafetycpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                 feeforbrandsafetycpcrateamount = a.Settings.FeeForBrandSafety?.CPCRate?.Amount,
                                 feeforbrandsafetycpcratecurrencycode = a.Settings.FeeForBrandSafety?.CPCRate?.CurrencyCode,
                                 feeforbrandsafetycpcrateinadvertisercurrencyamount = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.Amount,
                                 feeforbrandsafetycpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForBrandSafety?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                 brandsafetyenabled = a.Settings.BrandSafetyEnabled,
                                 adultcontentpornographymaturetopicsandnudity = a.Settings.AdultContentPornographyMatureTopicsAndNudity,
                                 adultcontentswimsuit = a.Settings.AdultContentSwimsuit,
                                 controversialsubjectsalternativelifestyles = a.Settings.ControversialSubjectsAlternativeLifestyles,
                                 controversialsubjectscelebritygossip = a.Settings.ControversialSubjectsCelebrityGossip,
                                 controversialsubjectsgambling = a.Settings.ControversialSubjectsGambling,
                                 controversialsubjectsoccult = a.Settings.ControversialSubjectsOccult,
                                 controversialsubjectssexeducation = a.Settings.ControversialSubjectsSexEducation,
                                 copyrightinfringement = a.Settings.CopyrightInfringement,
                                 disasteraviation = a.Settings.DisasterAviation,
                                 disastermanmade = a.Settings.DisasterManMade,
                                 disasternatural = a.Settings.DisasterNatural,
                                 disasterterroristevents = a.Settings.DisasterTerroristEvents,
                                 disastervehicle = a.Settings.DisasterVehicle,
                                 drugsalcoholcontrolledsubstancesalcohol = a.Settings.DrugsAlcoholControlledSubstancesAlcohol,
                                 drugsalcoholcontrolledsubstancessmoking = a.Settings.DrugsAlcoholControlledSubstancesSmoking,
                                 drugsalcoholcontrolledsubstancessubstanceabuse = a.Settings.DrugsAlcoholControlledSubstancesSubstanceAbuse,
                                 extremegraphicexplicitviolenceweapons = a.Settings.ExtremeGraphicExplicitViolenceWeapons,
                                 adimpressionfraud = a.Settings.AdImpressionFraud,
                                 hateprofanity = a.Settings.HateProfanity,
                                 illegalactivitiescriminalskills = a.Settings.IllegalActivitiesCriminalSkills,
                                 nuisancespywaremalwarewarez = a.Settings.NuisanceSpywareMalwareWarez,
                                 negativenewsfinancial = a.Settings.NegativeNewsFinancial,
                                 nonstandardcontentnonenglish = a.Settings.NonStandardContentNonEnglish,
                                 nonstandardcontentparkingpage = a.Settings.NonStandardContentParkingPage,
                                 unmoderatedugcforumsimagesandvideo = a.Settings.UnmoderatedUgcForumsImagesAndVideo,
                                 adserver = a.Settings.AdServer
                             }
                          );

                        string dimadgroupDoubleVerifyBrandSafetySettingsTempTableName = "ttd.#dimadgroupdoubleverifybrandsafetysettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupDoubleVerifyBrandSafetySettings, sbc, "metadata", dimadgroupDoubleVerifyBrandSafetySettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupDoubleVerifyBrandSafetySettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupDoubleVerifyBotAvoidanceSettings", JobLogGUID)));

                        var adGroupDoubleVerifyBotAvoidanceSettings = adgroups.Where(a => a.RTBAttributes?.SiteQualitySettings?.DoubleVerifySettings?.DoubleVerifyBotAvoidanceSettings != null)
                          .Select(a => new
                          {
                              a.AdGroupId,
                              Settings = a.RTBAttributes.SiteQualitySettings.DoubleVerifySettings?.DoubleVerifyBotAvoidanceSettings
                          })
                          .Select(a =>
                             new
                             {
                                 adgroupid = a.AdGroupId,
                                 feeforbotavoidancepercentofmediacostrate = a.Settings.FeeForBotAvoidance?.PercentOfMediaCostRate,
                                 feeforbotavoidancepercentofdatacostrate = a.Settings.FeeForBotAvoidance?.PercentOfDataCostRate,
                                 feeforbotavoidancecpmrateamount = a.Settings.FeeForBotAvoidance?.CPMRate?.Amount,
                                 feeforbotavoidancecpmratecurrencycode = a.Settings.FeeForBotAvoidance?.CPMRate?.CurrencyCode,
                                 feeforbotavoidancecpmrateinadvertisercurrencyamount = a.Settings.FeeForBotAvoidance?.CPMRateInAdvertiserCurrency?.Amount,
                                 feeforbotavoidancecpmrateinadvertisercurrencycurrencycode = a.Settings.FeeForBotAvoidance?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                 feeforbotavoidancecpcrateamount = a.Settings.FeeForBotAvoidance?.CPCRate?.Amount,
                                 feeforbotavoidancecpcratecurrencycode = a.Settings.FeeForBotAvoidance?.CPCRate?.CurrencyCode,
                                 feeforbotavoidancecpcrateinadvertisercurrencyamount = a.Settings.FeeForBotAvoidance?.CPCRateInAdvertiserCurrency?.Amount,
                                 feeforbotavoidancecpcrateinadvertisercurrencycurrencycode = a.Settings.FeeForBotAvoidance?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                 botavoidanceenabled = a.Settings.BotAvoidanceEnabled
                             }
                          );

                        string dimadgroupDoubleVerifyBotAvoidanceSettingsTempTableName = "ttd.#dimadgroupdoubleverifybotavoidancesettings_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupDoubleVerifyBotAvoidanceSettings, sbc, "metadata", dimadgroupDoubleVerifyBotAvoidanceSettingsTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupDoubleVerifyBotAvoidanceSettings", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Processing AdGroupQualityAllianceViewabilityTargeting", JobLogGUID)));
                        var adGroupQualityAllianceViewabilityTargeting = adgroups.Where(a => a.RTBAttributes?.QualityAllianceViewabilityTargeting != null)
                          .Select(a => new
                          {
                              a.AdGroupId,
                              Targeting = a.RTBAttributes?.QualityAllianceViewabilityTargeting
                          })
                          .Select(a =>
                             new
                             {
                                 adgroupid = a.AdGroupId,
                                 feepercentofmediacostrate = a.Targeting.Fee?.PercentOfMediaCostRate,
                                 feepercentofdatacostrate = a.Targeting.Fee?.PercentOfDataCostRate,
                                 feecpmrateamount = a.Targeting.Fee?.CPMRate?.Amount,
                                 feecpmratecurrencycode = a.Targeting.Fee?.CPMRate?.CurrencyCode,
                                 feecpmrateinadvertisercurrencyamount = a.Targeting.Fee?.CPMRateInAdvertiserCurrency?.Amount,
                                 feecpmrateinadvertisercurrencycurrencycode = a.Targeting.Fee?.CPMRateInAdvertiserCurrency?.CurrencyCode,
                                 feecpcrateamount = a.Targeting.Fee?.CPCRate?.Amount,
                                 feecpcratecurrencycode = a.Targeting.Fee?.CPCRate?.CurrencyCode,
                                 feecpcrateinadvertisercurrencyamount = a.Targeting.Fee?.CPCRateInAdvertiserCurrency?.Amount,
                                 feecpcrateinadvertisercurrencycurrencycode = a.Targeting.Fee?.CPCRateInAdvertiserCurrency?.CurrencyCode,
                                 qualityallianceviewabilityenabledstate = a.Targeting.QualityAllianceViewabilityEnabledState,
                                 qualityallianceviewabilityminimalpercentage = a.Targeting.QualityAllianceViewabilityMinimalPercentage,
                                 qualityallianceviewabilityprofile = a.Targeting.QualityAllianceViewabilityProfile
                             }
                          );

                        string dimadgroupQualityAllianceViewabilityTargetingTempTableName = "ttd.#dimadgroupqualityallianceviewabilitytargeting_" + fileGuidString;
                        BulkLoadData<dynamic>(adGroupQualityAllianceViewabilityTargeting, sbc, "metadata", dimadgroupQualityAllianceViewabilityTargetingTempTableName);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Processing AdGroupQualityAllianceViewabilityTargeting", JobLogGUID)));

                        #endregion
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start ProcessDimension", JobLogGUID)));

                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(fileGuidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End ProcessDimension", JobLogGUID)));
                    }

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadTTDDeltaAPIData)} | Exception details : {exception}"));
                    trans.Rollback(fileGuidString);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("Start BulkCopy failed: {0}", exc.Message)));
                    trans.Rollback(fileGuidString);
                    throw;
                }
            }//end using-trans
        }//end using connection

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, fileGuidString))
            {
                try
                {
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("Start BulkCopy failed: {0}", exc.Message)));
                    trans.Rollback(fileGuidString);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }
    #endregion

    #region DBM
    [Obsolete]
    public void LoadDBMPublicMetadata(Guid guid, Uri sourcePath, int sourceID, int integrationID, int countryID, IList<FileCollectionItem> files)
    {
        if (files == null || files.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException(JobLogGUID + " - LoadDBMPublicMetadata failed, because no files were passed in");
        }

        var strGruid = guid.ToString().Replace("-", string.Empty).ToString();

        string pattern = @"[\s\(\)\-\/\:\/\\_]+";
        var regEx = new Regex(pattern);

        var guidParam = new SqlParameter(@"@FileGUID", strGruid);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();
        string nsFmt = "Greenhouse.Data.DataSource.DBM.{0}";

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        //Have to break temp table creations into two steps. Doing it in a proc creates them in a separate scope.
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        //TODO: reference sourcefile from S3.
                        sbc.BulkCopyTimeout = 12000;
                        foreach (var file in files)
                        {
                            sbc.ColumnMappings.Clear();
                            string tableName = string.Format("dbm.#dim{0}_{1}", file.SourceFileName, strGruid);
                            sbc.DestinationTableName = tableName;

                            var s3FilePath = RemoteUri.CombineUri(s3Dir.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Start Processing Filename: {0}, s3Path: {1}", file.FilePath, s3FilePath, JobLogGUID)));

                            var s3FileStream = rac.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Retrieved S3 File Stream for file: {0}", file.FilePath, JobLogGUID)));

                            DataTable dt = null;
                            if (file.FilePath.Contains(".csv"))
                            {
                                dt = new DataTable();
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start Loading Filename: {0}", file.FilePath, JobLogGUID)));
                                using (var sr = new StreamReader(s3FileStream))
                                {
                                    using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(sr, true, ',', '"', '\"', '#', LumenWorks.Framework.IO.Csv.ValueTrimmingOptions.None))
                                    {
                                        csvReader.MissingFieldAction = LumenWorks.Framework.IO.Csv.MissingFieldAction.ParseError;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start Streaming CSV file: {0}", file.FilePath, JobLogGUID)));
                                        string[] headers = csvReader.GetFieldHeaders();

                                        foreach (string header in headers)
                                        {
                                            var sanitizedHeader = regEx.Replace(header, string.Empty);
                                            sbc.ColumnMappings.Add(header, sanitizedHeader.ToLower());
                                        }

                                        sbc.WriteToServer(csvReader);
                                    }
                                }
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End Loading Filename: {0}", file.FilePath, JobLogGUID)));
                            }
                            else //JSON
                            {
                                if (file.SourceFileName.Equals("summary", StringComparison.InvariantCulture))
                                {
                                    var summaryFileData = DeserializeJSONStream(new SummaryFile(), s3FileStream);
                                    var summaryData = summaryFileData.SelectMany(x => x.file);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End DeserializeJSON Filename: {0}; Data count {2}", file.FilePath, JobLogGUID, summaryData.Count())));
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Bulk Load Data", JobLogGUID)));
                                    BulkLoadData(summaryData, sbc, file.SourceFileName, tableName);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Bulk Load Data", JobLogGUID)));
                                }
                                else
                                {
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start DeserializeJSON Filename: {0}", file.FilePath, JobLogGUID)));
                                    var assembly = AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(x => x.FullName.StartsWith("Greenhouse.Data", StringComparison.CurrentCultureIgnoreCase));
                                    var objType = assembly.GetType(string.Format(nsFmt, file.SourceFileName), true, true);
                                    var obj = Activator.CreateInstance(objType);

                                    var data = DeserializeJSONStream<dynamic>(obj, s3FileStream);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End DeserializeJSON Filename: {0}; Data count {2}", file.FilePath, JobLogGUID, data.Count)));
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start Bulk Load Data", JobLogGUID)));
                                    BulkLoadData(data, sbc, file.SourceFileName);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End Bulk Load Data", JobLogGUID)));
                                }
                            }
                        }//end loading each source file

                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start processdimensions", JobLogGUID)));
                        cmd.ExecuteNonQuery();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End processdimensions", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start [CompareTableCounts]", JobLogGUID)));
                        cmd.CommandText = "[dbo].[CompareTableCounts]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        int dataValidCount = (int)cmd.ExecuteScalar();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End [CompareTableCounts]=> returned {1}", JobLogGUID, dataValidCount)));
                        if (dataValidCount != 1)
                            throw new ETLProviderException("Temp Data Inserted did not match row counts from summary file");
                    }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadDBMPublicMetadata)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{1} - LoadDBMPublicMetadata failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }

    [Obsolete]
    public void LoadDBMPrivateMetadata(Guid guid, Uri sourcePath, int sourceID, int integrationID, int countryID, IList<FileCollectionItem> files)
    {
        if (files == null || files.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException(JobLogGUID + " - LoadDBMPrivateMetadata failed, because no files were passed in");
        }
        int batchSize = 10000;
        var strGruid = guid.ToString().Replace("-", string.Empty).ToString();

        string pattern = @"[\s\(\)\-\/\:\/\\_]+";
        var regEx = new Regex(pattern);
        var guidParam = new SqlParameter(@"@FileGUID", strGruid);
        var sourceParam = new SqlParameter(@"@SourceID", sourceID);

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        //Have to break temp table creations into two steps. Doing it in a proc creates them in a separate scope.
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = "[dbo].[CreateTempTables]";
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        var result = cmd.ExecuteScalar();

                        cmd.CommandText = result.ToString();
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.Clear();
                        cmd.ExecuteNonQuery();

                        //TODO: reference sourcefile from S3.
                        sbc.BulkCopyTimeout = 36000;
                        string localDir = $"{Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseTransformPath}\\{integrationID}";
                        foreach (var file in files)
                        {
                            sbc.ColumnMappings.Clear();
                            string tableName = string.Format("dbm.#dim{0}_{1}", file.SourceFileName, strGruid);
                            sbc.DestinationTableName = tableName;

                            var s3FilePath = RemoteUri.CombineUri(s3Dir.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Start Processing Filename: {0}, s3Path: {1}", file.FilePath, s3FilePath, JobLogGUID)));

                            //TODO: Check filesize. If it's greater than 1GB, then we should download it locally before streaming it.
                            Stream s3FileStream = null;
                            var s3File = rac.WithFile(s3FilePath);

                            s3FileStream = GetS3FileStream(integrationID, s3File, localDir, guid);

                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Retrieved File Stream for file: {0}", file.FilePath, JobLogGUID)));

                            dynamic dtHelper = new Utilities.DataTables.ObjectShredder<object>();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start DeserializeJSONStream Filename: {0}", file.FilePath, JobLogGUID)));
                            switch (file.SourceFileName.ToLower())
                            {

                                case "advertiser":
                                    #region advertiser
                                    foreach (var advertiserObject in DeserializeJSONStream(new Advertiser(), s3FileStream, batchSize))
                                    {
                                        int advertiserCount = advertiserObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, advertiserCount)));
                                        if (advertiserCount == 0)
                                            break;

                                        var commonAdvertiserObject = advertiserObject.Where(x => x.common_data != null);
                                        var advertiserData = commonAdvertiserObject
                                            .Select(x => new
                                            {
                                                x.common_data.id,
                                                x.common_data.active,
                                                x.common_data.integration_code,
                                                x.common_data.name,
                                                x.currency_code,
                                                x.dcm_configuration,
                                                x.dcm_floodlight_group_id,
                                                x.dcm_network_id,
                                                x.enable_oba_tags,
                                                x.timezone_code,
                                                x.landing_page_url,
                                                x.partner_id
                                            });

                                        var advertiserBlacklistChannelData = commonAdvertiserObject.Where(x => x.blacklist_channel_id != null && x.blacklist_channel_id.Count != 0)
                                            .SelectMany(x => x.blacklist_channel_id.Select(b => new { x.common_data.id, blacklistchannelid = b }));

                                        var advertiserDcmData = commonAdvertiserObject.Where(x => x.dcm_advertiser_id != null && x.dcm_advertiser_id.Count != 0)
                                            .SelectMany(x => x.dcm_advertiser_id.Select(b => new { x.common_data.id, dcmadvertiserid = b }));

                                        var advertiserDcmSyncSiteData = commonAdvertiserObject.Where(x => x.dcm_syncable_site_ids != null && x.dcm_syncable_site_ids.Count != 0)
                                            .SelectMany(x => x.dcm_syncable_site_ids.Select(b => new { x.common_data.id, dcmsyncablesiteid = b }));

                                        BulkLoadData<dynamic>(advertiserData, sbc, file.SourceFileName, tableName);
                                        BulkLoadData<dynamic>(advertiserBlacklistChannelData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toblacklistchannel_{1}", file.SourceFileName, strGruid));
                                        BulkLoadData<dynamic>(advertiserDcmData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}todcmadvertiser_{1}", file.SourceFileName, strGruid));
                                        BulkLoadData<dynamic>(advertiserDcmSyncSiteData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}todcmsyncablesite_{1}", file.SourceFileName, strGruid));
                                    }//end foreach

                                    #endregion
                                    break;

                                case "creative":
                                    #region creative
                                    foreach (var creativeObject in DeserializeJSONStream(new Creative(), s3FileStream, batchSize))
                                    {
                                        int creativeCount = creativeObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, creativeCount)));
                                        if (creativeCount == 0)
                                            break;

                                        var commonCreativeData = creativeObject.Where(x => x.common_data != null);
                                        var creativeData = commonCreativeData.Select(x => new
                                        {
                                            x.common_data.id,
                                            x.common_data.integration_code,
                                            x.common_data.name,
                                            x.common_data.active,
                                            x.advertiser_id,
                                            x.creative_type,
                                            x.dcm_placement_id,
                                            x.height_pixels,
                                            x.width_pixels

                                        });

                                        BulkLoadData<dynamic>(creativeData, sbc, file.SourceFileName, tableName);

                                        var creativeApprovalStatusData = commonCreativeData.Where(x => x.approval_status != null && x.approval_status.Count != 0)
                                                                        .SelectMany(x => x.approval_status.Select(s => new
                                                                        {
                                                                            x.common_data.id,
                                                                            s.auditor,
                                                                            s.status,
                                                                            s.sync_time,
                                                                            s.external_id
                                                                        }));

                                        BulkLoadData<dynamic>(creativeApprovalStatusData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toapprovalstatus_{1}", file.SourceFileName, strGruid));
                                    }
                                    #endregion
                                    break;
                                case "customaffinity":
                                    foreach (var customAffinityData in DeserializeJSONStream(new CustomAffinity(), s3FileStream, batchSize))
                                    {
                                        int customAffinityCount = customAffinityData?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, customAffinityCount)));
                                        if (customAffinityCount == 0)
                                            break;

                                        BulkLoadData<dynamic>(customAffinityData, sbc, file.SourceFileName, tableName);
                                    }

                                    break;

                                case "inventorysource":

                                    #region inventory source
                                    foreach (var inventoryObject in DeserializeJSONStream(new InventorySource(), s3FileStream, batchSize))
                                    {
                                        int inventoryCount = inventoryObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, inventoryCount)));

                                        if (inventoryCount == 0)
                                            break;

                                        var inventoryData = inventoryObject.Select(x => new
                                        {
                                            x.id,
                                            x.unclassified,
                                            x.exchange_id,
                                            x.inventory_name,
                                            x.external_id,
                                            x.min_cpm_micros,
                                            x.min_cpm_currency_code
                                        });

                                        var inventoryAdvertiser = inventoryObject.Where(x => x.accessing_advertisers != null && x.accessing_advertisers.Count != 0).
                                            SelectMany(x => x.accessing_advertisers.Select(a => new { x.id, advertiserid = a }));

                                        BulkLoadData<dynamic>(inventoryData, sbc, file.SourceFileName, tableName);
                                        BulkLoadData<dynamic>(inventoryAdvertiser, sbc, "InventoryAdvertiser", string.Format("dbm.#dim{0}toaccessingadvertiser_{1}", file.SourceFileName, strGruid));
                                    }
                                    #endregion
                                    break;
                                case "insertionorder":
                                    foreach (var insertionOrderObject in DeserializeJSONStream(new InsertionOrder(), s3FileStream, batchSize))
                                    {
                                        int insertionorderCount = insertionOrderObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, insertionorderCount)));
                                        if (insertionorderCount == 0)
                                            break;

                                        var commonInsertionOrderObject = insertionOrderObject.Where(x => x.common_data != null);
                                        var insertionOrderData = commonInsertionOrderObject
                                            .Select(x =>
                                            {
                                                if (x.default_partner_costs == null)
                                                    x.default_partner_costs = new DefaultPartnerCost();
                                                if (x.overall_budget == null)
                                                    x.overall_budget = new OverallBudget();
                                                if (x.frequency_cap == null)
                                                    x.frequency_cap = new FrequencyCap();
                                                return x;
                                            }).Select(x => new
                                            {
                                                x.common_data.id,
                                                x.common_data.active,
                                                x.common_data.integration_code,
                                                x.common_data.name,
                                                x.advertiser_id,
                                                x.overall_budget.end_time_usec,
                                                x.overall_budget.pacing_type,
                                                x.overall_budget.start_time_usec,
                                                x.overall_budget.max_spend_advertiser_micros,
                                                x.overall_budget.pacing_max_spend_advertiser_micros,
                                                x.overall_budget.pacing_distribution,
                                                x.frequency_cap.max_impressions,
                                                x.frequency_cap.time_range,
                                                x.frequency_cap.time_unit,
                                                x.default_partner_costs.cpm_fee_1_advertiser_micros,
                                                x.default_partner_costs.cpm_fee_1_bill_to_type,
                                                x.default_partner_costs.cpm_fee_1_cost_type,
                                                x.default_partner_costs.cpm_fee_2_advertiser_micros,
                                                x.default_partner_costs.cpm_fee_2_bill_to_type,
                                                x.default_partner_costs.cpm_fee_2_cost_type,
                                                x.default_partner_costs.media_fee_percent_1_bill_to_type,
                                                x.default_partner_costs.media_fee_percent_1_cost_type,
                                                x.default_partner_costs.media_fee_percent_1_millis,
                                                x.default_partner_costs.media_fee_percent_2_bill_to_type,
                                                x.default_partner_costs.media_fee_percent_2_cost_type,
                                                x.default_partner_costs.media_fee_percent_2_millis,
                                                x.default_target_list.kct_include_uncrawled_sites
                                            });

                                        BulkLoadData<dynamic>(insertionOrderData, sbc, file.SourceFileName, tableName);

                                        var ioScheduledSegmentData = commonInsertionOrderObject.Where(x => x.scheduled_segments != null && x.scheduled_segments.Count != 0)
                                                                    .SelectMany(x => x.scheduled_segments.Select(s => new
                                                                    {
                                                                        x.common_data.id,
                                                                        s.start_time_usec,
                                                                        s.end_time_usec,
                                                                        max_spend_advertiser_micros = s.max_spend_advertiser_micros ?? ""
                                                                    }));

                                        BulkLoadData<dynamic>(ioScheduledSegmentData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toscheduledsegment_{1}", file.SourceFileName, strGruid));

                                        var ioDefaultTargetListObject = commonInsertionOrderObject.Where(x => x.default_target_list != null).Select(x => new { x.common_data.id, x.default_target_list });

                                        var ioGeoLocationData = ioDefaultTargetListObject.Where(x => x.default_target_list.geo_locations != null).SelectMany(x => x.default_target_list.geo_locations.Select(d => new
                                        {
                                            x.id,
                                            d.criteria_id,
                                            d.excluded
                                        }));

                                        BulkLoadData<dynamic>(ioGeoLocationData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}togeolocation_{1}", file.SourceFileName, strGruid));

                                        var ioInventorySourcesData = ioDefaultTargetListObject.Where(x => x.default_target_list.inventory_sources != null).SelectMany(x => x.default_target_list.inventory_sources.Select(d => new
                                        {
                                            x.id,
                                            d.criteria_id,
                                            d.excluded
                                        }));

                                        BulkLoadData<dynamic>(ioInventorySourcesData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toinventorysource_{1}", file.SourceFileName, strGruid));

                                        var ioKeywordsData = ioDefaultTargetListObject.Where(x => x.default_target_list.keywords != null).SelectMany(x => x.default_target_list.keywords.Select(d => new
                                        {
                                            x.id,
                                            d.parameter,
                                            d.excluded
                                        }));
                                        BulkLoadData<dynamic>(ioKeywordsData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}tokeyword_{1}", file.SourceFileName, strGruid));

                                        var ioLanguageData = ioDefaultTargetListObject.Where(x => x.default_target_list.languages != null && x.default_target_list.languages.union != null)
                                                        .SelectMany(x => x.default_target_list.languages.union.Select(d => new
                                                        {
                                                            x.id,
                                                            d.criteria_id,
                                                            d.parameter,
                                                            x.default_target_list.languages.excluded
                                                        }));
                                        BulkLoadData<dynamic>(ioLanguageData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}tolanguage_{1}", file.SourceFileName, strGruid));

                                        var ioUniversalChannelData = ioDefaultTargetListObject.Where(x => x.default_target_list.universal_channels != null)
                                                            .SelectMany(x => x.default_target_list.universal_channels.Select(d => new
                                                            {
                                                                x.id,
                                                                d.criteria_id,
                                                                d.excluded
                                                            }));
                                        BulkLoadData<dynamic>(ioUniversalChannelData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}touniversalchannel_{1}", file.SourceFileName, strGruid));

                                        var ioAudienceIntersectData = ioDefaultTargetListObject
                                            .Where(x => x.default_target_list.audience_intersect != null)
                                                            .SelectMany(x => x.default_target_list.audience_intersect
                                                                .Where(au => au.union != null)
                                                                .SelectMany(adi => adi.union.Select(d => new
                                                                {
                                                                    x.id,
                                                                    adi.excluded,
                                                                    d.criteria_id,
                                                                    d.parameter
                                                                })));
                                        BulkLoadData<dynamic>(ioAudienceIntersectData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toaudienceintersect_{1}", file.SourceFileName, strGruid));

                                        var ioSiteData = ioDefaultTargetListObject.Where(x => x.default_target_list.sites != null).SelectMany(x => x.default_target_list.sites.Select(d => new
                                        {
                                            x.id,
                                            d.excluded,
                                            d.parameter
                                        }));
                                        BulkLoadData<dynamic>(ioSiteData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}tosite_{1}", file.SourceFileName, strGruid));
                                    }//end foreach

                                    break;

                                case "lineitem":
                                    foreach (var lineItemObject in DeserializeJSONStream(new LineItem(), s3FileStream, batchSize))
                                    {
                                        int lineItemObjectCount = lineItemObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, lineItemObjectCount)));
                                        if (lineItemObjectCount == 0)
                                            break;

                                        var lineItemData = lineItemObject.Select(x => new
                                        {
                                            x.common_data.id,
                                            x.common_data.name,
                                            x.common_data.integration_code,
                                            x.common_data.active,
                                            x.line_item_type,
                                            x.insertion_order_id,
                                            x.max_cpm_advertiser_micros,
                                            x.performance_goal,
                                            x.partner_revenue_model.type,
                                            x.partner_revenue_model.media_cost_markup_percent_millis,
                                            x.partner_revenue_model.post_view_conversion_tracking_fraction,
                                            x.budget.start_time_usec,
                                            x.budget.end_time_usec,
                                            x.budget.pacing_type,
                                            x.budget.max_spend_advertiser_micros,
                                            x.budget.pacing_distribution,
                                            x.partner_costs.cpm_fee_1_advertiser_micros,
                                            x.partner_costs.cpm_fee_2_advertiser_micros,
                                            x.partner_costs.cpm_fee_1_bill_to_type,
                                            x.partner_costs.cpm_fee_1_cost_type,
                                            x.partner_costs.cpm_fee_2_bill_to_type,
                                            x.partner_costs.cpm_fee_2_cost_type,
                                            x.partner_costs.media_fee_percent_1_bill_to_type,
                                            x.partner_costs.media_fee_percent_1_cost_type,
                                            x.partner_costs.media_fee_percent_1_millis,
                                            x.partner_costs.media_fee_percent_2_bill_to_type,
                                            x.partner_costs.media_fee_percent_2_cost_type,
                                            x.partner_costs.media_fee_percent_2_millis,
                                            x.target_list.kct_include_uncrawled_sites,
                                            x.frequency_cap?.max_impressions,
                                            x.frequency_cap?.time_range,
                                            x.frequency_cap?.time_unit
                                        });

                                        BulkLoadData<dynamic>(lineItemData, sbc, file.SourceFileName, tableName);

                                        var loCreativeData = lineItemObject.Where(x => x.creative_ids != null).SelectMany(x => x.creative_ids.Select(d => new
                                        {
                                            x.common_data.id,
                                            creative_id = d
                                        }));

                                        BulkLoadData<dynamic>(loCreativeData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}tocreative_{1}", file.SourceFileName, strGruid));

                                        var loCommonData = lineItemObject.Where(x => x.target_list != null).Select(x => new { x.common_data.id, data = x.target_list });

                                        var loGeolocationData = loCommonData.Where(x => x.data.geo_locations != null).SelectMany(x => x.data.geo_locations.Select(d => new
                                        {
                                            x.id,
                                            d.criteria_id,
                                            d.excluded
                                        }));

                                        BulkLoadData<dynamic>(loGeolocationData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}togeolocation_{1}", file.SourceFileName, strGruid));

                                        var loInventorySource = loCommonData.Where(x => x.data.inventory_sources != null).SelectMany(x => x.data.inventory_sources.Select(d => new
                                        {
                                            x.id,
                                            d.criteria_id,
                                            d.excluded
                                        }));

                                        BulkLoadData<dynamic>(loInventorySource, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toinventorysource_{1}", file.SourceFileName, strGruid));

                                        var loAudienceIntersectData = loCommonData.Where(x => x.data.audience_intersect != null)
                                            .SelectMany(x => x.data.audience_intersect
                                            .Where(un => un.union != null)
                                            .SelectMany(un => un.union.Select(d => new
                                            {
                                                x.id,
                                                un.excluded,
                                                d.criteria_id,
                                                d.parameter
                                            })));

                                        BulkLoadData<dynamic>(loAudienceIntersectData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toaudienceintersect_{1}", file.SourceFileName, strGruid));

                                        var loSiteData = loCommonData.Where(x => x.data.sites != null).SelectMany(x => x.data.sites.Select(d => new
                                        {
                                            x.id,
                                            d.parameter,
                                            d.excluded
                                        }));

                                        BulkLoadData<dynamic>(loSiteData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}tosite_{1}", file.SourceFileName, strGruid));

                                        var loUniversalChannelData = loCommonData.Where(x => x.data.universal_channels != null).SelectMany(x => x.data.universal_channels.Select(d => new
                                        {
                                            x.id,
                                            d.excluded,
                                            d.criteria_id
                                        }));

                                        BulkLoadData<dynamic>(loUniversalChannelData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}touniversalchannel_{1}", file.SourceFileName, strGruid));

                                        var loPageCategory = loCommonData.Where(x => x.data.page_categories != null).SelectMany(x => x.data.page_categories.Select(d => new
                                        {
                                            x.id,
                                            d.excluded,
                                            d.criteria_id
                                        }));

                                        BulkLoadData<dynamic>(loPageCategory, sbc, file.SourceFileName, string.Format("dbm.#dim{0}topagecategory_{1}", file.SourceFileName, strGruid));
                                    }//end foreach

                                    break;
                                case "partner":
                                    #region partner
                                    foreach (var partnerObject in DeserializeJSONStream(new Partner(), s3FileStream, batchSize))
                                    {
                                        var partnerCount = partnerObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, partnerCount)));

                                        if (partnerCount == 0)
                                            break;

                                        var partnerData = partnerObject.Select(x => new
                                        {
                                            x.common_data.id,
                                            x.common_data.name,
                                            x.common_data.active,
                                            x.currency_code,
                                            x.default_partner_costs.cpm_fee_1_advertiser_micros,
                                            x.default_partner_costs.cpm_fee_1_bill_to_type,
                                            x.default_partner_costs.cpm_fee_1_cost_type,
                                            x.default_partner_costs.cpm_fee_2_advertiser_micros,
                                            x.default_partner_costs.cpm_fee_2_bill_to_type,
                                            x.default_partner_costs.cpm_fee_2_cost_type,
                                            x.default_partner_costs.media_fee_percent_1_bill_to_type,
                                            x.default_partner_costs.media_fee_percent_1_cost_type,
                                            x.default_partner_costs.media_fee_percent_1_millis,
                                            x.default_partner_costs.media_fee_percent_2_bill_to_type,
                                            x.default_partner_costs.media_fee_percent_2_cost_type,
                                            x.default_partner_costs.media_fee_percent_2_millis,
                                            x.default_partner_revenue_model.type,
                                            x.default_partner_revenue_model.media_cost_markup_percent_millis,
                                            x.default_partner_revenue_model.post_view_conversion_tracking_fraction,
                                            x.default_target_list.kct_include_uncrawled_sites
                                        });

                                        BulkLoadData<dynamic>(partnerData, sbc, file.SourceFileName, tableName);

                                        var partnerInventorySourceData = partnerObject.Where(x => x.default_target_list != null).
                                            SelectMany(x => x.default_target_list.inventory_sources.Select(d => new
                                            {
                                                x.common_data.id,
                                                d.criteria_id,
                                                d.excluded
                                            }));

                                        BulkLoadData<dynamic>(partnerInventorySourceData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toinventorysource_{1}", file.SourceFileName, strGruid));

                                        var partnerExchangeSettingData = partnerObject.Where(x => x.default_target_list != null && x.exchange_settings != null)
                                            .SelectMany(x => x.exchange_settings.Select(d => new
                                            {
                                                x.common_data.id,
                                                d.exchange_id,
                                                d.seat_id
                                            }));

                                        BulkLoadData<dynamic>(partnerExchangeSettingData, sbc, file.SourceFileName, string.Format("dbm.#dim{0}toexchangesetting_{1}", file.SourceFileName, strGruid));
                                    }//end foreach

                                    #endregion

                                    break;
                                case "pixel":
                                    foreach (var pixelObject in DeserializeJSONStream(new Pixel(), s3FileStream, batchSize))
                                    {
                                        int pixelCount = pixelObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, pixelCount)));
                                        if (pixelCount == 0)
                                            break;

                                        var pixelData = pixelObject.Where(x => x.common_data != null).Select(x => new
                                        {
                                            x.common_data.id,
                                            x.common_data.name,
                                            x.common_data.integration_code,
                                            x.advertiser_id,
                                            x.partner_id,
                                            x.dcm_floodlight_id,
                                            x.allow_google_redirect,
                                            x.allow_rm_redirect,
                                            x.remarketing_enabled,
                                            x.is_secure
                                        });

                                        BulkLoadData<dynamic>(pixelData, sbc, file.SourceFileName, tableName);
                                    }//end foreach

                                    break;
                                case "summary":
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start DeserializeJSON Filename: {0}", file.FilePath, JobLogGUID)));
                                    var summaryData = DeserializeJSONStream(new SummaryFile(), s3FileStream);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End DeserializeJSON Filename: {0}", file.FilePath, JobLogGUID)));

                                    var summaries = summaryData.SelectMany(x => x.file);
                                    BulkLoadData<dynamic>(summaries, sbc, file.SourceFileName, tableName);

                                    break;
                                case "userlist":
                                    #region user list
                                    foreach (var userListObject in DeserializeJSONStream(new UserList(), s3FileStream, batchSize))
                                    {
                                        var userListCount = userListObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, userListCount)));
                                        if (userListCount == 0)
                                            break;

                                        var userListData = userListObject.Select(x => new
                                        {
                                            x.id,
                                            x.name
                                        });

                                        var userListAdvertiserData = userListObject.Where(x => x.accessing_advertisers != null && x.accessing_advertisers.Count != 0)
                                                                            .SelectMany(x => x.accessing_advertisers
                                                                                .Select(a => new { x.id, advertiserid = a }));

                                        BulkLoadData<dynamic>(userListData, sbc, file.SourceFileName, tableName);
                                        BulkLoadData<dynamic>(userListAdvertiserData, sbc, "UserListAdvertiser", string.Format("dbm.#dim{0}toaccessingadvertiser_{1}", file.SourceFileName, strGruid));
                                    }
                                    #endregion
                                    break;
                                case "universalchannel":
                                    #region universal channel

                                    foreach (var universalChannelObject in DeserializeJSONStream(new UniversalChannel(), s3FileStream, batchSize))
                                    {
                                        var universalChannelCount = universalChannelObject?.Count ?? 0;
                                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - DeserializeJSONStream Filename: {0} returned {2} records", file.FilePath, JobLogGUID, universalChannelCount)));
                                        if (universalChannelCount == 0)
                                            break;
                                        var universalChannelData = universalChannelObject.Select(x => new
                                        {
                                            x.id,
                                            x.name,
                                            x.is_brand_safe_channel,
                                            x.is_deleted
                                        });

                                        BulkLoadData<dynamic>(universalChannelData, sbc, file.SourceFileName, tableName);

                                        var universalChannelSiteData = universalChannelObject.Where(x => x.site_ids != null && x.site_ids.Count != 0).SelectMany(x => x.site_ids.Select(a => new { x.id, siteid = a }));
                                        BulkLoadData<dynamic>(universalChannelSiteData, sbc, "UniversalChannelSite", string.Format("dbm.#dim{0}tosite_{1}", file.SourceFileName, strGruid));

                                        var universalChannelAdvertiserData = universalChannelObject.Where(x => x.accessing_advertisers != null && x.accessing_advertisers.Count != 0)
                                                    .SelectMany(x => x.accessing_advertisers.Select(a => new { x.id, advertiserid = a }));
                                        BulkLoadData<dynamic>(universalChannelAdvertiserData, sbc, "UniversalChannelAdvertiser", string.Format("dbm.#dim{0}toaccessingadvertiser_{1}", file.SourceFileName, strGruid));
                                    } //end foreach

                                    #endregion
                                    break;
                            }//end switch	
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End DeserializeJSONStream Stream Filename: {0}", file.FilePath, JobLogGUID)));
                        }//end loading each source file

                        if (System.IO.Directory.Exists(localDir))
                        {
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - {2} Start Deleting local files in : {0}", localDir, JobLogGUID, guid)));
                            System.IO.Directory.Delete(localDir, true);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - {2} End Deleting local files in : {0}", localDir, JobLogGUID, guid)));
                        }

                        var integrationParam = new SqlParameter(@"@IntegrationID", integrationID);
                        var countryParam = new SqlParameter(@"@CountryID", countryID);

                        cmd.CommandText = "[dbo].[ProcessDimensions]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        cmd.Parameters.Add(integrationParam);
                        cmd.Parameters.Add(countryParam);
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 8000;

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start processdimensions", JobLogGUID)));
                        cmd.ExecuteNonQuery();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End processdimensions", JobLogGUID)));

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start [CompareTableCounts]", JobLogGUID)));
                        cmd.CommandText = "[dbo].[CompareTableCounts]";
                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(guidParam);
                        cmd.Parameters.Add(sourceParam);
                        int dataValidCount = (int)cmd.ExecuteScalar();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End [CompareTableCounts]=> returned {1}", JobLogGUID, dataValidCount)));
                        if (dataValidCount != 1)
                            throw new ETLProviderException("Temp Data Inserted did not match row counts from summary file");
                    }//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadDBMPrivateMetadata)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{1} - LoadDBMPrivateMetadata failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            }//end using-trans
        }//end using connection
    }

    private Stream GetS3FileStream(int integrationID, IFile s3File, string localDirectory, Guid fileGUID)
    {
        Stream s3FileStream;
        var gbSize = 1024 * 1024 * 1024;

        if ((double)(s3File.Length / gbSize) > 1)
        {
            FileSystemFile localFile = CreateLocalFile(s3File.Name, localDirectory);

            s3File.CopyTo(localFile, true);
            s3FileStream = localFile.Get();

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - {2} Copy file locally: {0}", localFile, JobLogGUID, fileGUID)));
        }
        else
        {
            s3FileStream = s3File.Get();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - {2} Stream file directly from s3: {0}", s3File.FullName, JobLogGUID, fileGUID)));
        }

        return s3FileStream;
    }

    /// <summary>
    /// creates file and directory if they do not exists. Always overwrite
    /// </summary>
    /// <param name="fileName"></param>
    /// <param name="localDirectory"></param>
    /// <returns></returns>
    private static FileSystemFile CreateLocalFile(string fileName, string localDirectory, bool overWrite = true)
    {
        var localFileName = $"{localDirectory.TrimEnd(Constants.BACKWARD_SLASH_ARRAY)}\\{fileName}";
        var localFile = new FileSystemFile(new Uri(@localFileName));

        if (!localFile.Directory.Exists)
            localFile.Directory.Create();

        localDirectory = localFile.Directory.ToString();
        if (overWrite && localFile.Exists)
            localFile.Delete();
        return localFile;
    }

    [Obsolete]
    private void BulkCopyDatatable(DataTable source, SqlBulkCopy sbc, string fileType, string destinationTable = "")
    {
        string pattern = @"[\s\(\)\-\/\:\/\\_]+";
        var regEx = new Regex(pattern);

        sbc.ColumnMappings.Clear();
        foreach (var header in source.Columns.Cast<DataColumn>())
        {
            var sanitizedHeader = regEx.Replace(header.ColumnName, string.Empty);
            sbc.ColumnMappings.Add(header.ColumnName, sanitizedHeader);
        }

        if (!string.IsNullOrEmpty(destinationTable))
            sbc.DestinationTableName = destinationTable;

        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Start sqlbulkcopy Filename: {0}; Table: {2}; Row Count: {3}", fileType, JobLogGUID, source.TableName, source.Rows.Count)));
        sbc.WriteToServer(source);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - End sqlbulkcopy Filename: {0}; Table: {2}; Row Count: {3}", fileType, JobLogGUID, source.TableName, source.Rows.Count)));
    }

    public static T DeserializeJSON<T>(Stream file)
    {
        var jsonSerializer = new JsonSerializer();
        T data = default(T);
        using (var strm = new StreamReader(file))
        {
            using (var r = new JsonTextReader(strm))
            {
                data = jsonSerializer.Deserialize<T>(r);
            }
        }
        return data;
    }

    public static IEnumerable<DataTable> CreateDataTables<T>(IEnumerable<T> source, string tableName = "")
    {
        var dataTables = new List<DataTable>();
        DataTable dt = null;
        var datalist = UtilsText.GetSublistFromList(source, 50000);
        var dtExt = new Utilities.DataTables.ObjectShredder<T>();

        foreach (var list in datalist)
        {
            var dataTable = dtExt.Shred(list, dt, null);
            if (dt == null)
                dt = dataTable.Clone();

            dataTable.TableName = string.Format("{0}", tableName);
            yield return dataTable;
        }
    }

    [Obsolete]
    public void BulkLoadData<T>(IEnumerable<T> source, SqlBulkCopy sbc, string sourceFileType, string tableName = "")
    {
        DataTable dt = null;
        var datalist = UtilsText.GetSublistFromList(source, 50000);
        var dtExt = new Utilities.DataTables.ObjectShredder<T>();

        foreach (var list in datalist)
        {
            var dataTable = dtExt.Shred(list, dt, null);
            if (dt == null)
                dt = dataTable.Clone();

            dataTable.TableName = string.Format("{0}", tableName);
            BulkCopyDatatable(dataTable, sbc, sourceFileType, tableName);
            dataTable.Clear();
            dataTable.Dispose();
        }
    }

    public static List<T> DeserializeJSONStream<T>(T input, Stream fileStream)
    {
        var jsonSerializer = new JsonSerializer();
        var dataList = new List<T>();
        using (var strm = new StreamReader(fileStream))
        {
            using (var r = new JsonTextReader(strm))
            {
                r.SupportMultipleContent = true;
                while (r.Read())
                {
                    if (r.TokenType == JsonToken.StartObject)
                    {
                        var data = (T)jsonSerializer.Deserialize(r, input.GetType());
                        if (data != null)
                        {
                            dataList.Add(data);
                        }
                    }
                }
            }
        }
        return dataList;
    }

    /// <summary>
    /// Used for large files. Stream and return a deserialized set number of records.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="input">Concrete Type to help compiler determine the correct Type of T</param>
    /// <param name="fileStream"></param>
    /// <param name="records">number of records to return per set</param>
    /// <returns></returns>
    public static IEnumerable<List<T>> DeserializeJSONStream<T>(T input, Stream fileStream, int records = 50000)
    {
        var jsonSerializer = new JsonSerializer();
        var dataList = new List<T>();

        using (var strm = new StreamReader(fileStream))
        {
            int count = 0;
            using (var reader = new JsonTextReader(strm))
            {
                reader.SupportMultipleContent = true;
                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        if (count > 0 && count++ % records == 0)
                        {
                            yield return dataList;
                            dataList.Clear();
                        }

                        var data = (T)jsonSerializer.Deserialize(reader, input.GetType());
                        if (data != null)
                        {
                            dataList.Add(data);
                        }
                    }
                }
            }//end using reader
        }//end stream

        yield return dataList;
    }
    #endregion

    #region SizmekDSP Metadata
    [Obsolete]
    public void LoadSizmekDSPMetadata(Guid guid, Uri sourcePath, int sourceID, int integrationID, int countryID, string fileName, DateTime fileDate)
    {
        if (string.IsNullOrEmpty(fileName))
        {
            throw new NullOrEmptyFileCollectionException($"{JobLogGUID}, {guid.ToString()} - LoadSizmekDSPMetadata failed, because no files were passed in");
        }

        var strGruid = guid.ToString().Replace("-", string.Empty).ToString();

        string tableName = "Dimension_Stage.dbo.SizmekDSPMetadata";

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        #region Stream file into DataTable
        var s3FilePath = RemoteUri.CombineUri(s3Dir.Uri, fileName);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Start Processing Filename: {fileName}, s3Path: {s3FilePath}, {JobLogGUID}, {guid.ToString()}"));

        var s3FileStream = rac.WithFile(s3FilePath).Get();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} -Retrieved S3 File Stream for file: {fileName}"));

        DataTable dt = new DataTable("SizmekDSP-Metadata");
        using (var zipStream = new GZipInputStream(s3FileStream))
        {
            using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(new StreamReader(zipStream), true))
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {fileName} into DataTable"));
                dt.Load(csvReader);
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {fileName} into DataTable"));
            }
        }
        dt.Columns[0].ColumnName = "objectid";
        dt.Columns[1].ColumnName = "objecttype";
        dt.Columns[2].ColumnName = "objectname";
        dt.Columns[3].ColumnName = "parentid";
        dt.Columns.Add(new DataColumn("FileGUID", typeof(string)) { DefaultValue = guid });

        #endregion

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        //truncate staging
                        var cmd = connection.CreateCommand();
                        cmd.Transaction = trans;
                        cmd.Connection = connection;
                        cmd.CommandText = $"Truncate table {tableName}";
                        cmd.CommandType = CommandType.Text;
                        var result = cmd.ExecuteNonQuery();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - truncated staging table: {tableName}"));

                        sbc.BulkCopyTimeout = 12000;
                        sbc.ColumnMappings.Clear();
                        sbc.DestinationTableName = tableName;
                        sbc.BatchSize = 100000;

                        for (int i = 0; i < dt.Columns.Count; i++)
                        {
                            sbc.ColumnMappings.Add(i, dt.Columns[i].ColumnName);
                        }

                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - Start BulkCopy Filename: {fileName}"));
                        sbc.WriteToServer(dt);
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - End BulkCopy Filename: {fileName}"));

                        cmd.CommandText = "Dimension_Stage.dbo.ProcessDimensions_SizmekDSP";
                        cmd.Parameters.Clear();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }

                    trans.Commit();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID},{guid.ToString()} - Start BulkCopy completed: {fileName}"));
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID), exc));
                    throw;
                }
            }//end using-trans
        }//end using connection
    }
    #endregion

    #region Generic Metadata

    public void LoadMetadataStage(Guid guid, Uri sourcePath, int sourceID, int integrationID, int countryID,
        string fileName, DateTime fileDate, string sourceFileName, string postProcessing, long fileSize)
    {
        var file = new FileCollectionItem()
        {
            FilePath = fileName,
            SourceFileName = sourceFileName,
            FileSize = fileSize
        };

        var filesJSON = new List<FileCollectionItem>() { file };

        LoadMetadataStageCollection(guid, sourcePath, sourceID, integrationID, countryID,
            filesJSON, postProcessing);
    }

    [Obsolete]
    public void LoadMetadataStageCollection(Guid guid, Uri sourcePath, int sourceID, int integrationID,
        int countryID, IList<FileCollectionItem> files, string postProcessing)
    {
        if (files == null || files.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException($"{JobLogGUID}, {guid.ToString()} - LoadMetadataStageCollection failed, because no files were passed in");
        }

        var strGruid = guid.ToString().Replace("-", string.Empty).ToString();

        var stageMetadataTables =
            SetupService.GetAll<MetadataStageConfiguration>().Where(x => x.SourceID == sourceID);
        var sourceFiles = SetupService.GetAll<SourceFile>().Where(x => x.SourceID == sourceID);

        var stageMetadataConfig =
            stageMetadataTables.Join(sourceFiles, m => m.SourceFileID, s => s.SourceFileID, (m, s) =>
            {
                var metadataTableWithSourceFile = new
                {
                    tableName = m.TableName,
                    sourceFileName = s.SourceFileName,
                    fileDelimiter = s.FileDelimiter,
                    quotedIdentifier = s.QuotedIdentifier,
                    hasHeader = s.HasHeader,
                    FieldName = m.FieldName,
                    regexMask = s.RegexMask,
                    fieldOrder = m.FieldOrder
                };
                return metadataTableWithSourceFile;
            }).GroupBy(x => x.tableName)
                .Select(sf =>
                {
                    var values = sf.First();
                    var metadataTableWithSourceFileGroup = new
                    {
                        tableName = sf.Key,
                        sourceFileName = values.sourceFileName,
                        fileDelimiter = values.fileDelimiter,
                        quotedIdentifier = values.quotedIdentifier,
                        hasHeader = values.hasHeader,
                        metadataColumns = sf.OrderBy(x => x.fieldOrder).ToList(),
                        regexMask = values.regexMask
                    };
                    return metadataTableWithSourceFileGroup;
                });

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        var cmd = connection.CreateCommand();

                        sbc.BulkCopyTimeout = 12000;
                        foreach (var file in files)
                        {
                            var metadataConfig =
                                stageMetadataConfig.FirstOrDefault(
                                    x => x.sourceFileName.Equals(file.SourceFileName));

                            #region Stream file into DataTable

                            var s3FilePath =
                                RemoteUri.CombineUri(s3Dir.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{2} - Start Processing Filename: {0}, s3Path: {1}",
                                    file.FilePath, s3FilePath, JobLogGUID)));

                            var s3FileStream = rac.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{1} - Retrieved S3 File Stream for file: {0}", file.FilePath,
                                    JobLogGUID)));

                            string tableName = "Dimension_Stage.dbo." + metadataConfig.tableName;

                            var fieldSeparator = Regex.Unescape(metadataConfig.fileDelimiter);
                            char delimiter = char.Parse(fieldSeparator);

                            var quote = '\0';
                            if (!string.IsNullOrEmpty(metadataConfig.quotedIdentifier))
                            {
                                var quotedIdentifier = Regex.Unescape(metadataConfig.quotedIdentifier);
                                quote = char.Parse(quotedIdentifier);
                            }

                            DataTable dt = new DataTable(tableName);
                            using (var zipStream = new GZipInputStream(s3FileStream))
                            {
                                using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(
                                    new StreamReader(zipStream), metadataConfig.hasHeader, delimiter, quote, '\0', '\0',
                                    LumenWorks.Framework.IO.Csv.ValueTrimmingOptions.None))
                                {
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                        $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {file.FilePath} into DataTable"));
                                    dt.Load(csvReader);
                                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                        $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {file.FilePath} into DataTable"));
                                }
                            }

                            for (int i = 0; i < metadataConfig.metadataColumns.Count; i++)
                            {
                                dt.Columns[i].ColumnName = metadataConfig.metadataColumns[i].FieldName;
                            }

                            dt.Columns.Add(new DataColumn("FileGUID", typeof(string)) { DefaultValue = guid });

                            #endregion

                            cmd.Transaction = trans;
                            cmd.Connection = connection;
                            cmd.CommandText = $"Truncate table {tableName}";
                            cmd.CommandType = CommandType.Text;
                            var result = cmd.ExecuteNonQuery();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                $"{JobLogGUID},{guid.ToString()} - truncated staging table: {tableName}"));

                            sbc.BulkCopyTimeout = 12000;
                            sbc.ColumnMappings.Clear();
                            sbc.DestinationTableName = tableName;
                            sbc.BatchSize = 100000;

                            for (int i = 0; i < dt.Columns.Count; i++)
                            {
                                sbc.ColumnMappings.Add(i, dt.Columns[i].ColumnName);
                            }

                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                $"{JobLogGUID},{guid.ToString()} - Start BulkCopy Filename: {file.FilePath}"));
                            sbc.WriteToServer(dt);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                $"{JobLogGUID},{guid.ToString()} - End BulkCopy Filename: {file.FilePath}"));
                        } //end loading each source file

                        string spLoadDimension = "Dimension_Stage.dbo." + postProcessing;
                        cmd.CommandText = spLoadDimension;
                        cmd.Parameters.Clear();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadMetadataStageCollection)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            } //end using-trans
        } //end using connection
    }

    #endregion

    #region Sizmek MDX Metadata

    [Obsolete]
    public void LoadMetadataSizmekMdx(Guid guid, Uri sourcePath, int sourceID, int integrationID,
        int countryID, string fileName, string postProcessing)
    {
        var strGruid = guid.ToString().Replace("-", string.Empty);

        var stageMetadataTables =
            SetupService.GetAll<MetadataStageConfiguration>().Where(x => x.SourceID == sourceID);
        var sourceFiles = SetupService.GetAll<SourceFile>().Where(x => x.SourceID == sourceID);

        var stageMetadataConfig =
            stageMetadataTables.Join(sourceFiles, m => m.SourceFileID, s => s.SourceFileID, (m, s) =>
            {
                var metadataTableWithSourceFile = new
                {
                    tableName = m.TableName,
                    sourceFileName = s.SourceFileName,
                    fileDelimiter = s.FileDelimiter,
                    FieldName = m.FieldName,
                    regexMask = s.RegexMask,
                    fieldOrder = m.FieldOrder
                };
                return metadataTableWithSourceFile;
            }).GroupBy(x => x.tableName)
                .Select(sf =>
                {
                    var values = sf.First();
                    var metadataTableWithSourceFileGroup = new
                    {
                        tableName = sf.Key,
                        sourceFileName = values.sourceFileName,
                        fileDelimiter = values.fileDelimiter,
                        metadataColumns = sf.OrderBy(x => x.fieldOrder).ToList(),
                        regexMask = values.regexMask
                    };
                    return metadataTableWithSourceFileGroup;
                });

        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        var cmd = connection.CreateCommand();

                        sbc.BulkCopyTimeout = 12000;
                        foreach (var metadataConfig in stageMetadataConfig)
                        {
                            #region Stream file into DataTable

                            var s3FilePath =
                                RemoteUri.CombineUri(s3Dir.Uri, fileName);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{2} - Start Processing Filename: {3} - {0}, s3Path: {1}",
                                    fileName, s3FilePath, JobLogGUID, metadataConfig.sourceFileName)));

                            var s3FileStream = rac.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{1} - Retrieved S3 File Stream for file: {0}", fileName,
                                    JobLogGUID)));

                            string tableName = "Dimension_Stage.dbo." + metadataConfig.tableName;

                            var fieldSeparator = Regex.Unescape(metadataConfig.fileDelimiter);
                            char delimiter = char.Parse(fieldSeparator);

                            DataTable dt = new DataTable(tableName);
                            using (GZipInputStream inStream = new GZipInputStream(s3FileStream))
                            {
                                TarInputStream tarInStream = new TarInputStream(inStream, Encoding.UTF8);
                                TarEntry tarEntry;

                                while ((tarEntry = tarInStream.GetNextEntry()) != null)
                                {
                                    if (tarEntry.IsDirectory)
                                        continue;
                                    RegexCodec tarRegex = new RegexCodec(metadataConfig.regexMask);

                                    if (tarRegex.TryParse(tarEntry.Name))
                                    {
                                        using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(
                                            new StreamReader(tarInStream), true, delimiter, '\0', '\0',
                                            '\0',
                                            LumenWorks.Framework.IO.Csv.ValueTrimmingOptions.None))
                                        {
                                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                                $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {tarEntry.Name} into DataTable"));
                                            dt.Load(csvReader);
                                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                                $"{JobLogGUID},{guid.ToString()} - Start Streaming CSV file: {tarEntry.Name} into DataTable"));
                                        }
                                        break;
                                    }
                                }
                                tarInStream.Close();
                            }
                            #endregion

                            if (dt.Rows.Count > 0)
                            {
                                for (int i = 0; i < metadataConfig.metadataColumns.Count; i++)
                                {
                                    dt.Columns[i].ColumnName = metadataConfig.metadataColumns[i].FieldName;
                                }

                                dt.Columns.Add(new DataColumn("FileGUID", typeof(string)) { DefaultValue = guid });

                                cmd.Transaction = trans;
                                cmd.Connection = connection;
                                cmd.CommandText = $"Truncate table {tableName}";
                                cmd.CommandType = CommandType.Text;
                                var result = cmd.ExecuteNonQuery();
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - truncated staging table: {tableName}"));

                                sbc.BulkCopyTimeout = 12000;
                                sbc.ColumnMappings.Clear();
                                sbc.DestinationTableName = tableName;
                                sbc.BatchSize = 100000;

                                for (int i = 0; i < dt.Columns.Count; i++)
                                {
                                    sbc.ColumnMappings.Add(i, dt.Columns[i].ColumnName);
                                }

                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - Start BulkCopy Filename: {fileName}"));
                                sbc.WriteToServer(dt);
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - End BulkCopy Filename: {fileName}"));
                            }
                        } //end loading each source file

                        string spLoadDimension = "Dimension_Stage.dbo." + postProcessing;
                        cmd.CommandText = spLoadDimension;
                        cmd.Parameters.Clear();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadMetadataSizmekMdx)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            } //end using-trans
        } //end using connection
    }

    #endregion

    #region RUNDSP

    [Obsolete]
    public void LoadRundspMetadataStageCollection(Guid guid, Uri sourcePath,
        int sourceID, IList<FileCollectionItem> files, string postProcessing)
    {
        if (files == null || files.Count == 0)
        {
            throw new NullOrEmptyFileCollectionException($"{JobLogGUID}, {guid.ToString()} - LoadJsonMetadataStageCollection failed, because no files were passed in");
        }

        var strGruid = guid.ToString().Replace("-", string.Empty);
        var rac = new RemoteAccessClient(sourcePath, GreenhouseAWSCredential);
        var s3Dir = rac.WithDirectory();

        using (SqlConnection connection = new SqlConnection(ConnectionString(true)))
        {
            connection.Open();
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
            {
                try
                {
                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
                    {
                        var cmd = connection.CreateCommand();

                        sbc.BulkCopyTimeout = 12000;
                        foreach (var file in files)
                        {
                            var s3FilePath =
                                RemoteUri.CombineUri(s3Dir.Uri, file.FilePath);
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{2} - Start Processing Filename: {0}, s3Path: {1}",
                                    file.FilePath, s3FilePath, JobLogGUID)));

                            var s3FileStream = rac.WithFile(s3FilePath).Get();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                string.Format("{1} - Retrieved S3 File Stream for file: {0}", file.FilePath,
                                    JobLogGUID)));

                            var allMetadata = Data.DataSource.RUNDSP.RUN1.Run1Service.GetRun1Metadata(sourceID, file, s3FileStream, guid);

                            foreach (var data in allMetadata)
                            {
                                var dt = data.Value;
                                var tableName = "Dimension_Stage.dbo." + data.Key;

                                cmd.Transaction = trans;
                                cmd.Connection = connection;
                                cmd.CommandText = $"Truncate table {tableName}";
                                cmd.CommandType = CommandType.Text;
                                var result = cmd.ExecuteNonQuery();
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - truncated staging table: {tableName}"));

                                sbc.BulkCopyTimeout = 12000;
                                sbc.ColumnMappings.Clear();
                                sbc.DestinationTableName = tableName;
                                sbc.BatchSize = 100000;

                                for (int i = 0; i < dt.Columns.Count; i++)
                                {
                                    sbc.ColumnMappings.Add(i, dt.Columns[i].ColumnName);
                                }

                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - Start BulkCopy Filename: {file.FilePath}"));
                                sbc.WriteToServer(dt);
                                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                                    $"{JobLogGUID},{guid.ToString()} - End BulkCopy Filename: {file.FilePath}"));
                            }
                        } //end loading each source file

                        string spLoadDimension = "Dimension_Stage.dbo." + postProcessing;
                        cmd.CommandText = spLoadDimension;
                        cmd.Parameters.Clear();
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 6000;
                        cmd.ExecuteNonQuery();
                    }

                    trans.Commit();
                }
                catch (HttpClientProviderRequestException exception)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(LoadRundspMetadataStageCollection)} | Exception details : {exception}"));
                    trans.Rollback(strGruid);
                    throw;
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        string.Format("{1} - Start BulkCopy failed: {0}", exc.Message, JobLogGUID)));
                    trans.Rollback(strGruid);
                    throw;
                }
            } //end using-trans
        } //end using connection
    }

    #endregion

    #region Redshift

    public static string GetRedshiftScripts(string bucket, params string[] redShiftScriptPath)
    {
        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Configuration.Settings.Current.AWS.Region, bucket);
        var rac = new RemoteAccessClient(baseUri, GreenhouseAWSCredential);

        var _scriptPath = RemoteUri.CombineUri(baseUri, redShiftScriptPath);
        var scriptFile = rac.WithFile(_scriptPath);

        if (!scriptFile.Exists)
        {
            //Check if etl script exists
            throw new FileNotFoundException($"The Etl script is missing: {_scriptPath.AbsolutePath}.");
        }

        string script = string.Empty;
        using (StreamReader reader = new StreamReader(scriptFile.Get()))
        {
            script = reader.ReadToEnd();
        };
        return script;
    }

    public static string GenerateManifestFile(RedshiftManifest manifest, Uri baseUri, params string[] manifestPath)
    {
        var manifestContent = manifest.GetManifestBody();

        var rac = new RemoteAccessClient(baseUri, GreenhouseAWSCredential);

        var scriptPath = RemoteUri.CombineUri(baseUri, manifestPath);

        var manifestFile = rac.WithFile(scriptPath);

        using (var stream = new MemoryStream())
        {
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true))
            {
                writer.Write(manifestContent);
                writer.Flush();
                stream.Position = 0;
            }

            // Upload the stream to S3
            manifestFile.Put(stream);
        }

        var manifestFilePath = System.Net.WebUtility.UrlDecode($"{scriptPath.OriginalString.TrimStart('/')}");
        return manifestFilePath;
    }

    public static List<FileCollectionItem> CreateManifestFiles(Queue queueItem, List<FileCollectionItem> extractedFiles, Uri outputLocation, Func<DateTime, string> GetDatedPartition)
    {
        List<FileCollectionItem> manifests = new List<FileCollectionItem>();
        var groups = extractedFiles.GroupBy(f => f.SourceFileName).Select(g => new { g.Key, List = g.ToList() });

        foreach (var group in groups)
        {
            var manifest = new RedshiftManifest();

            foreach (var file in group.List)
            {
                manifest.AddEntry(file.FilePath, true);
            }

            var fileName = $"{queueItem.FileGUID}_{group.Key}.manifest";
            var manifestPath = GetManifestRelativePath(queueItem, GetDatedPartition, fileName);

            GenerateManifestFile(manifest, outputLocation, manifestPath);

            manifests.Add(new FileCollectionItem
            {
                SourceFileName = group.Key,
                FilePath = fileName,
                FileSize = group.List.Sum(g => g.FileSize)
            });
        }

        return manifests;
    }

    private static string[] GetManifestRelativePath(Queue queueItem, Func<DateTime, string> GetDatedPartition,
        string fileName)
    {
        return
        [
            queueItem.EntityID.ToLower(),
            GetDatedPartition(queueItem.FileDate),
            fileName.ToLower()
        ];
    }

    public static string GetManifestPath(Queue queueItem, Func<DateTime, string> getDatedPartition,
        string fileName, Uri baseUri)
    {
        string[] relativePath = GetManifestRelativePath(queueItem, getDatedPartition, fileName);
        return RemoteUri.CombineUri(baseUri, relativePath).ToString();
    }

    public static string GenerateManifestFile(RedshiftManifest manifest, string bucketName, params string[] manifestPath)
    {
        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Configuration.Settings.Current.AWS.Region, bucketName);
        return GenerateManifestFile(manifest, baseUri, manifestPath);
    }

    public static string GetScript(IFile scriptFile)
    {
        string script = string.Empty;

        if (scriptFile.Exists)
        {
            using (StreamReader reader = new StreamReader(scriptFile.Get()))
            {
                script = reader.ReadToEnd();
            }
        }
        else
        {
            throw new FileNotFoundException(string.Format("Cannot process data. No script file at expected location: {0}", scriptFile.ToString()));
        }

        return script;
    }

    public static void SerializeRedshiftJson(object entity, IFile transformedFile, Encoding encoding = null)
    {
        JsonSerializerSettings redshiftSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.None };
        redshiftSerializerSettings.Converters.Add(new Greenhouse.Utilities.IO.RedshiftConverter());
        JsonSerializer redshiftSerializer = JsonSerializer.Create(redshiftSerializerSettings);

        using (var stream = new MemoryStream())
        {
            using (var writer = new StreamWriter(stream, encoding, leaveOpen: true))
            {
                // Serialize the object to the stream
                redshiftSerializer.Serialize(writer, entity);
                writer.Flush();
                stream.Position = 0;
            }

            // Upload the stream to S3
            transformedFile.Put(stream);
        }
    }

    #endregion

    public static IEnumerable<DateTime> GenerateDatesBetween(DateTime startDate, DateTime endDate)
    {
        for (var day = startDate.Date; day.Date <= endDate.Date; day = day.AddDays(1))
            yield return day;
    }

    public static T DeserializeType<T>(string json, JsonSerializerSettings settings = null)
    {
        settings = settings ?? new JsonSerializerSettings()
        {
            MissingMemberHandling = MissingMemberHandling.Ignore,
            NullValueHandling = NullValueHandling.Ignore
        };

        return JsonConvert.DeserializeObject<T>(json.ToString(), settings);
    }

    #region Innovid-API
    public void StageInnovidFiles(IEnumerable<FileCollectionItem> queueFiles, Uri sourceURI, Uri destURI)
    {
        var rac = new RemoteAccessClient(sourceURI, GreenhouseAWSCredential);
        var files = queueFiles.ToList();
        if (files.All(x => x.FilePath.EndsWith($".{Constants.CompressionType.ZIP}", true, null)))
        {
            files.ForEach(file =>
            {
                var srcFile = RemoteUri.CombineUri(sourceURI, file.FilePath);
                var s3FileStream = rac.WithFile(srcFile).Get();

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Retrieved S3 File Stream for file: {0}", file.FilePath, JobLogGUID)));

                using (var fileStream = new ZipInputStream(s3FileStream))
                {
                    ZipEntry zipEntry = null;
                    while ((zipEntry = fileStream.GetNextEntry()) != null)
                    {
                        if (!zipEntry.IsFile)
                            continue; //skip directories

                        string destFileName = zipEntry.Name;
                        var destFile = RemoteUri.CombineUri(destURI, destFileName);

                        var s3DestFile = rac.WithFile(destFile);
                        if (s3DestFile.Exists)
                        {
                            s3DestFile.Delete();
                        }

                        using (var entryStream = new MemoryStream())
                        {
                            fileStream.CopyTo(entryStream);
                            entryStream.Position = 0;
                            s3DestFile.Put(entryStream);
                        }
                    }
                }
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Finished File Stream for file: {0}", file.FilePath, JobLogGUID)));
            });//end foreach file
        }
        else
        {
            throw new FileFormatException("Innovid file is not in expected zip archive format; unable to stage files for processing.");
        }
    }
    #endregion
    /// <summary>
    /// Copy a new S3 destination CSV file from the source CSV by stripping out any
    /// lines before the [Report Fields] line and after the last [Grand Total] line.
    /// The new CSV file is then used to load data into staging tables.
    /// It will be the caller's responsibilty to clean up temp files in localDirectory
    /// </summary>
    /// <param name="sourceURI">The original file name with path.</param>
    /// <param name="destURI">The destination path.</param>
    /// <param name="sanitizedHeader">Remove special character from header.</param>
    /// <param name="endOfHeaderText">Only include lines after header.</param>
    /// <param name="startOfFooterText">Only include lines before footer.</param>
    /// <param name="localDirectory">Local directory.</param>
    /// <param name="fileGUID">FileGUID.</param>
    /// <param name="transferUtility">Multi-part transfer utility in base framework.</param>
    /// <param name="compressFile">Mark true if stage files should be gzip (default is false).</param>
    public void CopyRawToStageCsv(Uri sourceURI, Uri destURI, bool sanitizedHeader, string endOfHeaderText, string startOfFooterText, string localDirectory, System.Guid fileGUID, Func<string, Amazon.S3.Transfer.TransferUtility> transferUtility, bool compressFile = false)
    {
        var rac = new RemoteAccessClient(sourceURI, GreenhouseAWSCredential);
        bool writeToFile = false;
        bool isHeaderLine = false;

        //Get source stream
        var s3FileSrc = rac.WithFile(sourceURI);
        var s3FileSrcStream = GetS3FileStream(0, s3FileSrc, localDirectory, fileGUID);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Retrieved S3 File Stream for file: {sourceURI.AbsolutePath}"));

        //Get destination stream            
        string tmpStageFile = $"stage_{s3FileSrc.Name}";
        var localSrcFile = CreateLocalFile(tmpStageFile, localDirectory);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Staging file: {sourceURI.AbsolutePath}, to: {localSrcFile.FullName}"));

        if (string.IsNullOrEmpty(endOfHeaderText))
        {
            //if endOfHeaderText is not specified, the file has no summary at the beginning and starts with the header
            writeToFile = true;
            isHeaderLine = true;
        }

        //Copy source stream to local destination stream line by line
        using (StreamReader sr = new StreamReader(s3FileSrcStream))
        {
            using (StreamWriter file = new StreamWriter(@localSrcFile.FullName))
            {
                while (!sr.EndOfStream)
                {
                    string currentLine = sr.ReadLine();
                    if (isHeaderLine)
                    {
                        isHeaderLine = false;
                        if (sanitizedHeader)
                        {
                            currentLine = RemoveSpecialCharacter(currentLine); // Remove special characters from header
                        }
                    }

                    if (writeToFile && currentLine.Length == 0) //empty line in the data means that we reach the end of the data
                    {
                        break;
                    }

                    if (!string.IsNullOrEmpty(endOfHeaderText) && currentLine.StartsWith(endOfHeaderText, StringComparison.InvariantCultureIgnoreCase))
                    {
                        writeToFile = true;
                        isHeaderLine = true;
                        continue;
                    }
                    else if (!string.IsNullOrEmpty(startOfFooterText) && currentLine.StartsWith(startOfFooterText, StringComparison.InvariantCultureIgnoreCase))
                    {
                        break; //the string startOfFooterText marked the beginning of the footer that is not kept
                    }

                    if (writeToFile)
                    {
                        file.WriteLine(currentLine);
                    }
                }
            }
        }

        //remove raw file that is created temporarily for files over 1gb
        var localRawFile = $"{localDirectory.TrimEnd(Constants.BACKWARD_SLASH_ARRAY)}\\{s3FileSrc.Name}";
        if (System.IO.File.Exists(localRawFile))
        {
            System.IO.File.Delete(localRawFile);
        }

        if (compressFile)
        {
            var stageFile = new FileInfo(@localSrcFile.FullName);

            using (FileStream originalFileStream = stageFile.OpenRead())
            {
                if (stageFile.Extension != ".gz")
                {
                    using (FileStream compressedFileStream = File.Create(stageFile.FullName + ".gz"))
                    {
                        using (GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress))
                        {
                            originalFileStream.CopyTo(compressionStream);
                        }
                    }
                }
            }
        }

        /* upload local staged file to stage folder on s3*/
        var destFile = rac.WithFile(destURI) as S3File;
        var tu = transferUtility(destFile.S3Uri.Region.SystemName);
        //append gzip extension if file needs to be compressed
        var localSrcFilePath = compressFile ? $"{@localSrcFile.FullName}.gz" : @localSrcFile.FullName;
        var destKey = compressFile ? $"{destFile.S3Uri.Key}.gz" : destFile.S3Uri.Key;
        tu.Upload(localSrcFilePath, destFile.S3Uri.Bucket, destKey);
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Completed File Stream for file: {sourceURI.AbsolutePath}, to: {destFile.S3Uri.Bucket}/{destKey}"));
    }

    /// <summary>
    /// Breaks stage files down to 512MB (default) raw uncompressed text. Configurable in Lookup "". gzip staged file before upload to s3.
    /// </summary>
    /// <param name="sourceURI"></param>
    /// <param name="destURI"></param>
    /// <param name="sanitizedHeader"></param>
    /// <param name="endOfHeaderText"></param>
    /// <param name="startOfFooterText"></param>
    /// <param name="localDirectory"></param>
    /// <param name="fileGUID"></param>
    /// <param name="transferUtility"></param>
    /// <returns>The header from the staged file.</returns>
    public string CopyRawToStageCsvGzip(Uri sourceURI, Uri destURI, bool sanitizedHeader, string endOfHeaderText, string startOfFooterText, string localDirectory, System.Guid fileGUID, Func<string, Amazon.S3.Transfer.TransferUtility> transferUtility)
    {
        var rac = new RemoteAccessClient(sourceURI, GreenhouseAWSCredential);
        string headerLine = string.Empty;
        Stream s3FileSrcStream = null;

        try
        {
            //Get source stream
            var s3FileSrc = rac.WithFile(sourceURI);

            s3FileSrcStream = GetS3FileStream(0, s3FileSrc, localDirectory, fileGUID);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                $"{JobLogGUID} - Retrieved S3 File Stream for file: {sourceURI.AbsolutePath}"));

            headerLine = CopyLocalToStageCsvGzip(s3FileSrc, s3FileSrcStream, destURI, sanitizedHeader, endOfHeaderText,
                startOfFooterText,
                localDirectory, fileGUID, transferUtility);

            //remove raw file that is created temporarily for files over 1gb
            var localRawFile = $"{localDirectory.TrimEnd(Constants.BACKWARD_SLASH_ARRAY)}\\{s3FileSrc.Name}";
            if (System.IO.File.Exists(localRawFile))
            {
                System.IO.File.Delete(localRawFile);
            }
        }
        catch (HttpClientProviderRequestException exception)
        {
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"ETLProvider | {nameof(CopyRawToStageCsvGzip)} | Exception details : {exception}"));
            throw;
        }
        catch
        {
            throw;
        }
        finally
        {
            if (s3FileSrcStream != null) s3FileSrcStream.Dispose();
        }

        return headerLine;
    }

    public string CopyLocalToStageCsvGzip(IFile localFile, Stream localFileStream, Uri destURI, bool sanitizedHeader, string endOfHeaderText, string startOfFooterText, string localDirectory, System.Guid fileGUID, Func<string, Amazon.S3.Transfer.TransferUtility> transferUtility, Encoding encoding = null)
    {
        var rac = new RemoteAccessClient(destURI, GreenhouseAWSCredential);
        bool writeToFile = false;
        bool isHeaderLine = false;
        string headerLine = string.Empty;
        GZipStream gzipStream = null;
        FileStream localgzipStream = null;

        try
        {
            //Get destination stream     
            int fileCounter = 1;
            string[] srcFileName = localFile.Name.Split(Constants.DOT_ARRAY, 2);
            string tmpStageFile = $"{srcFileName[0]}_{fileCounter++}.{srcFileName[1]}.gz";
            var localgzipFile = CreateLocalFile(tmpStageFile, localDirectory);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Staging file: {localFile.FullName}, to: {localgzipFile.FullName}"));

            localgzipStream = new FileStream(localgzipFile.FullName, FileMode.Create, FileAccess.Write);
            gzipStream = new GZipStream(localgzipStream, CompressionMode.Compress);

            //break files into 512MB files based on raw text. 
            if (!int.TryParse(SetupService.GetById<Lookup>(Constants.MAX_STAGE_FILE_SIZE)?.Value, out int fileSizeLimit))
            {
                fileSizeLimit = 512;
            };
            fileSizeLimit *= 1024 * 1024;
            long currBytes = 0L;

            if (string.IsNullOrEmpty(endOfHeaderText))
            {
                //if endOfHeaderText is not specified, the file has no summary at the beginning and starts with the header
                writeToFile = true;
                isHeaderLine = true;
            }

            //Copy source stream to local destination stream line by line
            using (StreamReader sr = new StreamReader(localFileStream))
            {
                StreamWriter file = encoding == null ? new StreamWriter(gzipStream) : new StreamWriter(gzipStream, encoding);

                while (!sr.EndOfStream)
                {
                    string currentLine = sr.ReadLine();
                    if (isHeaderLine)
                    {
                        isHeaderLine = false;
                        if (sanitizedHeader)
                        {
                            currentLine = RemoveSpecialCharacter(currentLine); // Remove special characters from header
                        }
                        headerLine = currentLine;
                    }
                    else
                    {
                        currBytes += System.Text.ASCIIEncoding.ASCII.GetByteCount(currentLine);
                    }

                    if (writeToFile && currentLine.Length == 0) //empty line in the data means that we reach the end of the data
                    {
                        break;
                    }

                    if (!string.IsNullOrEmpty(endOfHeaderText) && currentLine.StartsWith(endOfHeaderText, StringComparison.InvariantCultureIgnoreCase))
                    {
                        writeToFile = true;
                        isHeaderLine = true;
                        continue;
                    }
                    else if (!string.IsNullOrEmpty(startOfFooterText) && currentLine.StartsWith(startOfFooterText, StringComparison.InvariantCultureIgnoreCase))
                    {
                        break; //the string startOfFooterText marked the beginning of the footer that is not kept
                    }

                    if (writeToFile)
                    {
                        file.WriteLine(currentLine);
                    }

                    //start new file if file size limit has been met and there's more data
                    if (currBytes >= fileSizeLimit && sr.Peek() > -1)
                    {
                        file.Close();
                        localgzipStream.Close();
                        gzipStream.Close();
                        currBytes = 0;

                        //write headers to every file
                        tmpStageFile = $"{srcFileName[0]}_{fileCounter++}.{srcFileName[1]}.gz";
                        localgzipFile = CreateLocalFile(tmpStageFile, localDirectory);

                        localgzipStream = new FileStream(localgzipFile.FullName, FileMode.Create, FileAccess.Write);

                        gzipStream = new GZipStream(localgzipStream, CompressionMode.Compress);

                        file = new StreamWriter(gzipStream);// (@localSrcFile.FullName);
                        file.WriteLine(headerLine);
                    }
                }//end while

                // clean up
                file.Close();
                file.Dispose();
                localgzipStream.Close();
                localgzipStream.Dispose();
                gzipStream.Close();
                gzipStream.Dispose();
            }

            var files = localgzipFile.Directory.GetFiles()?.Where(file => file.Name.Contains(srcFileName[0]) & file.Extension == ".gz");
            var destFile = rac.WithFile(destURI) as S3File;
            var tu = transferUtility(destFile.S3Uri.Region.SystemName);

            //remove filename from dest filepath (ie.  /stage/source/date/file_xyz.ext => /stage/source/date)
            var paths = destFile.S3Uri.Key.Split(Constants.FORWARD_SLASH_ARRAY, StringSplitOptions.RemoveEmptyEntries).ToList();
            var lastPathItem = paths.Last();

            if (sourceArray.Any(x => lastPathItem.Trim().EndsWith(x)))
            {
                paths.Remove(lastPathItem);
            }
            var stagePath = string.Join("/", paths);

            foreach (var file in files)
            {
                /* upload local staged file to stage folder on s3*/
                var localSrcFilePath = file.FullName;
                var destKey = $"{stagePath}/{file.Name}";

                tu.UploadAsync(localSrcFilePath, destFile.S3Uri.Bucket, destKey).GetAwaiter().GetResult();
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Completed File Stream for file: {file.FullName}, to: {destFile.S3Uri.Bucket}/{destKey}"));
            }
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            localgzipStream?.Dispose();
            localgzipStream = null;

            gzipStream?.Dispose();
            gzipStream = null;
        }

        return SanitizeColumns(headerLine);
    }

    /// <summary>
    /// Remove special characters from string
    /// space ( ) - / :
    /// </summary>
    /// <param name="currentLine"></param>
    /// <returns>String with special characters removed</returns>
    private static string RemoveSpecialCharacter(string currentLine)
    {
        string pattern = @"[\s\(\)\-\/\:]+";
        Regex regex = new Regex(pattern);
        currentLine = regex.Replace(currentLine, string.Empty);
        return currentLine;
    }

    private static string SanitizeColumnName(string columnName)
    {
        return SantizeColumnNameRegex().Replace(columnName, "");
    }

    #region Generic DSP Aggregate
    /// <summary>
    /// Get the header column count from source file
    /// </summary>
    /// <param name="sourceURI">The original file name with path.</param>
    /// <returns>File header columns count</returns>
    public int GetHeaderColCount(Uri sourceURI)
    {
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Retrieved S3 File Stream for file: {sourceURI.AbsolutePath} to get header count"));

        var rac = new RemoteAccessClient(sourceURI, GreenhouseAWSCredential);
        var headerColCount = 0;

        //Get source stream
        var s3FileSrcStream = rac.WithFile(sourceURI).Get();

        if (sourceURI.AbsolutePath.EndsWith(".zip", StringComparison.InvariantCultureIgnoreCase))
        {
            List<int> allFileHeaderCount = new List<int>();
            using (ZipArchive archive = new ZipArchive(s3FileSrcStream))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(Constants.FORWARD_SLASH) && string.IsNullOrEmpty(entry.Name)) continue;
                    using (StreamReader sr = new StreamReader(entry.Open()))
                    {
                        var headerLine = sr.ReadLine();
                        var headerLineSeparator = entry.Name.Contains(".csv") ? $"," : $"\t";
                        allFileHeaderCount.Add(string.IsNullOrEmpty(headerLine) ?
                        0 : headerLine.Split(headerLineSeparator.ToCharArray()).Length);
                    }
                }
            }

            if (allFileHeaderCount.Distinct()?.Count() != 1)
            {
                throw new ETLProviderException($"Error: {sourceURI.AbsolutePath} - " +
                    $"All compressed files do not have the same number of header columns. " +
                    $"All compressed files require the same number of columns for the ETL script to work.");
            }

            headerColCount = allFileHeaderCount.FirstOrDefault();
        }
        else
        {
            //Copy source stream to destination stream line by line
            using (StreamReader sr = new StreamReader(s3FileSrcStream))
            {
                var headerLine = string.Empty;
                if (sourceURI.AbsolutePath.EndsWith(".gz", StringComparison.InvariantCultureIgnoreCase))
                {
                    //Decompress before getting header count
                    using (var zipStream = new GZipStream(sr.BaseStream, CompressionMode.Decompress))
                    {
                        using (var unzip = new StreamReader(zipStream))
                        {
                            headerLine = unzip.ReadLine();
                        }
                    }
                }
                else
                {
                    headerLine = sr.ReadLine();
                }
                //Get header count
                var headerLineSeparator = sourceURI.AbsolutePath.Contains(".csv", StringComparison.InvariantCultureIgnoreCase) ? $"," : $"\t";
                headerColCount = string.IsNullOrEmpty(headerLine) ? 0 : headerLine.Split(headerLineSeparator.ToCharArray()).Length;

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Completed S3 File Stream for file: {sourceURI.AbsolutePath} to get header count"));
            }
        }

        if (headerColCount < 1)
        {
            throw new ETLProviderException($"Error: {sourceURI.AbsolutePath} - " +
                $"Unable to retrieved the header column count");
        }
        return headerColCount;
    }

    /// <summary>
    /// Gets all file from destURI that matches the fileGUID
    /// </summary>
    /// <param name="destURI">The file[s] directory</param>
    /// <returns>Return file[s] from destURI</returns>
    public static IEnumerable<IFile> GetStagedFilesWithGuid(Uri destURI, string fileGUID)
    {
        var rac = new RemoteAccessClient(destURI, GreenhouseAWSCredential);

        var dir = rac.WithDirectory();
        if (!dir.Exists) throw new DirectoryNotFoundException($"The S3 directory is missing: {destURI.AbsolutePath}. FileGUID: {fileGUID}");

        var files = dir.GetFiles().Where(f => f.Name.Contains(fileGUID));

        if (!files?.Any(f => f.Name.Contains(fileGUID)) == true) throw new FileNotFoundException($"The S3 files are missing from: {destURI.AbsolutePath}. FileGUID: {fileGUID}");

        return files;
    }

    /// <summary>
    /// Checks if the file is empty in the destURI and returns it
    /// </summary>
    /// <param name="destURI">The file name</param>
    /// <returns>Return empty file in the destURI</returns>
    public static IFile IsEmptyFile(Uri destURI)
    {
        var rac = new RemoteAccessClient(destURI, GreenhouseAWSCredential);

        //Get source stream
        var file = rac.WithFile(destURI);
        if (!file.Exists)
            throw new FileNotFoundException($"The S3 files are missing from: {destURI.AbsolutePath}.");

        return file.Length < 1 ? file : null;
    }

    /// <summary>
    /// Get etl script name, to run against redshift, based on [Lookup] table value setup
    /// </summary>
    /// <param name="sourceName">source name</param>
    /// <param name="headerColCount">file header column count</param>
    /// <param name="etlScriptVersionCollection">etl script version</param>
    /// <returns>etl script name</returns>
    public string GetEtlScriptName(string sourceName, int headerColCount, IEnumerable<Lookup> etlScriptVersionCollection)
    {
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Start Getting ETL script for Source name: {sourceName}"));

        var etlScriptVersionName = string.Empty;
        try
        {
            //Get etl script name
            etlScriptVersionName = etlScriptVersionCollection.First(sv => Convert.ToInt32(sv.Value) == headerColCount).Name;

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"{JobLogGUID} - Completed Getting ETL script for Source name: {sourceName}"));

            return etlScriptVersionName.ToLower();
        }
        catch (Exception)
        {
            //Check if version setup value exists
            if (string.IsNullOrEmpty(etlScriptVersionName)) { throw new ETLProviderException($"The Etl script version setup for Source: {sourceName} is missing for Value: {headerColCount} in the [Lookup] table."); }
            throw;
        }
    }

    /// <summary>
    /// Unzip and Stages files
    /// </summary>
    /// <param name="srcRemoteUri">Compressed file Uri</param>
    /// <param name="destRemoteFilePath">Destination file path</param>
    /// <param name="fileGUID"></param>
    /// <param name="localPath">Extract source files to this [local file system path Uri] before staging</param>
    /// <param name="sourceSetting">Greenhouse.Data.Model.Setup.AggregateProcessingSettings</param>
    /// <returns></returns>
    public List<FileCollectionItem> UnzipAndStageFiles(Uri srcRemoteUri, string destRemoteFilePath, string fileGUID,
        Uri localPath, Greenhouse.Data.Model.Setup.AggregateProcessingSettings sourceSetting, string sourceFileName,
        string filePassword)
    {
        var rac = new RemoteAccessClient(srcRemoteUri, GreenhouseAWSCredential);
        var srcRemoteFile = rac.WithFile();
        var extractedFileCollection = new List<IFile>();
        var fileCollection = new List<FileCollectionItem>();
        var fileUtils = new FileNameUtils();

        var localFileUri = RemoteUri.CombineUri(localPath, srcRemoteFile.Name);
        var localFile = new FileSystemFile(localFileUri);

        bool cleanup = false;
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
            cleanup = true;
        }

        try
        {
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"FileGUID: {fileGUID}. Start Copying file to local file system: {localFileUri.AbsolutePath}"));
            srcRemoteFile.CopyTo(localFile, true);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"FileGUID: {fileGUID}. Completed Copying file to local file system: {localFileUri.AbsolutePath}"));

            var emptyFileCounter = 0;

            //append SourceFile to FileGUID 
            string guidFileName = fileGUID + (sourceSetting.AddSourceFileNameToFileToName ? $"_{sourceFileName}" : string.Empty);

            if (srcRemoteFile.Name.EndsWith(".tar.gz"))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Start file(s) extraction: {localFile.FullName} to local file system {localFileUri.AbsolutePath}"));

                using (var srcLocalStream = new GZipInputStream(localFile.Get()))
                {
                    TarInputStream tarInStream = new TarInputStream(srcLocalStream, Encoding.UTF8);
                    TarEntry tarEntry;

                    while ((tarEntry = tarInStream.GetNextEntry()) != null)
                    {
                        if (tarEntry.IsDirectory)
                            continue;

                        if (!string.IsNullOrEmpty(sourceSetting.FilesToSkipRegex))
                        {
                            Match m = Regex.Match(tarEntry.Name, sourceSetting.FilesToSkipRegex,
                                RegexOptions.IgnoreCase);
                            if (m.Success)
                                continue;
                        }

                        var destLocalUri = fileUtils.AppendFileGUIDToFilename(localPath.AbsolutePath, tarEntry.Name, guidFileName, $"_");
                        var destLocalFile = rac.WithFile(destLocalUri);

                        using (Stream destLocalStream = destLocalFile.Create())
                        {
                            byte[] buffer = new byte[4096];
                            int read;
                            while ((read = tarInStream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                destLocalStream.Write(buffer, 0, read);
                            }
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Extracted: {0} ", destLocalFile.FullName)));
                        extractedFileCollection.Add(destLocalFile);

                        if (destLocalFile.Length < 1)
                        {
                            emptyFileCounter += 1;
                        }
                    }

                    tarInStream.Close();
                }

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Completed file(s) extraction: {srcRemoteFile.FullName} to local file system {localFileUri.AbsolutePath}"));
            }
            else if (srcRemoteFile.Name.EndsWith(".gz"))
            {
                string currentFileName = localFile.Name;
                string exportFileName = currentFileName.Remove(currentFileName.Length - localFile.Extension.Length);

                if (!string.IsNullOrEmpty(sourceSetting.FilesToSkipRegex))
                {
                    Match m = Regex.Match(exportFileName, sourceSetting.FilesToSkipRegex,
                        RegexOptions.IgnoreCase);
                    if (m.Success)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Not extracting file: {localFile.FullName} - File name is matching file to skip"));

                        return new List<FileCollectionItem>();
                    }
                }

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Start file(s) extraction: {localFile.FullName} to local file system {localFileUri.AbsolutePath}"));

                using (Stream sourceFile = localFile.Get())
                {
                    var destLocalUri = fileUtils.AppendFileGUIDToFilename(localPath.AbsolutePath, exportFileName, guidFileName, $"_");
                    var destLocalFile = rac.WithFile(destLocalUri);

                    using (Stream destLocalStream = destLocalFile.Create())
                    {
                        using (GZipStream decompressionStream = new GZipStream(sourceFile, CompressionMode.Decompress))
                        {
                            decompressionStream.CopyTo(destLocalStream);
                        }
                    }

                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Extracted: {0} ", destLocalFile.FullName)));
                    extractedFileCollection.Add(destLocalFile);

                    if (destLocalFile.Length < 1)
                    {
                        emptyFileCounter += 1;
                    }
                }

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Completed file(s) extraction: {srcRemoteFile.FullName} to local file system {localFileUri.AbsolutePath}"));
            }
            else if (srcRemoteFile.Name.EndsWith(".zip"))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Start file(s) extraction: {localFile.FullName} to local file system {localFileUri.AbsolutePath}"));

                using (var zipFile = new ICSharpCode.SharpZipLib.Zip.ZipFile(localFile.Get()))
                {
                    if (!string.IsNullOrEmpty(filePassword))
                        zipFile.Password = filePassword;

                    foreach (ZipEntry localZipEntry in zipFile)
                    {
                        if (!localZipEntry.IsFile)
                            continue; //skip directories

                        var fileName = localZipEntry.Name.Split('/').Last();

                        if (!string.IsNullOrEmpty(sourceSetting.FilesToSkipRegex))
                        {
                            Match m = Regex.Match(fileName, sourceSetting.FilesToSkipRegex, RegexOptions.IgnoreCase);
                            if (m.Success)
                                continue;
                        }

                        var destLocalUri = fileUtils.AppendFileGUIDToFilename(localPath.AbsolutePath, fileName, guidFileName, $"_");
                        var destLocalFile = rac.WithFile(destLocalUri);

                        Stream zipStream = zipFile.GetInputStream(localZipEntry);

                        using (var destLocalStream = destLocalFile.Create())
                        {
                            zipStream.CopyTo(destLocalStream, 4096);
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Extracted: {0} ", destLocalFile.FullName)));
                        extractedFileCollection.Add(destLocalFile);

                        if (destLocalFile.Length < 1)
                        {
                            emptyFileCounter += 1;
                        }
                    }

                    zipFile.Close();
                }

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Completed file(s) extraction: {srcRemoteFile.FullName} to local file system {localFileUri.AbsolutePath}"));
            }

            // any excel file needs to be converted to csv
            for (var i = 0; i < extractedFileCollection.Count; i++)
            {
                var file = extractedFileCollection[i];
                if (!file.Extension.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase)) continue;

                string newURI = string.Concat(file.Uri.AbsolutePath.AsSpan(0, file.Uri.AbsolutePath.Length - 4), "csv");
                var destLocalFile = rac.WithFile(new Uri(newURI));

                var fileSettings = sourceSetting.CompressedFilesSettings?.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.Match(fileUtils.GetOriginalName(file.Name)).Success);

                FileConverterHelper.ConvertExcelToCSV(file, destLocalFile, fileSettings?.LinesToSkip ?? 0, fileSettings?.AddRowNumber ?? false);

                // deleting the xlsx file and replacing it with the csv in the list of extracted files
                file.Delete();
                extractedFileCollection[i] = destLocalFile;
            }

            //before we stage files, we check if any of the extracted files are empty
            if (emptyFileCounter > 0 && !sourceSetting.AllowEmptyFiles)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    $"{JobLogGUID} - Deleting files from local import directory; fileGUID: {fileGUID}. Path {localFile.Directory.Uri.AbsolutePath}."));
                localFile.Directory.Delete(true);

                var emptyFileNames = extractedFileCollection.Where(f => f.Length < 1).Select(x => x.Name).Aggregate((current, next) => current + "|" + next);

                throw new ETLProviderException($"{JobLogGUID} - FileGuid: {fileGUID}; Skipping empty file(s): [{emptyFileNames}]");
            }

            //copy files to s3 stage
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"Start staging extracted files from local file system: {localPath} to {destRemoteFilePath}."));

            // DIAT-15253: files that are currently UTF8 and are not compressed, will be found in the boolean check "encodeCompressAllFiles"
            // the following logic is compressing all the files if any is non utf8 
            // all or nothing: if any file need to be compressed they will all be compressed

            var extractedFiles = new List<ExtractedFileInfo>();

            foreach (var file in extractedFileCollection)
            {
                var compressionType = UtilsIO.GetCompressionType(Path.GetExtension(file.Name));

                Encoding currentEncoding;
                using (var stream = file.Get())
                {
                    currentEncoding = UtilsIO.GetEncoding(compressionType, stream);
                }

                extractedFiles.Add(new ExtractedFileInfo
                {
                    File = file,
                    CompressionType = compressionType,
                    Encoding = currentEncoding
                });
            }

            // all or nothing: if any file need to be encoded and compressed - they all should be encoded and compressed
            bool encodeCompressAllFiles = extractedFiles.Any(f => f.Encoding.BodyName != Encoding.UTF8.BodyName || f.CompressionType != Constants.CompressionType.GZIP);

            foreach (var fileInfo in extractedFiles)
            {
                var stageFile = fileInfo.File;
                var stageFileName = stageFile.Name;
                //check file encoding
                if (encodeCompressAllFiles)
                {
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                        $"Current file {stageFileName} encoding is {fileInfo.Encoding.EncodingName} and will be converted to {Encoding.UTF8.EncodingName}; FileGuid: {fileGUID}"));
                    //we exclude the byte-order-mark because Redshift copy command fails for JSON files
                    var utf8WithoutBom = new UTF8Encoding(false);

                    using (var sourceStream = stageFile.Get())
                    {
                        var absolutePath = UtilsIO.ChangeFileEncoding(stageFile.Name, localPath, utf8WithoutBom,
                            fileInfo.Encoding, fileInfo.CompressionType, sourceStream, true);
                        if (fileInfo.CompressionType != Constants.CompressionType.GZIP)
                        {
                            stageFile = rac.WithFile(new Uri($"{absolutePath}"));
                            stageFileName = $"{stageFileName}.gz";
                        }
                    }
                }

                var destRemoteFile = rac.WithFile(new Uri($"{destRemoteFilePath}/{stageFileName}"));
                stageFile.CopyTo(destRemoteFile, true);
                fileCollection.Add(new FileCollectionItem()
                {
                    FilePath = destRemoteFile.Name,
                    FileSize = destRemoteFile.Length,
                    SourceFileName = sourceFileName
                });
            }

            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"Completed staging extracted files from local file system: {localPath} to {destRemoteFilePath}."));
        }
        finally
        {
            if (cleanup)
            {
                localFile.Directory.Delete(true);
            }
            else
            {
                foreach (var file in extractedFileCollection)
                {
                    file.Delete();
                }
            }
        }

        return fileCollection;
    }

    private sealed class ExtractedFileInfo
    {
        public IFile File { get; set; }
        public Constants.CompressionType CompressionType { get; set; }
        public Encoding Encoding { get; set; }
    }

    private sealed class FileNameUtils
    {
        private Dictionary<string, string> OriginalName { get; set; } = new Dictionary<string, string>();
        /// <summary>
        ///  Append the fileGUID to the filename and return the URI
        /// </summary>
        /// <param name="filePath">The file path</param>
        /// <param name="fileName">The name of the file</param>
        /// <param name="fileGUID">The file GUID</param>
        /// <param name="separator">The filename will be split by this character</param>
        /// <param name="index">The position where the file GUID will be appended</param>
        /// <returns>The URI with the filename with the file GUID appended</returns>
        public Uri AppendFileGUIDToFilename(string filePath, string fileName, string fileGUID, string separator)
        {
            string newFileName = $"{fileGUID}{separator}{fileName}";

            OriginalName[newFileName] = fileName;

            return new Uri($"{filePath}/{newFileName}");
        }

        public string GetOriginalName(string updatedName)
        {
            return OriginalName.GetValueOrDefault(updatedName);
        }
    }

    public static string GetHeaderLineFromFile(Uri filePath, string filename, string fileDelimiter = null)
    {
        string header = string.Empty;
        List<string> sanitizedColumns = new List<string>();

        var remoteUri = RemoteUri.CombineUri(filePath, filename);
        var rac = new RemoteAccessClient(remoteUri, GreenhouseAWSCredential);
        var remoteFile = rac.WithFile();
        if (!remoteFile.Exists) return null;

        using (var sr = new StreamReader(remoteFile.Get()))
        {
            header = sr.ReadLine();
        }

        if (header == null) return null;

        string getColumnsPattern;
        if (!string.IsNullOrEmpty(fileDelimiter))
        {
            getColumnsPattern = $"(?<=^|{fileDelimiter})(\"(?:[^\"]|\"\")*\"|[^{fileDelimiter}]*)";
        }
        else if (filename.EndsWith(".csv"))
        {
            getColumnsPattern = "(?<=^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)";
        }
        else if (filename.EndsWith(".tsv"))
        {
            getColumnsPattern = "(?<=^|\t)(\"(?:[^\"]|\"\")*\"|[^\t]*)";
        }
        else
        {
            return null;
        }

        Regex getColumnsRegex = new Regex(getColumnsPattern);

        var matches = getColumnsRegex.Matches(header);
        int numberOfColumnsInLine = matches.Count;

        if (numberOfColumnsInLine <= 1) return null;

        foreach (Match match in matches)
        {
            sanitizedColumns.Add(SanitizeColumnName(match.Value));
        }

        //wrapping each column header with double quotes per Redshift standard
        return string.Join(",", sanitizedColumns.Select(column => "\"" + column + "\""));
    }

    //TODO: this is redundant code from GetHeaderLineFromFile and should be consolidated in future sprint
    public static string GetHeaderLineFromGzip(Uri filePath, string filename, string fileDelimiter = null)
    {
        string header = string.Empty;
        List<string> sanitizedColumns = new List<string>();

        var remoteUri = RemoteUri.CombineUri(filePath, filename);
        var rac = new RemoteAccessClient(remoteUri, GreenhouseAWSCredential);
        var remoteFile = rac.WithFile();
        if (!remoteFile.Exists) return null;

        using (var sr = new StreamReader(remoteFile.Get()))
        {
            if (remoteUri.AbsolutePath.EndsWith(".gz", StringComparison.InvariantCultureIgnoreCase))
            {
                using (var zipStream = new GZipStream(sr.BaseStream, CompressionMode.Decompress))
                {
                    using (var unzip = new StreamReader(zipStream))
                    {
                        header = unzip.ReadLine();
                    }
                }
            }
            else
            {
                header = sr.ReadLine();
            }
        }

        string getColumnsPattern;
        if (!string.IsNullOrEmpty(fileDelimiter))
        {
            getColumnsPattern = $"(?<=^|{fileDelimiter})(\"(?:[^\"]|\"\")*\"|[^{fileDelimiter}]*)";
        }
        else if (filename.EndsWith(".csv.gz"))
        {
            getColumnsPattern = "(?<=^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)";
        }
        else if (filename.EndsWith(".tsv.gz"))
        {
            getColumnsPattern = "(?<=^|\t)(\"(?:[^\"]|\"\")*\"|[^\t]*)";
        }
        else
        {
            return null;
        }

        //wrapping each column header with double quotes per Redshift standard
        return SanitizeColumns(header, getColumnsPattern);
    }
    private static string SanitizeColumns(string headers, string regexPattern = "(?<=^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)")
    {
        if (UtilsText.IsNullOrEmptyString(headers))
            return string.Empty;

        List<string> sanitizedColumns = new List<string>();

        Regex getColumnsRegex = new Regex(regexPattern);

        var matches = getColumnsRegex.Matches(headers);
        int numberOfColumnsInLine = matches.Count;

        if (numberOfColumnsInLine <= 1) return null;

        foreach (Match match in matches)
        {
            sanitizedColumns.Add(SanitizeColumnName(match.Value));
        }

        //wrapping each column header with double quotes per Redshift standard
        return string.Join(",", sanitizedColumns.Select(column => "\"" + column + "\""));
    }

    public static string[] GetApprovedCompressionTypes()
    {
        var lookupCompressionTypes = SetupService.GetById<Lookup>(Constants.AGG_DATALOAD_JOB_COMPRESSION_TYPES).Value;
        var compressionTypes = lookupCompressionTypes.Split(',').Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x)).ToArray();
        return compressionTypes;
    }

    public static string GetCompressionOption(IFileItem queueItem, string stagedfileCollectionJson)
    {
        var extension = string.Empty;
        var etl = new ETLProvider();
        var compressionTypes = GetApprovedCompressionTypes();

        if (string.IsNullOrEmpty(stagedfileCollectionJson))
        {
            if (compressionTypes.Any(c => queueItem.FileName.EndsWith(c)))
            {
                return string.Empty;
            }

            extension = Path.GetExtension(queueItem.FileName);
        }
        else
        {
            var stagedFileCollection = Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<FileCollectionItem>>(stagedfileCollectionJson);

            if (stagedFileCollection.Any(fc => compressionTypes.Any(c => fc.FilePath.EndsWith(c))))
            {
                //return no compression option here because this means the file collection contains zip archive files
                return string.Empty;
            }

            var fileExtensionGroups = stagedFileCollection
                .GroupBy(x => new { fileExtension = Path.GetExtension(x.FilePath) }).Select(x => x.Key);
            var count = fileExtensionGroups.Count();
            if (count == 1)
            {
                extension = fileExtensionGroups.FirstOrDefault().fileExtension;
            }
            else
            {
                //return no compression option here because this means the file collection has a mix of file extensions
                //can only pass a single compression variable to the etl script, eg GZIP, and not GZIP and BZIP at same time
                return string.Empty;
            }
        }

        var compressionType = Greenhouse.Utilities.UtilsIO.GetCompressionType(extension).ToString();

        var isRedshiftCompressionType = Enum.IsDefined(typeof(Constants.RedshiftCompressionType), compressionType);

        //return empty string if compression type is not GZIP, LZOP, or BZIP2
        if (!isRedshiftCompressionType) return string.Empty;

        return compressionType;
    }

    public static Dictionary<string, string> GetStageFileDictionary(string stageFilesJson)
    {
        //key - source file name; value - file path 
        var fileLists = new Dictionary<string, string>();

        var stageFiles = Newtonsoft.Json.JsonConvert.DeserializeObject<IEnumerable<FileCollectionItem>>(stageFilesJson);
        foreach (var file in stageFiles)
        {
            var sourceFileNameCleansed = RemoveSpecialCharacter(file.SourceFileName);
            if (fileLists.ContainsKey(sourceFileNameCleansed))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"SourceFile key already exists - skipping key:{sourceFileNameCleansed} and value:{file.FilePath}"));
                continue;
            }
            fileLists.Add(sourceFileNameCleansed, file.FilePath);
        }

        return fileLists;
    }

    public IFile ConvertToUTF8(IFile rawFile, Uri localPath, Uri destUri, Encoding inputEncoding, string fileGUID, bool compressFile = false)
    {
        //add fileguid if not already present to raw filename so it is clear which stage files to remove
        var hasFileGuid = rawFile.Name.Contains(fileGUID, StringComparison.InvariantCultureIgnoreCase);
        var localFileUri = hasFileGuid ? RemoteUri.CombineUri(localPath, rawFile.Name) : new FileNameUtils().AppendFileGUIDToFilename(localPath.AbsolutePath, rawFile.Name, fileGUID.ToLower(), $"_");
        var localFile = new FileSystemFile(localFileUri);

        IFile destRemoteFile;

        bool cleanup = false;
        if (!localFile.Directory.Exists)
        {
            cleanup = true;
            localFile.Directory.Create();
        }

        try
        {
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"FileGUID: {fileGUID}. Start Copying file to local file system: {localFileUri.AbsolutePath}"));
            rawFile.CopyTo(localFile, true);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"FileGUID: {fileGUID}. Completed Copying file to local file system: {localFileUri.AbsolutePath}"));

            var compressionType = UtilsIO.GetCompressionType(Path.GetExtension(localFile.Name));
            //The BOM or byte-order-mark is what we use to check the encoding of the file
            //Redshift copy command fails when JSON files have this byte-order-mark
            //we exclude this byte-order-mark here when we stage the files because we only need these UTF8 files to be loaded into Redshift and then deleted after
            var utf8WithoutBom = new UTF8Encoding(false);
            UtilsIO.ChangeFileEncoding(localFile.Name, localPath, utf8WithoutBom, inputEncoding, compressionType, localFile.Get(), compressFile);
            var rac = new RemoteAccessClient(destUri, GreenhouseAWSCredential);
            var destFileName = localFile.Name;
            if (compressFile)
            {
                destFileName = $"{destFileName}.gz";
                var localGzipUri = RemoteUri.CombineUri(localPath, destFileName);
                localFile = new FileSystemFile(localGzipUri);
            }

            destRemoteFile = rac.WithFile(new Uri($"{destUri.AbsoluteUri}/{destFileName}"));
            localFile.CopyTo(destRemoteFile, true);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                $"Completed staging converted files from local file system: {localPath} to {destUri.AbsoluteUri}."));

            //clean up local directory
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                $"{JobLogGUID} - Deleting files from local import directory; fileGUID: {fileGUID}. Path {localFile.Directory.Uri.AbsolutePath}."));
        }
        finally
        {
            if (cleanup)
            {
                localFile.Directory.Delete(true);
            }
            else
            {
                localFile.Delete();
            }
        }

        return destRemoteFile;
    }

    #endregion

    #region Google Ads
    /// <summary>
    /// The entire object is wrapped between [], making it an array of object(s). If isEnd == false, then each object is separated by comma.
    /// </summary>
    /// <param name="webStream">input stream</param>
    /// <param name="isStart"></param>
    /// <param name="isEnd"></param>
    /// <param name="paths"></param>
    /// <param name="streamLength"></param>
    /// <returns>IFile that was written to</returns>
    public int WriteJsonStreamToFile(Stream webStream, StreamWriter fileStream)
    {
        int streamLength = 0;
        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JobLogGUID} - start saving data to local file"));

        using (var sr = new StreamReader(webStream))
        {
            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                fileStream.WriteLine(line);
                streamLength += line.Length;
            }
            sr.Dispose();
        }

        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JobLogGUID} - finished saving data to local file"));

        return streamLength;
    }

    [GeneratedRegex("[^a-zA-Z0-9_]+", RegexOptions.Compiled)]
    private static partial Regex SantizeColumnNameRegex();
    #endregion

}//end class

[Serializable]
internal sealed class ETLProviderException : Exception
{
    public ETLProviderException()
    {
    }

    public ETLProviderException(string message) : base(message)
    {
    }

    public ETLProviderException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

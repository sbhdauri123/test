using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.DBM.Metadata.Public
{
    [Export("DBM-PublicMetadataDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;

            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }
        public void Execute()
        {
            var queueItem = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).FirstOrDefault();
            string jobGUID = this.JED.JobGUID.ToString();

            var etl = new Greenhouse.DAL.ETLProvider();
            etl.SetJobLogGUID(jobGUID);
            var baseDestUri = base.GetDestinationFolder();
            string[] paths = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
            Uri path = RemoteUri.CombineUri(baseDestUri, paths);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Start loading {3} file->path:{1};guid:{2}; ", jobGUID, path, queueItem.FileGUID, CurrentIntegration.IntegrationName)));

            etl.LoadDBMPublicMetadata(queueItem.FileGUID, path, base.SourceId, base.IntegrationId, CurrentIntegration.CountryID, queueItem.FileCollection.ToList());

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - End  loading {1}", jobGUID, CurrentIntegration.IntegrationName)));

            Data.Services.JobService.Delete<Queue>(queueItem.ID);
        }

        //TODO: Need to move this into Greenhouse.Data or Services project. 
        //public void LoadDCMMetadata(IList<Greenhouse.Data.Model.Core.FileCollectionItem> files)
        //{
        //	if (files == null || files?.Count() == 0)
        //	{
        //		throw new Exception("LoadDCMMetadata failed, because no files were passed in");
        //	}

        //	var strGruid = System.Guid.NewGuid().ToString().Replace("-", string.Empty).ToString();

        //	string pattern = @"[\s\(\)\-\/\:\/]+";
        //	var regEx = new System.Text.RegularExpressions.Regex(pattern);

        //	var guidParam = new SqlParameter(@"@FileGUID", strGruid);

        //	var connStr = Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
        //	// Open a connection to the AdventureWorks database.guid
        //	using (SqlConnection connection = new SqlConnection(connStr))
        //	{
        //		connection.Open();
        //		using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, strGruid))
        //		{
        //			try
        //			{
        //				using (SqlBulkCopy sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, trans))
        //				{
        //					//Have to break temp table creations into two steps. Doing it in a proc creates them in a separate scope.
        //					var cmd = connection.CreateCommand();
        //					cmd.CommandText = "[dcm].[CreateTempTablesSqlText]";
        //					cmd.Transaction = trans;
        //					cmd.Connection = connection;
        //					cmd.CommandType = CommandType.StoredProcedure;
        //					cmd.Parameters.Add(guidParam);
        //					var result = cmd.ExecuteScalar();

        //					cmd.CommandText = result.ToString();
        //					cmd.CommandType = CommandType.Text;
        //					cmd.Parameters.Clear();
        //					cmd.ExecuteNonQuery();

        //					//TODO: reference sourcefile from S3.
        //					sbc.BulkCopyTimeout = 6000;
        //					foreach (var file in files)
        //					{
        //						sbc.ColumnMappings.Clear();

        //						string tableName = string.Format("dcm.#dim{0}_{1}", file.SourceFileName, strGruid);
        //						sbc.DestinationTableName = tableName;
        //						sbc.BatchSize = 10000;

        //						logger.Log(new LogEventInfo(LogLevel.Info, logger.Name, string.Format("Start Processing Filename: {0}", file.FilePath)));
        //						using (var zipStream = new GZipInputStream(new System.IO.FileStream(file.FilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read)))
        //						{
        //							using (var csvReader = new LumenWorks.Framework.IO.Csv.CsvReader(new System.IO.StreamReader(zipStream), true))
        //							{
        //								logger.Log(new LogEventInfo(LogLevel.Info, logger.Name, string.Format("Start Streaming CSV file: {0}", file.FilePath)));
        //								string[] headers = csvReader.GetFieldHeaders();

        //								foreach (string header in headers)
        //								{
        //									var sanitizedHeader = regEx.Replace(header, string.Empty);
        //									sbc.ColumnMappings.Add(header, sanitizedHeader);
        //								}

        //								logger.Log(new LogEventInfo(LogLevel.Info, logger.Name, string.Format("Start BulkCopy Filename: {0}", file.FilePath)));
        //								sbc.WriteToServer(csvReader);
        //								logger.Log(new LogEventInfo(LogLevel.Info, logger.Name, string.Format("End BulkCopy Filename: {0}", file.FilePath)));
        //							}
        //						}

        //					}//end loading each source file

        //					var sourceParam = new SqlParameter(@"@SourceID", base.SourceId);
        //					var integrationParam = new SqlParameter(@"@IntegrationID", CurrentIntegration.IntegrationID);
        //					var countryParam = new SqlParameter(@"@CountryID", CurrentIntegration.CountryID);

        //					cmd.CommandText = "[dcm].[ProcessDimensions]";
        //					cmd.Parameters.Clear();
        //					cmd.Parameters.Add(guidParam);
        //					cmd.Parameters.Add(sourceParam);
        //					cmd.Parameters.Add(integrationParam);
        //					cmd.Parameters.Add(countryParam);
        //					cmd.CommandType = CommandType.StoredProcedure;
        //					cmd.ExecuteNonQuery();

        //				}//using (SqlBulkCopy sbc = new SqlBulkCopy(connection)) {

        //				trans.Commit();
        //			}
        //			catch (Exception exc)
        //			{
        //				logger.Log(new LogEventInfo(LogLevel.Error, logger.Name, string.Format("Start BulkCopy failed: {0}", exc.Message)));
        //				trans.Rollback(strGruid);
        //				throw;
        //			}
        //		}//end using-trans
        //	}//end using connection
        //}
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

        ~DataLoadJob()
        {
            Dispose(false);
        }
    }
}

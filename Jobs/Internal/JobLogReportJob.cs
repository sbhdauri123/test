using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Mail;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Linq;
using System.Net.Mail;

namespace Greenhouse.Jobs.Internal
{
    [Export("JobLogReportJob", typeof(IDragoJob))]
    public class JobLogReportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private List<string> _emailList;
        private MailMessage _mailMessage;
        private readonly AggDataStatusRepository aggDataStatusRepository = new AggDataStatusRepository();

        public void PreExecute()
        {
            _emailList = Data.Services.SetupService.GetById<Lookup>(Greenhouse.Common.Constants.JOB_LOG_REPORT_RECIPIENTS).Value.Split(',').ToList();

            _mailMessage = GetBaseMailMessage();
        }

        public void Execute()
        {
            DataSet dsJobStatus = new DataSet("JobStatus");
            string dateFormat = "yyyy-MM-dd";

            // WILL RETURN ONLY "Delayed"
            var jobStatus = new { Status = "Delayed" };
            var jobstatusLogSources = Data.Services.SetupService.GetItems<DataStatusSource>(jobStatus).Where(d => d.ETLTypeId != (int)Constants.ETLProviderType.Redshift);
            var jobstatusAggSources = aggDataStatusRepository.CallGetAggDataStatusForEmail();
            var jobstatusIntegrations = Data.Services.SetupService.GetItems<DataStatusIntegration>(jobStatus).Where(j => j.ETLTypeID == (int)Constants.ETLProviderType.Spark);

            if (jobstatusLogSources?.Any() == true)
            {
                jobstatusLogSources = jobstatusLogSources.OrderBy(j => j.SourceName);
                DataTable dtDelayedSources = new DataTable("Delayed Log Sources");
                dtDelayedSources.Columns.Add("Source Name", typeof(string));
                dtDelayedSources.Columns.Add("Max File Date", typeof(string));
                foreach (var source in jobstatusLogSources)
                {
                    dtDelayedSources.Rows.Add(source.SourceName, source.MaxFileDate.ToString(dateFormat));
                }
                dsJobStatus.Tables.Add(dtDelayedSources);
            }

            if (jobstatusAggSources?.Any() == true)
            {
                jobstatusAggSources = jobstatusAggSources.OrderBy(j => j.SourceName);
                DataTable dtDelayedSources = new DataTable("Delayed Aggregate Sources");
                dtDelayedSources.Columns.Add("Source ID", typeof(string));
                dtDelayedSources.Columns.Add("Source Name", typeof(string));
                dtDelayedSources.Columns.Add("Max Data Date", typeof(string));
                dtDelayedSources.Columns.Add("Num of Measured FileLogs", typeof(string));
                dtDelayedSources.Columns.Add("Num COMPLETE", typeof(string));
                dtDelayedSources.Columns.Add("Num INCOMPLETE", typeof(string));
                dtDelayedSources.Columns.Add("INCOMPLETE EXAMPLES", typeof(string));
                foreach (var source in jobstatusAggSources)
                {
                    var maxDataDate = source.MaxDataDate.HasValue ? source.MaxDataDate.Value.ToString(dateFormat) : "NULL";

                    dtDelayedSources.Rows.Add(source.SourceID, source.SourceName, maxDataDate, source.NumMeasuredFileGuid, source.NumComplete, source.NumIncomplete, source.IncompleteExample);
                }
                dsJobStatus.Tables.Add(dtDelayedSources);
            }

            if (jobstatusIntegrations?.Any() == true)
            {
                jobstatusIntegrations = jobstatusIntegrations.OrderBy(j => j.IntegrationName);
                DataTable dtDelayedIntegrations = new DataTable("Delayed Log Integrations");
                dtDelayedIntegrations.Columns.Add("Integration Name", typeof(string));
                dtDelayedIntegrations.Columns.Add("Source Name", typeof(string));
                dtDelayedIntegrations.Columns.Add("Max File Date", typeof(string));
                foreach (var integration in jobstatusIntegrations)
                {
                    // FOR Errors FileDate IS POPULATED WITH LastUpdated (PROCEDURE: UpdateJobStatus)
                    // dtError.Rows.Add(j.IntegrationName, j.SourceName, j.FileDate == null ? "" : j.FileDate.Value.ToString("yyyy-MM-dd"), j.Message);
                    dtDelayedIntegrations.Rows.Add(integration.IntegrationName, integration.SourceName, integration.MaxFileDate.ToString(dateFormat));
                }
                dsJobStatus.Tables.Add(dtDelayedIntegrations);
            }

            // WILL RETURN ONLY "Error" ENTRIES VIA THE PROCEDURE
            var jobstatusError = Data.Services.SetupService.GetJobStatusSourceFile().Where(x => x.Status.Equals("error", StringComparison.InvariantCultureIgnoreCase));
            if (jobstatusError?.Any() == true)
            {
                DataTable dtError = new DataTable("Job Errors");
                dtError.Columns.Add("Integration Name", typeof(string));
                dtError.Columns.Add("Source Name", typeof(string));
                dtError.Columns.Add("Last Updated", typeof(string));
                dtError.Columns.Add("Error Message", typeof(string));
                foreach (JobStatusSourceFile j in jobstatusError)
                {
                    // FOR Errors FileDate IS POPULATED WITH LastUpdated (PROCEDURE: UpdateJobStatus)
                    dtError.Rows.Add(j.IntegrationName, j.SourceName, j.FileDate == null ? "" : j.FileDate.Value.ToString(dateFormat), j.Message);
                }

                dsJobStatus.Tables.Add(dtError);
            }

            if (dsJobStatus.Tables.Count > 0)
            {
                //convert to HTML
                _mailMessage.Body = Greenhouse.Utilities.UtilsIO.DataSetToHTML(dsJobStatus, true, true);
            }
            else
            {
                _mailMessage.Body = "No job errors have occurred.";
            }

            foreach (string User in _emailList)
            {
                if (User != null)
                {
                    string emailAddress = User.Trim();
                    if ((emailAddress.Length > 0) && (emailAddress.Contains('@')))
                    {
                        _mailMessage.To.Add(new MailAddress(emailAddress));
                    }
                }
            }

            if (_mailMessage.To.Any())
            {
                var mailResult = SMTPMailClient.SendMessage(_mailMessage);
                if (mailResult == null || !MailResult.Success)
                {
                    throw new SmtpException($"Error Sending Email in JobLogReportJob.Execute - Exception: {mailResult?.Error?.Message} - Recipent(s): {string.Join(",", _mailMessage.To.Select(to => to.Address))} - Email Body: {_mailMessage?.Body}");
                }
            }
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

        ~JobLogReportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        #region Helper Methods

        private MailMessage GetBaseMailMessage()
        {
            //Build mail _mailMessage
            MailMessage mailMessage = new System.Net.Mail.MailMessage();
            mailMessage.IsBodyHtml = true;
            mailMessage.Sender = new MailAddress(Greenhouse.Configuration.Settings.Current.Email.MailAdminFrom, "DataLake Admin");
            mailMessage.From = new MailAddress(Greenhouse.Configuration.Settings.Current.Email.MailAdminFrom, "DataLake Admin");
            string env = (base.Environment == "PROD" ? string.Empty : string.Format("{0}-", base.Environment.ToUpper()));
            mailMessage.Subject = string.Format("{0}{1}", env, "Job Log Report");
            mailMessage.Priority = MailPriority.High;

            return mailMessage;
        }

        private static Greenhouse.Mail.Clients.SMTPMailClient SMTPMailClient
        {
            get
            {
                return new Greenhouse.Mail.Clients.SMTPMailClient();
            }
        }
        #endregion
    }
}

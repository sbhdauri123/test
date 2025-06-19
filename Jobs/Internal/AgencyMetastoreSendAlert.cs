using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Mail;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net.Mail;

namespace Greenhouse.Jobs.Internal
{
    [Export("AgencyMetastoreSendAlert", typeof(IDragoJob))]
    public class AgencyMetastoreSendAlert : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public void PreExecute()
        {
        }

        public void Execute()
        {
            var metadataPartitionCollection = Data.Services.SetupService.GetAgencyMetastoreStatus().ToList();
            if (metadataPartitionCollection?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Agency Metastore Partition is up-to-date. Email Alert will NOT be sent.")));
                return;
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"START: Agency Metastore Partition job.")));

            SendEmailAlert(metadataPartitionCollection);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"END: Agency Metastore Partition job")));
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

        ~AgencyMetastoreSendAlert()
        {
            Dispose(false);
        }

        /// <summary>
        /// Setup email alert to be sent
        /// </summary>
        /// <param name="agencyMetastoreStatus"></param>
        public void SendEmailAlert(List<MetadataPartition> agencyMetastoreStatus)
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"START: Sending Email.")));

            var env = Greenhouse.Configuration.Settings.Current.Application.Environment;
            var subj = "Agency Metastore Partition Status Report";

            string subject = (env == "PROD") ? $"{subj}" : $"{env} - {subj}";
            string sender = Greenhouse.Configuration.Settings.Current.Email.MailAdminFrom;
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"START: Generating Email Message Body.")));
            string messageBody = GenerateEmailAlertMessageBody(agencyMetastoreStatus);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"END: Generating Email Message Body.")));
            string attachmentFilename = string.Empty;
            string emailRecipients = Data.Services.SetupService.GetById<Lookup>(Constants.AGENCY_METASTORE_EMAIL_RECIPIENTS).Value;

            SendEmailAlert(subject, sender, messageBody, attachmentFilename, emailRecipients);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"END: Sending Email.")));
        }
        /// <summary>
        /// Generates the email alert message body
        /// </summary>
        /// <param name="agencyMetastoreStatus"></param>
        /// <returns></returns>
        private static string GenerateEmailAlertMessageBody(IEnumerable<MetadataPartition> agencyMetastoreStatus)
        {
            var agencyMetastoreStatusDictionary = agencyMetastoreStatus.GroupBy(fileItems => fileItems.Agency).ToDictionary(agency => agency.Key, status => status.ToList());
            var textInfo = new CultureInfo("en-US", false).TextInfo;

            DataSet dsJobStatus = new DataSet("JobStatus");

            foreach (var agency in agencyMetastoreStatusDictionary)
            {
                DataTable dtDelayedAgencyMetastore = new DataTable($"Delayed {textInfo.ToTitleCase(agency.Key)}");
                dtDelayedAgencyMetastore.Columns.Add("Source Name", typeof(string));
                dtDelayedAgencyMetastore.Columns.Add("File Type", typeof(string));
                dtDelayedAgencyMetastore.Columns.Add("Partition Created Date", typeof(string));
                foreach (var metatstoreStatus in agency.Value)
                {
                    dtDelayedAgencyMetastore.Rows.Add(metatstoreStatus.Source, textInfo.ToTitleCase(metatstoreStatus.TableName.ToLower().Replace("fact", "")), metatstoreStatus.PartitionCreatedDate.ToString("yyyy-MM-dd"));
                }

                dsJobStatus.Tables.Add(dtDelayedAgencyMetastore);
                dtDelayedAgencyMetastore = new DataTable();
            }

            return Greenhouse.Utilities.UtilsIO.DataSetToHTML(dsJobStatus, false, false);
        }

        /// <summary>
        /// Sends email alert
        /// </summary>
        /// <param name="agencyMetastoreStatus"></param>
        /// <returns></returns>
        public void SendEmailAlert(string subject, string sender, string messageBody, string attachmentFilename, string emailRecipients)
        {
            MailMessage mailMessage = new MailMessage();
            mailMessage.IsBodyHtml = true;
            mailMessage.From = new MailAddress(sender);
            mailMessage.Subject = subject;
            mailMessage.Body = messageBody;
            foreach (var address in emailRecipients.Split(Constants.SEMICOLON_ARRAY, StringSplitOptions.RemoveEmptyEntries))
            {
                if (!string.IsNullOrEmpty(address))
                {
                    mailMessage.To.Add(address);
                }
            }
            if (!string.IsNullOrEmpty(attachmentFilename)) { mailMessage.Attachments.Add(new Attachment(attachmentFilename)); }
            Mail.Clients.SMTPMailClient client = new Mail.Clients.SMTPMailClient();
            var mailResult = client.SendMessage(mailMessage);
            if (mailResult == null || !MailResult.Success)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, $"Error Sending Email in AgencyMetastoreSendAlert.SendEmailAlert - Exception: {mailResult?.Error?.Message} - Recipent(s): {string.Join(",", mailMessage.To.Select(to => to.Address))} - Email Body: {messageBody}"));
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Email sent to: {emailRecipients}.")));
        }
    }
}

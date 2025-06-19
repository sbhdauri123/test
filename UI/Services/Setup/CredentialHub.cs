using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;

namespace Greenhouse.UI.Services.Setup
{
    public class CredentialHub : BaseHub<Credential>
    {
        public override Credential Update(Credential item)
        {
            var action = Constants.AuditLogAction.Update.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            SetLastUpdated(item);

            Data.Services.SetupService.Update(item);
            Clients.Others.update(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        public override Credential Create(Credential item)
        {
            var action = Constants.AuditLogAction.Create.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            Data.Services.SetupService.Add(item);
            Clients.Others.create(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }
    }
}
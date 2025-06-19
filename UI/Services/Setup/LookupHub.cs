using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;

namespace Greenhouse.UI.Services.Setup
{
    public class LookupHub : BaseHub<Lookup>
    {
        public override Lookup Create(Lookup item)
        {
            var action = Constants.AuditLogAction.Create.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            LookupRepository.AddOrUpdateLookup(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        public override Lookup Update(Lookup item)
        {
            var action = Constants.AuditLogAction.Update.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            LookupRepository.AddOrUpdateLookup(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }
    }
}
using Greenhouse.Common;
using Greenhouse.Data.Model.AdTag;
using Greenhouse.Data.Repositories;

namespace Greenhouse.UI.Services.AdTag
{
    public class JobRunHub : BaseHub<JobRun>
    {
        private readonly AdTagJobRunRepository _repo;
        public JobRunHub(Greenhouse.Data.Services.AdTagService service) : base(service)
        {
            _repo = new AdTagJobRunRepository();
        }

        public IEnumerable<JobRun> ReadAll()
        {
            var jobRuns = _repo.GetAll();
            return jobRuns;
        }

        public override JobRun Destroy(JobRun item)
        {
            var action = Constants.AuditLogAction.Delete.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            _repo.Delete(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }
    }
}
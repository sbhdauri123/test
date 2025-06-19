using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Newtonsoft.Json;

namespace Greenhouse.UI.Services.Setup
{
    public class InstanceHub : BaseHub<Instance>
    {
        private readonly InstanceRepository _repo;

        public InstanceHub()
        {
            _repo = new InstanceRepository();
        }

        public override IEnumerable<Instance> Read()
        {
            var data = _repo.GetAll();

            return data;
        }

        public override Instance Create(Instance item)
        {
            var action = Constants.AuditLogAction.Create.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            item.EMRClusterId = item.EMRClusterId.TrimStart().TrimEnd();

            _repo.Add(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        public override Instance Update(Instance item)
        {
            Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();

            var entity = typeof(Instance);
            auditLog.AppComponent = entity.Name;
            auditLog.Action = Constants.AuditLogAction.Update.ToString();

            var props = Data.Services.SetupService.GetPropertyInfo<Instance>();

            var keyName = props.Select(x => x.Name).FirstOrDefault();

            if (keyName != null)
            {
                var value = typeof(Instance).GetProperties().FirstOrDefault(x => x.Name == keyName).GetValue(item);
                var origItem = _repo.GetById(value);

                auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    OriginalValue = origItem,
                    ModifiedValue = item
                });
            }
            else
            {
                auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    OriginalValue = "NA - could not find record by ID",
                    ModifiedValue = item
                });
            }

            SetLastUpdated(item);

            item.EMRClusterId = item.EMRClusterId.TrimStart().TrimEnd();

            _repo.Update(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        public override Instance Destroy(Instance item)
        {
            var action = Constants.AuditLogAction.Delete.ToString();
            var auditLog = base.CreateAuditLog(item, action);

            _repo.Delete(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        //public override void Destroy(JobCategory item)
        //{
        //if (item != null)
        //{
        //    var JobCategoryfiles = context.Scan<JobCategoryFile>(new ScanCondition("JobCategoryGUID", Amazon.DynamoDBv2.DocumentModel.ScanOperator.Equal, item.GUID));

        //    context.Delete(item);
        //    Clients.Others.destroy(item);

        //    foreach (var JobCategoryfile in JobCategoryfiles)
        //    {
        //        context.Delete(JobCategoryfile);
        //        Clients.Others.destroy(JobCategoryfile);
        //    }
        //}
        //}

    }
}

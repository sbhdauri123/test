using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.UI.Services.Setup;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;

namespace Greenhouse.UI.Services.AdTag
{
    public abstract class BaseHub<T> : Hub<IBaseHub<T>>
    {
        protected readonly Greenhouse.Data.Services.AdTagService baseService;

        public BaseHub(Greenhouse.Data.Services.AdTagService service) : base()
        {
            this.baseService = service;
        }

        protected void SaveAuditLog(AuditLog auditLog)
        {
            auditLog.ModifiedBy = Context.User.Identity.Name;
            baseService.Add<AuditLog>(auditLog);
        }

        protected AuditLog CreateAuditLog(T item, string action, bool getOriginalValue = true)
        {
            Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();

            var entity = typeof(T);
            auditLog.AppComponent = entity.Name;
            auditLog.Action = action;

            if (action == Constants.AuditLogAction.Update.ToString())
            {
                var props = Data.Services.AdTagService.GetPropertyInfo<T>();

                var keyName = props.Select(x => x.Name).FirstOrDefault();

                if (keyName != null && getOriginalValue)
                {
                    var value = typeof(T).GetProperties().FirstOrDefault(x => x.Name == keyName).GetValue(item);
                    var origItem = baseService.GetById<T>(value);

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
            }
            else
            {
                auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    ModifiedValue = item
                });
            }

            return auditLog;
        }

        #region CRUD

        public virtual async Task<IEnumerable<T>> ReadAsync()
        {
            var data = await Task.Run(() => baseService.GetAll<T>());
            return data;
        }

        public virtual IEnumerable<T> Read()
        {
            var data = baseService.GetAll<T>();
            return data;
        }

        public virtual T ReadByID(string guid)
        {
            return baseService.GetById<T>(guid);
        }

        public virtual T Update(T item)
        {
            var property = item.GetType().GetProperty("LastUpdated");

            var action = Constants.AuditLogAction.Update.ToString();
            var auditLog = CreateAuditLog(item, action);

            if (property != null)
            {
                property.SetValue(item, DateTime.UtcNow);
            }

            baseService.Update<T>(item);
            Clients.Others.update(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        public virtual T Destroy(T item)
        {
            if (item != null)
            {
                var action = Constants.AuditLogAction.Delete.ToString();
                var auditLog = CreateAuditLog(item, action);

                baseService.Delete(item);
                Clients.Others.destroy(item);

                auditLog.CreatedDate = DateTime.Now;
                SaveAuditLog(auditLog);
            }
            return item;
        }

        public virtual T Create(T item)
        {
            var action = Constants.AuditLogAction.Create.ToString();
            var auditLog = CreateAuditLog(item, action);

            baseService.Add<T>(item);
            Clients.Others.create(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        #endregion
    }
}

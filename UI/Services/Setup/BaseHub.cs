using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;

namespace Greenhouse.UI.Services.Setup
{
    public abstract class BaseHub<T> : Hub<IBaseHub<T>>
    {
        protected void SaveAuditLog(AuditLog auditLog)
        {
            auditLog.ModifiedBy = Context.User?.Identity?.Name ?? "Unknown";
            Data.Services.SetupService.Add(auditLog);
        }

        protected AuditLog CreateAuditLog(T item, string action, bool getOriginalValue = true)
        {
            Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();

            var entity = typeof(T);
            auditLog.AppComponent = entity.Name;
            auditLog.Action = action;

            if (action == Constants.AuditLogAction.Update.ToString())
            {
                var props = Data.Services.SetupService.GetPropertyInfo<T>();

                var keyName = props.Select(x => x.Name).FirstOrDefault();

                if (keyName != null && getOriginalValue)
                {
                    var value = typeof(T).GetProperties().FirstOrDefault(x => x.Name == keyName)?.GetValue(item);
                    var origItem = Data.Services.SetupService.GetById<T>(value);

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
            var data = await Task.Run(() => Data.Services.SetupService.GetAll<T>());
            return data;
        }

        public virtual IEnumerable<T> Read()
        {
            var data = Data.Services.SetupService.GetAll<T>();
            return data;
        }

        public virtual T ReadByID(string guid)
        {
            return Data.Services.SetupService.GetById<T>(guid);
        }

        public virtual T Update(T item)
        {
            var action = Constants.AuditLogAction.Update.ToString();
            var auditLog = CreateAuditLog(item, action);

            SetLastUpdated(item);

            Data.Services.SetupService.Update(item);
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

                Data.Services.SetupService.Delete(item);
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

            Data.Services.SetupService.Add(item);
            Clients.Others.create(item);

            auditLog.CreatedDate = DateTime.Now;
            SaveAuditLog(auditLog);

            return item;
        }

        #endregion
        public static void SetLastUpdated(T item)
        {
            var property = item?.GetType().GetProperty("LastUpdated");

            property?.SetValue(item, DateTime.UtcNow);
        }
    }
}

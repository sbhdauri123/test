using Dapper;
using Greenhouse.Data.Model.Setup;
using System.Data;

namespace Greenhouse.UI.Services.Setup
{
    public class ManageUserHub : BaseHub<Data.Model.Setup.UserMapping>
    {
        private readonly Data.Repositories.UserMappingRepository repo = new Data.Repositories.UserMappingRepository();

        public IEnumerable<dynamic> ReadAll()
        {
            Data.Repositories.UserMappingRepository repo = new Data.Repositories.UserMappingRepository();
            var result = repo.GetAllUsersMapping();
            var data = result.Select(x =>
            {
                var user = new
                {
                    x.UserID,
                    x.IsAdvertiser,
                    AdvertiserName = (x.IsAdvertiser) ? x.MappedName : string.Empty,
                    InstanceName = (!x.IsAdvertiser) ? x.MappedName : string.Empty,
                };
                return user;
            }).GroupBy(x => x.UserID)
            .Select(x => new
            {
                UserID = x.Key,
                Advertisers = string.Join(",", x.Where(a => a.IsAdvertiser && !string.IsNullOrEmpty(a.AdvertiserName)).Select(a => a.AdvertiserName)),
                Instances = string.Join(",", x.Where(a => !a.IsAdvertiser && !string.IsNullOrEmpty(a.InstanceName)).Select(a => a.InstanceName))
            }
            );

            return data;
        }
        public dynamic UpdateUser(UserMapping user)
        {
            base.SaveAuditLog(new AuditLog
            {
                AppComponent = "ManageUserHub",
                Action = "UpdateUser",
                AdditionalDetails = user.ToString()
            });

            user.LastUpdated = DateTime.Now;
            UpdateUserMappings(user);
            Clients.Others.update(user);
            return user;
        }

        public dynamic CreateUser(UserMapping user)
        {
            base.SaveAuditLog(new AuditLog
            {
                AppComponent = "ManageUserHub",
                Action = "CreateUser",
                AdditionalDetails = user.ToString()
            });

            user.LastUpdated = DateTime.Now;
            UpdateUserMappings(user);
            Clients.Others.create(user);
            return user;
        }

        public override UserMapping Destroy(UserMapping user)
        {
            base.SaveAuditLog(new AuditLog
            {
                AppComponent = "ManageUserHub",
                Action = "DeleteUserMapping",
                AdditionalDetails = user.ToString()
            });

            string procName = "DeleteUserMapping";
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@UserID", user.UserID);
            repo.ExecuteStoredProc(procName, parameters);

            Clients.Others.destroy(user);
            return user;
        }

        private void UpdateUserMappings(UserMapping user)
        {
            string procName = "UpdateUserMapping";

            var advertiserMappingTable = CreateMappingTable("AdvertiserMappingID");
            PopulateMappingTable(advertiserMappingTable, user.AdvertiserIDS);

            var instanceMappingTable = CreateMappingTable("InstanceID");
            PopulateMappingTable(instanceMappingTable, user.InstanceIDS);

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@UserID", user.UserID);
            parameters.Add("@AdvertiserIDS", advertiserMappingTable.AsTableValuedParameter("dbo.AdvertiserMappingTableType"));
            parameters.Add("@InstanceIDS", instanceMappingTable.AsTableValuedParameter("dbo.InstanceMappingTableType"));
            repo.ExecuteStoredProc(procName, parameters);
        }

        private static DataTable CreateMappingTable(string columnName)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add(columnName, typeof(string));
            return dt;
        }

        private static IEnumerable<int> GetMappingIds(string idList)
        {
            IEnumerable<int> mappingIds = new[] { 0 };

            if (string.IsNullOrEmpty(idList)) return mappingIds;
            var advertiserIdArray = idList.Split(',');
            mappingIds = advertiserIdArray.Select(int.Parse);

            return mappingIds;
        }

        private static void PopulateMappingTable(DataTable mappingTable, string idList)
        {
            var advertiserMappingIds = GetMappingIds(idList);
            foreach (var id in advertiserMappingIds)
            {
                mappingTable.Rows.Add(id);
            }
        }
    }
}

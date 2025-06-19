using Greenhouse.Data.Model.Setup;
using System;

namespace Greenhouse.Data.Repositories
{
    public class LookupRepository : BaseRepository<Model.Setup.Lookup>
    {
        public static void AddOrUpdateLookup(Lookup item)
        {
            string key = item.Name;
            Lookup existingLookup = Services.SetupService.GetById<Lookup>(key);

            if (existingLookup != null)
            {
                existingLookup.Value = item.Value;
                existingLookup.LastUpdated = DateTime.UtcNow;
                existingLookup.IsEditable = item.IsEditable;
                Services.SetupService.Update(existingLookup);
            }
            else
            {
                Services.SetupService.InsertIntoLookup(key, item.Value, item.IsEditable);
            }
        }
    }
}

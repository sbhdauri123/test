using System.Collections.Generic;

namespace Greenhouse.Data.Model.APIEntity
{
    public class DeactivatedInactiveEntitesAudit
    {
        public string JobRunDateTime { get; set; }
        public IEnumerable<string> Entities { get; set; }
    }
}

using Greenhouse.Data.Model.Aggregate;
using Microsoft.AspNetCore.SignalR;

namespace Greenhouse.UI.Services.Setup
{
    public class APIEntityHub : BaseHub<APIEntity>
    {
        public override IEnumerable<APIEntity> Read()
        {
            var data = base.Read();
            return data.Select(d =>
            {
                d.BackfillPriority = d.BackfillPriority ?? false;
                return d;
            });
        }

        public override APIEntity Update(APIEntity item)
        {
            try
            {
                item.StartDate = item.StartDate?.Date;
                return base.Update(item);
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("unique key constraint", StringComparison.InvariantCultureIgnoreCase))
                    throw new HubException("The combination of Entity Code and Source should be unique.");
                else
                    throw new HubException(ex.Message);
            }
        }

        public override APIEntity Create(APIEntity item)
        {
            try
            {
                return base.Create(item);
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("unique key constraint", StringComparison.InvariantCultureIgnoreCase))
                    throw new HubException("The combination of Entity Code and Source should be unique.");
                else
                    throw new HubException(ex.Message);
            }
        }
    }
}
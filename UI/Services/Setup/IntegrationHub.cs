using Greenhouse.Data.Model.Setup;
using Microsoft.AspNetCore.SignalR;

namespace Greenhouse.UI.Services.Setup
{
    public class IntegrationHub : BaseHub<Integration>
    {
        public override IEnumerable<Integration> Read()
        {
            var data = base.Read();
            data = data.Select(d =>
            {
                if (!d.InstanceID.HasValue)
                {
                    d.InstanceID = -1;
                }
                return d;
            });

            return data;
        }

        public override Integration Update(Integration item)
        {
            if (item.InstanceID == -1)
            {
                item.InstanceID = null;
            }
            try
            {
                return base.Update(item);
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("unique key constraint", StringComparison.InvariantCultureIgnoreCase))
                    throw new HubException("The combination of Source, EndPointURI and Credential should be unique.");
                else
                    throw new HubException(ex.Message);
            }
        }

        public override Integration Create(Integration item)
        {
            if (item.InstanceID == -1)
            {
                item.InstanceID = null;
            }
            try
            {
                return base.Create(item);
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("unique key constraint", StringComparison.InvariantCultureIgnoreCase))
                    throw new HubException("The combination of Source, EndPointURI and Credential should be unique.");
                else
                    throw new HubException(ex.Message);
            }
        }

        //public override Integration Update(Integration item)
        //{
        //	item.FileStartDate = Drago.Utilities.UtilsDate.GetUnixTimestamp(item.FileStartDateTime);
        //	return base.Update(item);

        //}

    }
}
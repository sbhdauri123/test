using Greenhouse.Data.Model.Setup;

namespace Greenhouse.UI.Services.Setup
{
    public class SchedulerConfigurationHub : BaseHub<SchedulerConfiguration>
    {
        //public override void Destroy(SchedulerConfiguration item)
        //{
        //if (item != null)
        //{
        //    var SchedulerConfigurationfiles = context.Scan<SchedulerConfigurationFile>(new ScanCondition("SchedulerConfigurationGUID", Amazon.DynamoDBv2.DocumentModel.ScanOperator.Equal, item.GUID));

        //    context.Delete(item);
        //    Clients.Others.destroy(item);

        //    foreach (var SchedulerConfigurationfile in SchedulerConfigurationfiles)
        //    {
        //        context.Delete(SchedulerConfigurationfile);
        //        Clients.Others.destroy(SchedulerConfigurationfile);
        //    }
        //}
        //}

    }
}
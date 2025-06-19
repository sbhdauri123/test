using Greenhouse.Data.Model.Setup;

namespace Greenhouse.UI.Services.Setup
{
    public class SecurityHub : BaseHub<Security>
    {
        //public override void Destroy(Security item)
        //{
        //if (item != null)
        //{
        //    var Securityfiles = context.Scan<SecurityFile>(new ScanCondition("SecurityGUID", Amazon.DynamoDBv2.DocumentModel.ScanOperator.Equal, item.GUID));

        //    context.Delete(item);
        //    Clients.Others.destroy(item);

        //    foreach (var Securityfile in Securityfiles)
        //    {
        //        context.Delete(Securityfile);
        //        Clients.Others.destroy(Securityfile);
        //    }
        //}
        //}

    }
}
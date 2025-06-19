using Greenhouse.Data.Model.Setup;

namespace Greenhouse.UI.Services.Setup
{
    public class JobCategoryHub : BaseHub<JobCategory>
    {
        //JobCategoryRepository repo;

        //public override IEnumerable<JobCategory> Read()
        //{
        //    repo = new JobCategoryRepository();
        //    var data = repo.GetAll();

        //    return data;
        //}

        //public override JobCategory Create(JobCategory item)
        //{
        //    repo = new JobCategoryRepository();
        //    repo.Add(item);

        //    return item;
        //}

        //public override JobCategory Update(JobCategory item)
        //{
        //    repo = new JobCategoryRepository();
        //    repo.Update(item);

        //    return item;
        //}

        //public override JobCategory Destroy(JobCategory item)
        //{
        //    repo = new JobCategoryRepository();
        //    repo.Delete(item);

        //    return item;
        //}

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

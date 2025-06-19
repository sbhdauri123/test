using Greenhouse.Data.Model.Setup;

namespace Greenhouse.UI.Services.Setup
{
    public class SourceFileHub : BaseHub<SourceFile>
    {
        public IEnumerable<SourceFile> ReadAll(string guid)
        {
            var sourceFiles = Data.Services.SetupService.GetItems<SourceFile>(new { SourceID = guid });
            return sourceFiles;
        }
    }
}
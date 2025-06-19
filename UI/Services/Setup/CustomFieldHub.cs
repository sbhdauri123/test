using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;

namespace Greenhouse.UI.Services.Setup
{
    public class CustomFieldHub : BaseHub<CustomField>
    {
        public override IEnumerable<CustomField> Read()
        {
            var customFieldRepository = new CustomFieldRepository();
            return CustomFieldRepository.GetAllSavedColumns();
        }

        public override CustomField Update(CustomField item)
        {
            var customFieldRepository = new CustomFieldRepository();
            bool success = CustomFieldRepository.Update(item);
            return success ? item : null;
        }
    }
}
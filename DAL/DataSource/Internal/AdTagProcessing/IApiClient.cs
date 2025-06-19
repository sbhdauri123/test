using Greenhouse.Data.Model.AdTag.APIAdServer;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Internal.AdTagProcessing
{
    public interface IApiClient
    {
        Task<string> UpdateDCMPlacementAsync(UpdateDCMPlacementOptions options);
        Task<List<Placement>> GetAllDCMPlacementsAsync(GetAllDCMPlacementsOptions options);
    }
}
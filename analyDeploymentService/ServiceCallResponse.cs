using System.Text;

namespace Greenhouse.DeploymentService
{
    [Serializable]
    public class ServiceCallResponse
    {
        public ServiceCallResponse()
        {
            ServiceResults = new List<ServiceResult>();
        }
        public bool Success
        {
            get
            {
                if (ServiceResults.Any(x => !x.Success))
                {
                    return false;
                }
                return true;
            }
        }

        public List<ServiceResult> ServiceResults { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine(string.Format("Success: {0}", this.Success));
            foreach (ServiceResult res in this.ServiceResults)
            {
                sb.AppendLine(res.ToString());
            }
            return sb.ToString();
        }
    }
}

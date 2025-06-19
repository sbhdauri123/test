using Greenhouse.Data.Model.Auth;

namespace Greenhouse.UI.Services.Setup
{
    public class AuthorizationHub : BaseHub<Data.Model.Auth.UserAuthorization>
    {

        public override UserAuthorization Update(UserAuthorization item)
        {
            item.LastUpdated = DateTime.Now;
            return base.Update(item);
        }

        public override UserAuthorization Create(UserAuthorization item)
        {
            item.LastUpdated = DateTime.Now;

            return base.Create(item);
        }
    }
}
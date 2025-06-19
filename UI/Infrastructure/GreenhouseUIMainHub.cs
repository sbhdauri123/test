using Microsoft.AspNetCore.SignalR;
//using Microsoft.AspNet.SignalR;

namespace Greenhouse.UI.Infrastructure
{
    public class GreenhouseUIMainHub : Hub, IChat
    {
        private sealed class GreenhouseUser
        {
            public string Group { get; set; }
            public string UserName { get; set; }
            public List<string> ConnectionID { get; set; }
        }

        public void Broadcast(string msg)
        {
            string str = string.Format("Broadcasted message \"{0}\" at {1} to all users", msg, DateTime.Now.ToShortTimeString());
            //Clients.All.messageBroadcast(str);
        }

        public void Connect()
        {
            if (Context.User != null)
            {
                var currUser = Context.User.Identity.Name;
                var users = Get<GreenhouseUser>("GreenhouseUsers");
                if (users == null) /*cache empty*/
                {
                    users = new List<GreenhouseUser>(){
                    new GreenhouseUser(){
                        UserName = currUser
                        ,ConnectionID = new List<string>{ Context.ConnectionId}
                    }
                };
                    Add(users, "GreenhouseUsers");
                }
                else
                {
                    var user = users.FirstOrDefault(x => x.UserName == currUser);
                    if (user == null) //Cache user connection
                    {
                        users.Add(new GreenhouseUser()
                        {
                            UserName = currUser,
                            ConnectionID = new List<string> { Context.ConnectionId }
                        });
                    }
                    else //Exisitng User connecting with a new session
                    {
                        user.ConnectionID.Add(Context.ConnectionId);
                    }
                }
            }
        }

        public void Disconnect()
        {
            var currUser = Context.User.Identity.Name;
            var users = Get<GreenhouseUser>("GreenhouseUsers");
            if (users != null)
            {
                var user = users.FirstOrDefault(x => x.UserName == currUser);
                if (user != null)
                {
                    users.Remove(user);
                }
            }
        }

        public IList<string> GetUsers()
        {
            var users = Get<GreenhouseUser>("GreenhouseUsers");
            return users.Select(x => x.UserName).ToList();
        }
        public async void SendMessageAsync(string msg, params string[] uid)
        {
            var users = Get<GreenhouseUser>("GreenhouseUsers");

            if (users == null)
                return;

            var targetUsers = users.Where(x => uid.Contains(x.UserName)).SelectMany(x => x.ConnectionID).Distinct().ToList();
            //Clients.Clients(targetUsers).sendMessage(msg);

            await Clients.Clients(targetUsers).SendAsync("messageReceived", msg);

            //var clusterHub = GlobalHost.ConnectionManager.GetHubContext<Greenhouse.UI.Areas.Stallion.Services.ClusterHub>();
            //clusterHub.Clients.All.Send("Are you on the cluster's page?");
        }

        private static void Add<T>(T o, string key)
        {
            //int timeoutInterval = 30;

            //HttpContext.Current.Cache.Insert(
            //	key,
            //	o,
            //	null,
            //	System.Web.Caching.Cache.NoAbsoluteExpiration,
            //	new TimeSpan(0, timeoutInterval, 0),
            //	System.Web.Caching.CacheItemPriority.Normal,
            //	null);
        }

        private static bool Exists(string key)
        {
            //return HttpContext.Current.Cache[key] != null;
            return false;
        }

        /// <summary>
        /// Retrieve cached item
        /// </summary>
        /// <typeparam name="T">Type of cached item</typeparam>
        /// <param name="key">Name of cached item</param>
        /// <param name="value">Cached value. Default(T) if
        /// item doesn't exist.</param>
        /// <returns>Cached item as type</returns>
        private static IList<T> Get<T>(string key)
        {
            IList<T> users = null;
            try
            {
                if (!Exists(key))
                {
                    users = null;
                }
                //users = (IList<T>)HttpContext.Current.Cache[key];
            }
            catch
            {
                users = null;
            }

            return users;
        }
    }
}
namespace Greenhouse.UI.Infrastructure
{
    internal interface IChat
    {
        void Connect();
        void Disconnect();
        void Broadcast(string msg);
        void SendMessageAsync(string msg, params string[] uid);
        IList<string> GetUsers();
    }

    internal interface IHub<T>
    {
        IEnumerable<T> Read();
        void Update(T item);
        void Delete(T item);
        T Create(T item);
    }
}

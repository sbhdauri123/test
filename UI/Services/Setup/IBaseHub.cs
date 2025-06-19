namespace Greenhouse.UI.Services.Setup
{
    public interface IBaseHub<T>
    {
        Task create(T item);
        Task update(T item);
        Task destroy(T item);
    }
}

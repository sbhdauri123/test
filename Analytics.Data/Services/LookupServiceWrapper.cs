namespace Greenhouse.Data.Services;

/// <summary>
/// This wrapper is used to register ILookupService in the service collection.
/// Original LookupService needs refactoring to convert it from a static class to a singleton service.
/// Once refactoring is done, and LookupService lifetime is now managed accordingly, we can discard this wrapper.
/// </summary>
public class LookupServiceWrapper : ILookupService
{
    public T GetAndDeserializeLookupValueWithDefault<T>(string key, T defaultValue)
    {
        return LookupService.GetAndDeserializeLookupValueWithDefault(key, defaultValue);
    }
}
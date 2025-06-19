namespace Greenhouse.Data.Services;

public interface ILookupService
{
    T GetAndDeserializeLookupValueWithDefault<T>(string lookupKey, T defaultValue);
}
using Greenhouse.JobService.Interfaces;

namespace Greenhouse.JobService.Strategies.Factory;

public class JobStrategyFactory : IJobStrategyFactory
{
    private readonly Dictionary<string, IJobStrategy> _strategies;

    public JobStrategyFactory(IEnumerable<IJobStrategy> strategies)
    {
        ArgumentNullException.ThrowIfNull(strategies);

        _strategies = strategies.ToDictionary(s => s.ContractKey);
    }

    public IJobStrategy GetStrategy(string contractKey)
    {
        ArgumentNullException.ThrowIfNull(contractKey);

        if (_strategies.TryGetValue(contractKey, out IJobStrategy? strategy))
        {
            return strategy;
        }

        throw new KeyNotFoundException($"No strategy found for contract key: {contractKey}");
    }
}
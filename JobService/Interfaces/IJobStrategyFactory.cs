namespace Greenhouse.JobService.Interfaces;

public interface IJobStrategyFactory
{
    IJobStrategy GetStrategy(string contractKey);
}
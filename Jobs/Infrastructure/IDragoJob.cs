using System;

namespace Greenhouse.Jobs.Infrastructure
{
    public interface IDragoJob : IDisposable
    {
        void PreExecute();

        void Execute();

        void PostExecute();
    }
}

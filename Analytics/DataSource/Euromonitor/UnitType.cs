using System;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public static class UnitType
    {
        public const int Percentage = 0;
        public const int Ranking = 1;
        public const int Actual = 2;

        public static int GetValue(string unitType)
        {
            switch (unitType)
            {
                case nameof(Percentage):
                    return Percentage;
                case nameof(Ranking):
                    return Ranking;
                case nameof(Actual):
                    return Actual;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
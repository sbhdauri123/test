using System;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public static class FileType
    {
        public const int Csv = 0;

        public const int Json = 2;

        public static int GetValue(string fileType)
        {
            switch (fileType)
            {
                case nameof(Csv):
                    return Csv;
                case nameof(Json):
                    return Json;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}

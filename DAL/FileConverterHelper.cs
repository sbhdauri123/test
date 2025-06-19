using ClosedXML.Excel;
using Greenhouse.Services.RemoteAccess;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Greenhouse.Utilities
{
    public static class FileConverterHelper
    {
        public static void ConvertExcelToCSV(IFile input, IFile output, int inputLinesToSkip = 0, bool addRowNumber = false)
        {
            using (var stream = input.Get())
            using (XLWorkbook workbook = new XLWorkbook(stream))
            using (var destLocalStream = output.Create())
            using (var writer = new StreamWriter(destLocalStream))
            {
                var worksheet = workbook.Worksheets.First();

                bool isFirstRow = true;
                int currentLineCount = 0, totalColumns = 0;

                int rowNumber = 0;

                foreach (var row in worksheet.Rows())
                {
                    // skip header lines if specified
                    if (inputLinesToSkip > currentLineCount++) continue;

                    var columnValues = new List<string>();

                    // first row is important: determinates the number of columns
                    // as there is no rule for excel columns to be consitent in a sheet
                    if (isFirstRow)
                    {
                        // using the column number of the last column returned 
                        // instead of a count of the columns as columns with no values
                        // are not returned
                        totalColumns = row.Cells().Last().Address.ColumnNumber;
                        isFirstRow = false;
                    }

                    int currentColumn = 1;

                    foreach (var cell in row.Cells())
                    {
                        // columns with no value are not returned by the library ClosedXML
                        // cell.Address.ColumnNumber contains the number each column
                        // and help to detect missing columns
                        while (currentColumn++ < cell.Address.ColumnNumber) columnValues.Add(string.Empty);

                        columnValues.Add(cell.Value.ToString());
                    }

                    // add eventual missing columns after last value
                    for (int j = currentColumn - 1; j < totalColumns; j++)
                    {
                        columnValues.Add(string.Empty);
                    }

                    if (addRowNumber)
                    {
                        if (rowNumber == 0)
                            columnValues.Add("RowNum");
                        else
                            columnValues.Add($"{rowNumber}");

                        rowNumber++;
                    }

                    writer.WriteLine(string.Join(',', columnValues.Select(v => "\"" + v.Replace("\"", "\\\"") + "\"")));
                }
            }
        }
    }
}

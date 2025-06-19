using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Text.RegularExpressions;

namespace Greenhouse.Data.Repositories
{
    public class RedshiftRepository
    {
        protected static OdbcConnection OpenConnection(int timeOut = 30)
        {
            string connString = Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseRedshiftDbConnectionString;
            OdbcConnection conn = new OdbcConnection(connString);
            conn.ConnectionTimeout = timeOut;
            return conn;
        }

        public static string PrepareCommandText(string cmdText, IEnumerable<OdbcParameter> parameters)
        {
            string result = cmdText;

            foreach (var parameter in parameters)
            {
                // Replace '@fileguid' with 'abc123' if @fileguid was abc123 -- retain single quote identifier
                result = Regex.Replace(result, GetReplacePattern(parameter), parameter.Value == null ? "null" : $"'{Convert.ToString(parameter.Value)}'");

                // Replace @fileguid with abc123 -- no quoted identifer
                result = Regex.Replace(result, GetReplacePattern(parameter, true), $"{Convert.ToString(parameter.Value)}");
            }

            return result;
        }

        /// <summary>
        /// Regex pattern to replace odbc parameter in ETL Redshift Script with actual value, ie '@fileguid'
        /// Set "includeUnderscore" to TRUE when parameters should be replaced with no quoted identifier and allow preceding and trailing underscore (@fileguid_)
        /// (?< !')@(?<=_){p.ParameterName}\b(?!'): Matches the parameter name preceded by an underscore and not already enclosed in single quotes.
        /// (?< !')@\b{p.ParameterName}(?=_)(?!'): Matches the parameter name followed by an underscore and not already enclosed in single quotes.
        /// '@\b{p.ParameterName}\b': Matches the parameter name with word boundaries and single quotes.
        /// </summary>
        private static string GetReplacePattern(OdbcParameter p, bool includeUnderscore = false)
        {
            return includeUnderscore ? $@"(?<!')@(?<=_){p.ParameterName}\b(?!')|(?<!')@\b{p.ParameterName}(?=_)(?!')|@\b{p.ParameterName}\b" : $@"'@\b{p.ParameterName}\b'";
        }

        public static int ExecuteRedshiftCommand(string cmdText, int? timeOut = null)
        {
            int retVal = -1;
            using (var conn = timeOut.HasValue ? OpenConnection(timeOut.Value) : OpenConnection())
            {
                using (var cmd = new OdbcCommand(cmdText, conn))
                {
                    conn.Open();
                    if (timeOut.HasValue)
                    {
                        cmd.CommandTimeout = timeOut.Value;
                    }
                    cmd.CommandType = System.Data.CommandType.Text;
                    retVal = cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
            return retVal;
        }

        public static IDataReader ExecuteRedshiftDataReader(string cmdText)
        {
            var conn = OpenConnection();
            var cmd = new OdbcCommand(cmdText, conn);
            conn.Open();
            cmd.CommandType = System.Data.CommandType.Text;
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        public static IEnumerable<T> ExecuteRedshiftDataReader<T>(string cmdText, int timeOut = 30)
        {
            using (IDbConnection connection = OpenConnection(timeOut))
            {
                return connection.Query<T>(cmdText);
            }
        }

        public static List<string> GetUserIDs()
        {
            var users = new List<string>();

            string sql = $"select usename from pg_user;";

            using (var dataReader = ExecuteRedshiftDataReader(sql))
            {
                while (dataReader.Read())
                {
                    string userName = dataReader.GetString(0);
                    users.Add(userName);
                }
            }

            return users;
        }
    }
}
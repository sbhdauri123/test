using Dapper;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text.RegularExpressions;

namespace Greenhouse.Data.Model.Setup
{
    /// <summary>
    [Serializable]
    public partial class Credential : BasePOCO
    {
        public Credential() : this(null)
        {
        }

        public Credential(string encryptedConnString = null)
        {
            CredentialSet = new ExpandoObject();
            ConnectionString = encryptedConnString;
        }

        [Key]
        public int CredentialID { get; set; }

        public string CredentialName { get; set; }

        public int CredentialTypeID { get; set; }

        public bool IsActive { get; set; }

        //matches dynamic key-values
        private static readonly Regex _connStrRegex = ConnStrRegex();

        public dynamic CredentialSet { get; set; }

        private string _connectionString;
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                _connectionString = value;
                ParseInternal();
            }
        }

        [NotMapped]
        public string ConnectionStringDecrypted
        {
            get
            {
                return UtilsText.Decrypt(ConnectionString);
            }
            set
            {
                ConnectionString = UtilsText.Encrypt(value);
            }
        }

        private void ParseInternal()
        {
            if (!string.IsNullOrEmpty(ConnectionString) && _connStrRegex.IsMatch(ConnectionStringDecrypted))
            {
                var matches = _connStrRegex.Matches(ConnectionStringDecrypted);

                var p = CredentialSet as IDictionary<string, object>;
                if (p == null) // object returned from client is not easily casted into a dictionary
                    return;
                p.Clear();
                foreach (Match m in matches)
                {
                    if (!p.ContainsKey(m.Groups["key"].Value))
                        ((IDictionary<string, Object>)p).Add(m.Groups["key"].Value, m.Groups["value"].Value);
                }
            }
        }

        private static Dictionary<string, string> ParseConnectionString(string connString)
        {
            Dictionary<string, string> nameValues = new Dictionary<string, string>();
            var matches = _connStrRegex.Matches(connString);
            foreach (Match m in matches)
            {
                nameValues.Add(m.Groups["key"].Value, m.Groups["value"].Value);
            }
            return nameValues;
        }

        public static Credential GetGreenhouseAWSCredentialFromProfile(string profileName)
        {
            string connString;
            var profileCredential = Greenhouse.Configuration.Settings.GetAwsCredentialsUsingProfile(profileName)?.GetCredentials();
            if (profileCredential == null)
                throw new ArgumentNullException(nameof(profileName), string.Format("ProfileName Not found {0}", profileName));
            connString = string.Format("AccessKey={0};SecretKey={1};Region={2}", profileCredential.AccessKey, profileCredential.SecretKey, Greenhouse.Configuration.Settings.Current.AWS.Region);
            return new Credential(Utilities.UtilsText.Encrypt(connString));
        }
        public static Credential GetGreenhouseAWSCredential(string profileName = null)
        {
            var awsCred = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials();
            string connString;
            if (!string.IsNullOrEmpty(profileName))
            {
                var profileCredential = Greenhouse.Configuration.Settings.GetAwsCredentialsUsingProfile(profileName)?.GetCredentials();
                if (profileCredential != null)
                    awsCred = profileCredential;
            }
            connString = string.Format("AccessKey={0};SecretKey={1};Region={2}", awsCred.AccessKey, awsCred.SecretKey, Greenhouse.Configuration.Settings.Current.AWS.Region);
            return new Credential(Utilities.UtilsText.Encrypt(connString));
        }
        public static Credential GetGreenhouseAWSAssumeRoleCredential(Credential creds)
        {
            var awsCred = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials();
            string connString;
            connString = string.Format("AccessKey={0};SecretKey={1};Region={2};AuthorizedARN={3};PartnerARN={4};ExternalId={5}", awsCred.AccessKey,
            awsCred.SecretKey, Greenhouse.Configuration.Settings.Current.AWS.Region, creds.CredentialSet.AuthorizedARN, creds.CredentialSet.PartnerARN, creds.CredentialSet.ExternalId);
            return new Credential(Utilities.UtilsText.Encrypt(connString));
        }

        [GeneratedRegex("(?<key>[^=;]+)=(?<value>[^;]+);?")]
        private static partial Regex ConnStrRegex();
    }
}
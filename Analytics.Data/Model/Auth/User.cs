using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Greenhouse.Data.Model.Auth
{
    [Serializable]
    public class User : Base, System.Security.Principal.IIdentity
    {
        public User()
        {
            UserInstances = new UserGroups();
            UserClients = new UserGroups();
            UserLineOfBusinesses = new UserGroups();
            //AccessibleInstances = new Instances();
            AppAccessInfo = new UserAppAccessInfo();
            UserOptions = new UsersOptions();
        }

        #region IIdentity Members
        public string AuthenticationType
        {
            get { return "FormsAuthentication"; }
        }

        public bool IsAuthenticated { get; set; }
        public string Name { get; set; }

        #endregion

        public string Uid { get; set; }
        public string UserPassword { get; set; }
        public string Surname { get; set; }
        public string DisplayName { get; set; }
        public string UserRole { get; set; }
        public bool IsActive { get; set; }
        public bool CanApproveQVApps { get; set; }
        public bool CanCreateQVServers { get; set; }
        public bool CanPublishQVApps { get; set; }
        public string CountryIso { get; set; }
        public string LanguageCode { get; set; }
        public string PivotUsername { get; set; }
        public string GlobalAppIds { get; set; }
        public DateTime? CreatedDate { get; set; }
        public DateTime? LastUpdated { get; set; }
        [JsonConverter(typeof(JsonCustomConverter<UserGroup>))]
        public UserGroups UserInstances { get; set; }
        [JsonConverter(typeof(JsonCustomConverter<UserGroup>))]
        public UserGroups UserClients { get; set; }
        [JsonConverter(typeof(JsonCustomConverter<UserGroup>))]
        public UserGroups UserLineOfBusinesses { get; set; }
        //public Instances AccessibleInstances { get; set; }
        public bool HasDatabaseAccess { get; set; }
        public UserAppAccessInfo AppAccessInfo { get; set; }
        public UsersOptions UserOptions { get; set; }

        public System.Globalization.CultureInfo UserCulture
        {
            get
            {
                string cultureCode = string.Format("{0}-{1}", this.LanguageCode.ToLower(), this.CountryIso.ToUpper());

                var isValidCulture = System.Globalization.CultureInfo
                .GetCultures(System.Globalization.CultureTypes.SpecificCultures)
                .Any(c => c.Name == cultureCode);

                if (isValidCulture)
                    return new System.Globalization.CultureInfo(cultureCode);
                else
                    return new System.Globalization.CultureInfo("en-US");
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            Type t = this.GetType();
            System.Reflection.PropertyInfo[] pis = t.GetProperties();
            for (int i = 0; i < pis.Length; i++)
            {
                System.Reflection.PropertyInfo pi = (System.Reflection.PropertyInfo)pis.GetValue(i);
                sb.AppendFormat("{0}: {1}, ", pi.Name, pi.GetValue(this, Array.Empty<object>()));
            }

            return sb.ToString();
        }
    }

    [Serializable]
    public class Users : List<User>
    {
        public Users() : base() { }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            foreach (object obj in this)
            {
                sb.Append(obj.ToString()).Append(Environment.NewLine);
            }

            return sb.ToString();
        }
    }

    [Serializable]
    public class UserAppRole
    {
        [Newtonsoft.Json.JsonPropertyAttribute("A")]
        public string ApplicationId { get; set; }
        [Newtonsoft.Json.JsonPropertyAttribute("R")]
        public string RoleId { get; set; }
        [Newtonsoft.Json.JsonPropertyAttribute("I")]
        public string InstanceId { get; set; }
    }

    [Serializable]
    public class UserAppAccessInfo
    {
        [Newtonsoft.Json.JsonPropertyAttribute("Sec")]
        [JsonConverter(typeof(JsonCustomConverter<UserAppRole>))]
        public List<UserAppRole> AppRoleMapping { get; set; }
    }

    [Serializable]
    public class UserOption
    {
        public bool ReceiveGlanceAnnoucements { get; set; }
        public bool ReceiveFailureEmails { get; set; }
        public bool ResetPassword { get; set; }
        public bool ClientUser { get; set; }
        public string InstanceId { get; set; }
    }

    [Serializable]

    public class UsersOptions
    {
        [JsonConverter(typeof(JsonCustomConverter<UserOption>))]
        public List<UserOption> UserOptions { get; set; }
    }

    public class JsonCustomConverter<T> : JsonConverter
    {
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            object retVal = new object();
            if (reader.TokenType == JsonToken.StartObject)
            {
                if (typeof(T) == typeof(UserGroup))
                {
                    var instance = (UserGroup)serializer.Deserialize(reader, typeof(UserGroup));
                    retVal = new UserGroups { instance };
                }
                else
                {
                    T instance = (T)serializer.Deserialize(reader, typeof(T));
                    retVal = new List<T> { instance };
                }
            }
            else if (reader.TokenType == JsonToken.StartArray)
            {
                retVal = serializer.Deserialize(reader, objectType);
            }
            return retVal;
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(T).IsAssignableFrom(objectType);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
        }
    }
}

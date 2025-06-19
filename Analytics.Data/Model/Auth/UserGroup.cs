using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Greenhouse.Data.Model.Auth
{
    [Serializable]
    public class UserGroup : Base
    {
        private static readonly Dictionary<GroupType, string> GroupObjectClassMappings = new Dictionary<GroupType, string>();

        static UserGroup()
        {
            GroupObjectClassMappings.Add(GroupType.ALL, "groupOfUniqueNames");
            GroupObjectClassMappings.Add(GroupType.CLIENT, "glanceClient");
            GroupObjectClassMappings.Add(GroupType.INSTANCE, "glanceInstance");
            GroupObjectClassMappings.Add(GroupType.LOB, "glanceLOB");
        }
        public enum GroupType
        {
            ALL,
            INSTANCE,
            CLIENT,
            LOB
        }

        public IDictionary<string, string[]> Attributes { get; set; }

        public GroupType UserGroupType { get; set; }
        public string ObjectClass
        {
            get
            {
                return UserGroup.GetObjectClass(UserGroupType);
            }
        }

        public static string GetObjectClass(GroupType groupType)
        {
            return GroupObjectClassMappings[groupType];
        }

        public static GroupType GetGroupType(string objectClass)
        {
            return (from item in GroupObjectClassMappings where item.Value == objectClass select item.Key).FirstOrDefault<GroupType>();
        }

        public UserGroup() { }

        public UserGroup(string dn, System.DirectoryServices.Protocols.SearchResultAttributeCollection attributes)
        {
            this.Attributes = new Dictionary<string, string[]>();
            //col.AddRange(attributes);

            foreach (DictionaryEntry de in attributes)
            {
                string key = de.Key.ToString();
                this.Attributes.Add(key, attributes[key].GetValues(typeof(string)) as string[]);
            }

            this.CommonName = attributes[Greenhouse.Common.Constants.COMMON_NAME][0] as string;
            this.DistinguishedName = dn;
            for (int i = 0; i < attributes[Greenhouse.Common.Constants.OBJECT_CLASS].Count; i++)
            {
                if (attributes[Greenhouse.Common.Constants.OBJECT_CLASS][i].ToString().Contains("glance"))
                {
                    this.UserGroupType = UserGroup.GetGroupType(attributes[Greenhouse.Common.Constants.OBJECT_CLASS][i].ToString());
                    break;
                }
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
    public class UserGroups : List<UserGroup>
    {
        public UserGroups() : base() { }

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
}

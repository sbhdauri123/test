using Greenhouse.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Greenhouse.Data.Model.Auth
{
    [Serializable]
    public abstract class Base
    {
        public string CommonName { get; set; }
        [System.Xml.Serialization.XmlIgnore]
        public string DistinguishedName { get; set; }

        [System.Xml.Serialization.XmlIgnore]
        public string QVIdentifier
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                Dictionary<string, string> cns = new Dictionary<string, string>();

                if (!string.IsNullOrEmpty(DistinguishedName))
                {
                    string[] listOfCNames = DistinguishedName.Split(',');

                    for (int i = (listOfCNames.Length - 1); i >= 0; i--)
                    {
                        if (listOfCNames[i].Equals("cn=groups", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (listOfCNames[i].Equals("cn=glance", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (listOfCNames[i].Equals("cn=users", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        if (listOfCNames[i].Contains("cn=", StringComparison.InvariantCultureIgnoreCase))
                        {
                            string[] splitCName = listOfCNames[i].Split('=');
                            string name = splitCName.AsEnumerable().Last();

                            if (cns.TryAdd(name, ""))
                            {
                                if (cns.Count > 0)
                                    sb.Append('|');

                                sb.Append(name);
                            }
                        }
                    }
                }

                return sb.ToString();
            }

            private set
            {
            }
        }

        internal string GetParent(int depth)
        {
            string parent = string.Empty;
            if (this.DistinguishedName != null && this.DistinguishedName != string.Empty)
            {
                string[] nodes = this.DistinguishedName.Split(Constants.COMMA_ARRAY);
                if (depth <= nodes.Length)
                {
                    nodes = nodes[depth].Split(Constants.EQAULS_ARRAY);
                    parent = nodes[1];
                }
            }
            return parent;
        }
    }
}

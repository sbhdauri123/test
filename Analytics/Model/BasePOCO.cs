using System;
using System.Text;

namespace Greenhouse.Data.Model
{
    [Serializable]
    public abstract class BasePOCO
    {
        protected BasePOCO()
        {
            this.CreatedDate = DateTime.UtcNow;
            this.LastUpdated = DateTime.UtcNow;
        }

        public DateTime CreatedDate { get; set; }
        public DateTime LastUpdated { get; set; }

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
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.Net.Http;

namespace froniuslocalcollector {
    class InfluxWriter {
        static readonly HttpClient client = new HttpClient();
        readonly string measurement;
        readonly string server;
        readonly string database;
        static readonly CultureInfo decimaldotculture = CultureInfo.InvariantCulture;
        static readonly long unixbase = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).ToFileTime();
        public InfluxWriter(string server, string database, string measurement)
        {
            this.measurement = measurement;
            this.server = server;
            this.database = database;
        }

        private static string influxescape(string data)
        {
            return data.Replace("\\", "\\\\").Replace(" ", "\\ ").Replace(",", "\\,").Replace("=", "\\=");
        }

        private static string influxvalue(object value)
        {
            if (value is int i) {
                return $"{i}i";
            } else if (value is double d) {
                return d.ToString("F", decimaldotculture.NumberFormat);
            } else {
                return $"\"{value.ToString().Replace("\"", "\\\"")}\"";
            }
        }
        public void Write(DateTime timestamp, Dictionary<string, object> values)
        {
            string linedata = $"{measurement} {string.Join(",", values.Select(kvp => $"{influxescape(kvp.Key)}={influxvalue(kvp.Value)}"))} {(timestamp.ToFileTime() - unixbase) / 10000}";
            HttpContent content = new StringContent(linedata);
            client.PostAsync($"http://{server}:8086/write?db={database}&precision=ms", content);
        }
    }
}

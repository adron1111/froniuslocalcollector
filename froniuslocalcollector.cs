using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http;

namespace froniuslocalcollector {
    public class FroniusConfigSection : ConfigurationSection {
        [ConfigurationProperty("host", IsRequired = true)]
        public string FroniusHost => this["host"] as string;

        [ConfigurationProperty("device", IsRequired = true)]
        public int FroniusDevice => (int)this["device"];

        [ConfigurationProperty("influxHost", IsRequired = true)]
        public string InfluxHost => this["influxHost"] as string;

        [ConfigurationProperty("influxDatabase", IsRequired = true)]
        public string InfluxDatabase => this["influxDatabase"] as string;

        [ConfigurationProperty("influxMeasurement", IsRequired = true)]
        public string InfluxMeasurement => this["influxMeasurement"] as string;
    }

    class Program {

#pragma warning disable CS0649
        private class UnitAndValue {
            public string Unit;
            public double Value;
        }

        private class CommonInverterData {
            public UnitAndValue DAY_ENERGY;
            public UnitAndValue FAC;
            public UnitAndValue IAC;
            public UnitAndValue IDC;
            public UnitAndValue PAC;
            public UnitAndValue TOTAL_ENERGY;
            public UnitAndValue UAC;
            public UnitAndValue UDC;
            public UnitAndValue YEAR_ENERGY;
        }

        private class RequestArguments {
            public string DataCollection;
            public string DeviceClass;
            public string DeviceId;
            public string Scope;
        }

        private class Status {
            public int Code;
            public string Reason;
            public string UserMessage;
        }

        private class HeadPart {
            public RequestArguments RequestArguments;
            public Status Status;
            public string Timestamp;
        }

        private class BodyPart<T> {
            public T Data;
        }

        private class InverterRealtimeData<T> {
            public BodyPart<T> Body;
            public HeadPart Head;
        }
#pragma warning restore CS0649

        static async Task Main(string[] args)
        {
            Configuration execonfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            FroniusConfigSection config = execonfig.GetSection("fronius") as FroniusConfigSection;

            if (string.IsNullOrEmpty(config.InfluxDatabase) || string.IsNullOrEmpty(config.InfluxHost) || string.IsNullOrEmpty(config.InfluxMeasurement)) {
                Console.WriteLine("Invalid database parameters");
                return;
            }
            if (string.IsNullOrEmpty(config.FroniusHost) || config.FroniusDevice < 0) {
                Console.WriteLine("Invalid pulse parameters");
                return;
            }

            InfluxWriter influx = new InfluxWriter(config.InfluxHost, config.InfluxDatabase, config.InfluxMeasurement);

            string commoninverterdataurl = $"http://{config.FroniusHost}/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId={config.FroniusDevice}&DataCollection=CommonInverterData";

            var cancelall = new CancellationTokenSource();
            var softexiting = new CancellationTokenSource();


            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                softexiting.Cancel();
                cancelall.CancelAfter(10000);
            };

            CancellationTokenSource timeout;
            double timeadjustment = 0;

            HttpClient httpClient = new HttpClient();

            while (!softexiting.IsCancellationRequested)
                using (timeout = CancellationTokenSource.CreateLinkedTokenSource(cancelall.Token))
                    try {
                        while (!softexiting.IsCancellationRequested) {
                            DateTime targetread = DateTime.Now;
                            // read every 15 seconds
                            targetread = new DateTime(targetread.Year, targetread.Month, targetread.Day, targetread.Hour, targetread.Minute, (targetread.Second / 15) * 15).AddSeconds(15);
                            DateTime adjustedtargetread = targetread.AddSeconds(timeadjustment);
                            await Task.Delay(adjustedtargetread - DateTime.Now, softexiting.Token);
                            string json = null;
                            try {
                                timeout.CancelAfter(10000);
                                var request = new HttpRequestMessage(HttpMethod.Get, commoninverterdataurl);
                                var response = await httpClient.SendAsync(request, timeout.Token);
                                json = await response.Content.ReadAsStringAsync();
                                timeout.CancelAfter(120000);
                            }
                            catch (HttpRequestException hre) {
                                Console.WriteLine($"Exception '{hre.Message}' retrieving CommonInverterData");
                            }

                            var data = JsonConvert.DeserializeObject<InverterRealtimeData<CommonInverterData>>(json);
                            Dictionary<string, object> values = new Dictionary<string, object>();
                            if(data.Body.Data.PAC != null)
                                values.Add("powerAC", data.Body.Data.PAC.Value);
                            if (data.Body.Data.UDC != null)
                                values.Add("voltageDC", data.Body.Data.UDC.Value);
                            if (data.Body.Data.IDC != null)
                                values.Add("currentDC", data.Body.Data.IDC.Value);
                            if (data.Body.Data.DAY_ENERGY != null)
                                values.Add("accumulatedDaily", data.Body.Data.DAY_ENERGY.Value);
                            DateTime timestamp = DateTime.Parse(data.Head.Timestamp);
                            timeadjustment += (targetread - timestamp).TotalSeconds;
                            if (Math.Abs(timeadjustment) >= 15)
                                timeadjustment -= Math.Truncate(timeadjustment / 15) * 15;
                            //Console.WriteLine($"Time adjustment: {timeadjustment}");
                            Console.Write($"{timestamp}: Power: {data.Body.Data.PAC?.Value.ToString() ?? "<null>"}, DC Voltage: {data.Body.Data.UDC?.Value.ToString() ?? "<null>"}, Daily Energy: {data.Body.Data.DAY_ENERGY?.Value.ToString() ?? "<null>"}          \r");
                            if(values.Count > 0) 
                                influx.Write(timestamp, values);
                        }
                    }
                    catch (OperationCanceledException) {
                        Console.WriteLine("Operation cancelled");
                    }
                    catch (Exception ex) {
                        Console.WriteLine($"Exception: '{ex.Message}', retrying");
                    }



        }
    }
}

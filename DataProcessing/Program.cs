/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.Configuration;
using System;
using System.Globalization;
using System.IO;

namespace QuantConnect.DataProcessing
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var tempOutputDirectory =
                Directory.CreateDirectory(Config.Get("temp-output-directory", "/temp-output-directory"));
            var processedDataFolder =
                Directory.CreateDirectory(Config.Get("processed-data-directory", Globals.DataFolder));

            var overwriteExistingEntries = Config.GetBool("overwrite-existing-entries");
            var outputVendorDirectory = Config.Get("output-vendor-directory", "vixcentral");

            var deploymentDateValue = Environment.GetEnvironmentVariable("QC_DATAFLEET_DEPLOYMENT_DATE");
            var deploymentDate = deploymentDateValue != null
                ? Parse.DateTimeExact(deploymentDateValue, "yyyyMMdd", DateTimeStyles.None)
                : DateTime.UtcNow.Date;

            var processStartDateValue = Config.Get("process-start-date", null);
            var processStartDate = processStartDateValue != null
                ? Parse.DateTimeExact(processStartDateValue, "yyyyMMdd", DateTimeStyles.None)
                : deploymentDate.AddDays(-1);

            var processor = new VIXContangoProcessor(
                tempOutputDirectory,
                processedDataFolder,
                processStartDate,
                deploymentDate,
                outputVendorDirectory,
                overwriteExistingEntries);

            processor.Process();
        }
    }
}

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

using QuantConnect.DataSource;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using QuantConnect.Data.Market;
using QuantConnect.Securities;
using QuantConnect.Securities.Future;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    public class VIXContangoProcessor
    {
        private const string _futuresBaseUrl = "https://www.cboe.com/us/futures/market_statistics/historical_data/products/csv/";
        private const string _userAgent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0";
        private const string _csvHeader = "Date,First Month,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,Contango 2/1,Contango 7/4,Con 7/4 div 3";

        private readonly DateTime _utcDate;
        private readonly DateTime _startDate;
        private readonly RateGate _rateGate;
        
        private readonly DirectoryInfo _tempOutputDirectory;
        private readonly DirectoryInfo _existingDataDirectory;
        private readonly DateTime _deploymentDate;
        private readonly string _outputVendorDirectoryName;
        private readonly bool _overwriteExistingData;
        private readonly bool _processOnlyDeploymentDateData;
        
        /// <summary>
        /// Creates a new instance of the VIX contango processor
        /// </summary>
        /// <param name="startDate">Starting date to process data from</param>
        /// <param name="baseOutputDirectory">Base output directory, e.g. /temp-output-directory</param>
        /// <param name="existingDataDirectory">Existing data directory, e.g. /Data</param>
        /// <param name="deploymentDate">The date that we're processing data for</param>
        /// <param name="outputVendorDirectoryName">Vendor directory name to write to inside the `{{baseOutputDirectory}}/alternative/` directory</param>
        /// <param name="overwriteExistingData">If true, we overwrite existing data with newly generated data</param>
        /// <param name="processOnlyDeploymentDateData">
        /// If true, will consider all data with any date for inclusion. Otherwise, only the processing
        /// date's data with be considered for inclusion into the data set.
        /// </param>
        public VIXContangoProcessor(
            DirectoryInfo baseOutputDirectory,
            DirectoryInfo existingDataDirectory,
            DateTime deploymentDate,
            string outputVendorDirectoryName,
            bool overwriteExistingData,
            bool processOnlyDeploymentDateData)
        {
            // To prevent ourselves from generating incorrect data, we need to load
            // data for contract months that came before the current front month contract.
            //
            // This is an example scenario that would corrupt the data if no contracts
            // before the current front month contract were downloaded/included:
            //
            // (futures contract month code reference: https://www.cmegroup.com/month-codes.html)
            //
            // * Processing/current date is 2021-02-20
            // * VXG1 expired on 2021-02-10
            // * Get settlement price data for VX chain (minimum 12 contracts because the Lean data source object has settlement prices for the first 12 contracts in the chain, so we need to make sure all data is considered)
            // * VXH1 settlement price data starts on 2020-05-01, same with other contracts
            // * Settlement data begins at 2020-05-01, so we start adding those data into a collection
            // * Fast-forward settlement data processing from 2020-05-01 to 2021-02-05
            // * On 2021-02-05, we think that VXH1 is the front month contract because we only downloaded data for VXH1 and after
            // * VXG1 is the actual front month contract
            // * We would write:        VXH1 settlement price data to contango.F1, VXJ1 to contango.F2, etc.
            // * It should actually be: VXG1 settlement price data to contango.F1, VXH1 to contango.F2, VXJ1 to contango.F3, etc.
            //
            // So on a date like 2021-02-05, we think that VXH1 is the front-month contract, when in fact it should be VXG1 if we don't download data from before, so it ends up corrupting the data/producing bad results.
            // We subtract two months from the current deployment date as a safe bet to ensure that there is sufficient room for warmup.
            _startDate = deploymentDate.AddMonths(-2);
            _deploymentDate = deploymentDate;

            _utcDate = DateTime.UtcNow.Date;
            _rateGate = new RateGate(1, TimeSpan.FromSeconds(5));

            _tempOutputDirectory = baseOutputDirectory;
            _existingDataDirectory = existingDataDirectory;
            _outputVendorDirectoryName = outputVendorDirectoryName;
            _overwriteExistingData = overwriteExistingData;
            _processOnlyDeploymentDateData = processOnlyDeploymentDateData;
        }

        /// <summary>
        /// Starts downloading and processing data
        /// </summary>
        public void Process()
        {
            var vxFutures = GetFutureChain();
            var vxCloses = DownloadFuturesSettlement(vxFutures);
            var contango = GetContango(vxCloses, vxFutures);

            WriteToFile(contango);
        }
        
        /// <summary>
        /// Gets the expiry function for the future matching the ticker provided
        /// </summary>
        /// <param name="ticker">Ticker of the future</param>
        /// <param name="market">Market</param>
        /// <returns>Expiry function</returns>
        /// <exception cref="Exception">Market or expiry function was not found</exception>
        public Func<DateTime, DateTime> GetExpiryFunc(string ticker, out string market)
        {
            market = GetFutureMarket(ticker);

            var future = Symbol.Create(ticker, SecurityType.Future, market);
            if (!FuturesExpiryFunctions.FuturesExpiryDictionary.TryGetValue(future, out var expiryFunc))
            {
                throw new Exception($"{ticker} expiry function not found with market: {market}");
            }

            return expiryFunc;
        }

        /// <summary>
        /// Gets the market of the provided future Symbol
        /// </summary>
        /// <param name="ticker">Ticker of the future</param>
        /// <returns>Market of the future</returns>
        /// <exception cref="Exception">Market not found in SPDB</exception>
        /// <remarks>This is made into its own method for overriding in unit tests</remarks>
        protected virtual string GetFutureMarket(string ticker)
        {
            var spdb = SymbolPropertiesDatabase.FromDataFolder();
            if (!spdb.TryGetMarket(ticker, SecurityType.Future, out var market))
            {
                throw new Exception($"No market found in SPDB for future: {ticker}");
            }

            return market;
        }

        /// <summary>
        /// Gets all of the futures Symbols that we will calculate contango for
        /// </summary>
        /// <returns>List of futures Symbols with expiry attached to them, ordered by expiry</returns>
        public List<Symbol> GetFutureChain()
        {
            var currentTime = _startDate;

            var expiries = new HashSet<DateTime>();
            var expiriesAfterToday = 0;
            var expiryFunc = GetExpiryFunc("VX", out var market);
            
            // We only want to at most load 12 contracts from today's date, since
            // it's very unlikely CBOE will have any more contracts than that.
            while (expiriesAfterToday != 12)
            {
                var expiryTime = expiryFunc(currentTime).Date;
                if (expiryTime < _startDate)
                {
                    // The contract has already expired, we need to keep searching
                    // for a contract that hasn't expired yet and start from there.
                    currentTime += TimeSpan.FromDays(1);
                    continue;
                }

                if (expiries.Add(expiryTime))
                {
                    Log.Trace($"VIXContangoProcessor.GetFutureChain(): Including expiry: {expiryTime:yyyy-MM-dd} for VX contango calculation");
                    if (expiryTime >= _utcDate)
                    {
                        // Only add at most 12 contracts past today's date's expiries,
                        // since CBOE won't have data available past 12 contracts in the
                        // best case scenario.
                        expiriesAfterToday++;
                    }
                }

                currentTime += TimeSpan.FromDays(1);
            }

            return expiries
                .OrderBy(x => x)
                .Select(x => Symbol.CreateFuture("VX", market, x))
                .ToList();
        }

        /// <summary>
        /// Downloads the historical settlement prices for the futures provided
        /// </summary>
        /// <param name="symbols">Symbols to download historical settlement price data for</param>
        /// <returns>Dictionary, keyed by trading date, containing a list of futures bars that had data in that day</returns>
        /// <exception cref="Exception">Max retries exceeded or invalid response encountered</exception>
        public Dictionary<DateTime, List<TradeBar>> DownloadFuturesSettlement(List<Symbol> symbols)
        {
            var vxClosesByDate = new Dictionary<DateTime, List<TradeBar>>();
            
            using var client = new HttpClient();
            client.DefaultRequestHeaders.UserAgent.ParseAdd(_userAgent);
            
            foreach (var symbol in symbols)
            {
                var url = $"{_futuresBaseUrl}/{symbol.ID.Symbol.ToUpperInvariant()}/{symbol.ID.Date.Date:yyyy-MM-dd}/";
                
                string result = null;
                for (var retry = 1; retry <= 5; retry++)
                {
                    try
                    {
                        // Impose a rate limit on ourselves to ease load on web server
                        _rateGate.WaitToProceed();

                        result = DownloadCBOESettlementData(client, url);
                        Log.Trace($"VIXContangoProcessor.DownloadFuturesSettlement(): Downloaded data for {symbol} (expiry: {symbol.ID.Date:yyyy-MM-dd} - attempt {retry}/5)");
                        break;
                    }
                    catch (HttpRequestException err)
                    {
                        if (err.StatusCode == HttpStatusCode.NotFound)
                        {
                            // We've finished downloading all of the data we can get right now, so
                            // let's return and begin processing.
                            Log.Trace($"VIXContangoProcessor.DownloadFuturesClose(): Data for {symbol.ID.Symbol} with expiry: {symbol.ID.Date.Date:yyyy-MM-dd} is too far into the future and does not exist. All available data has been downloaded and will now begin processing.");
                            return vxClosesByDate;
                        }

                        if (retry == 5)
                        {
                            throw new Exception("Max retries exceeded (5/5)");
                        }
                    }
                }

                if (result == null)
                {
                    throw new Exception($"Result returned is null for Symbol: {symbol}");
                }

                // Just in case, remove \r characters before we split for line breaks
                var lines = result
                    .Replace("\r", "")
                    .Split('\n');
                
                foreach (var line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line) || !char.IsNumber(line.FirstOrDefault()))
                    {
                        // We're dealing with the header of the CSV or the newline at the end of the file
                        continue;
                    }
                    // Column order is, and historically has been:
                    // Trade Date,Futures,Open,High,Low,Close,Settle,Change,Total Volume,EFP,Open Interest
                    var csv = line.Split(',');
                    if (csv.Length < 7)
                    {
                        continue;
                    }
                    
                    var i = 0;
                    var date = Parse.DateTimeExact(csv[i++], "yyyy-MM-dd");
                    if (date >= symbol.ID.Date.Date)
                    {
                        // VIXCentral excludes the final settlement of VIX in their
                        // outputted data, so to match the dataset 1:1, we'll want to
                        // exclude the final trading day as well.
                        continue;
                    }
                    
                    i++; // Skip "Futures" column, we already know which future we're parsing this data for.
                    var open = Parse.Decimal(csv[i++], NumberStyles.Any);
                    var high = Parse.Decimal(csv[i++], NumberStyles.Any);
                    var low = Parse.Decimal(csv[i++], NumberStyles.Any);
                    i++; // Skip "close" price since it can be zero. Instead, we use the settlement price for the close.
                    var settle = Parse.Decimal(csv[i++], NumberStyles.Any);

                    if (!vxClosesByDate.TryGetValue(date, out var bars))
                    {
                        bars = new List<TradeBar>();
                        vxClosesByDate[date] = bars;
                    }
                    
                    bars.Add(new TradeBar(date, symbol, open, high, low, settle, 0m, TimeSpan.FromDays(1)));
                }
            }

            return vxClosesByDate;
        }

        /// <summary>
        /// Downloads CBOE settlement data.
        /// </summary>
        /// <param name="client">HTTP client</param>
        /// <param name="url">URL to query</param>
        /// <returns>Data as a string</returns>
        /// <remarks>Made virtual for unit testing purposes</remarks>
        protected virtual string DownloadCBOESettlementData(HttpClient client, string url)
        {
            return client.GetStringAsync(url).SynchronouslyAwaitTaskResult();
        }

        /// <summary>
        /// Calculates and gets a list of Contango bars for each trading day
        /// </summary>
        /// <param name="futuresBars">Dictionary of futures traded on a given trading day</param>
        /// <param name="futureChain">The symbols that we downloaded data for</param>
        /// <returns>A list of calculated VIX contango, using the same formula VIXCentral uses</returns>
        public List<VIXCentralContango> GetContango(
            Dictionary<DateTime, List<TradeBar>> futuresBars,
            List<Symbol> futureChain)
        {
            if (futureChain.Count == 0)
            {
                return new List<VIXCentralContango>();
            }
            
            var vixContango = new List<VIXCentralContango>();
            // Get the previous expiry to figure out when we should start
            // generating data to be outputted, otherwise we would create data
            // without taking into account the previous month's expiry.
            var previousExpiry = GetPreviousChainExpiry(futureChain);

            foreach (var kvp in futuresBars)
            {
                var date = kvp.Key;
                var vxBars = kvp.Value;
                
                if (vxBars.Count < 8 || date < previousExpiry)
                {
                    // Eight bars is the absolute minimum number
                    // of bars that we expect in our data source.
                    continue;
                }

                vxBars = vxBars
                    .OrderBy(x => x.Symbol.ID.Date)
                    .ToList();
                
                var contango = new VIXCentralContango();

                contango.FrontMonth = vxBars[0]
                    .Symbol
                    .ID
                    .Date
                    .Month;

                var i = 0;
                contango.F1 = vxBars[i++].Close;
                contango.F2 = vxBars[i++].Close;
                contango.F3 = vxBars[i++].Close;
                contango.F4 = vxBars[i++].Close;
                contango.F5 = vxBars[i++].Close;
                contango.F6 = vxBars[i++].Close;
                contango.F7 = vxBars[i++].Close;
                contango.F8 = vxBars[i++].Close;
                // Since the rest of these fields are nullable, they can potentially be missing
                // and would result in an out-of-bounds exception if we tried accessing directly.
                contango.F9 = vxBars.Count >= 9
                    ? vxBars[i++].Close
                    : null;
                contango.F10 = vxBars.Count >= 10
                    ? vxBars[i++].Close
                    : null;
                contango.F11 = vxBars.Count >= 11
                    ? vxBars[i++].Close
                    : null;
                contango.F12 = vxBars.Count >= 12
                    ? vxBars[i++].Close
                    : null;

                // Contango calculations, as applied by VIXCentral
                contango.Contango_F2_Minus_F1 = (contango.F2 - contango.F1) / contango.F1;
                contango.Contango_F7_Minus_F4 = (contango.F7 - contango.F4) / contango.F4;
                contango.Contango_F7_Minus_F4_Div_3 = contango.Contango_F7_Minus_F4 / 3m;
                contango.Time = date;
                
                vixContango.Add(contango);
            }

            return vixContango;
        }

        /// <summary>
        /// Gets the previous expiry in the future chain
        /// </summary>
        /// <param name="futureChain">The current future chain</param>
        /// <returns>The datetime of the previous expiry</returns>
        /// <remarks>This is made into its own method for overriding in unit tests</remarks>
        protected virtual DateTime GetPreviousChainExpiry(List<Symbol> futureChain)
        {
            var frontMonthSymbol = futureChain[0];
            var expiryFunc = FuturesExpiryFunctions.FuturesExpiryFunction(frontMonthSymbol.Canonical);
            return expiryFunc(frontMonthSymbol.ID.Date.AddMonths(-1));
        }

        /// <summary>
        /// Writes the contango data to the provided output directory
        /// </summary>
        /// <param name="contangoEntries">Contango data</param>
        public void WriteToFile(List<VIXCentralContango> contangoEntries)
        {
            var lines = new List<string>
            {
                // CSV header required in output for backwards compatibility
                _csvHeader
            };
            
            // For backwards compatibility, we write to two different files with identical contents
            var outputDirectory = Directory.CreateDirectory(Path.Combine(_tempOutputDirectory.FullName, "alternative", _outputVendorDirectoryName));
            var outputFilePath1 = Path.Combine(outputDirectory.FullName, "vix_contago.csv");
            var outputFilePath2 = Path.Combine(outputDirectory.FullName, "vix_contango.csv");
            
            // Read from the first file we find (if any)
            var existingDataDirectoryPath = Path.Combine(_existingDataDirectory.FullName, "alternative", _outputVendorDirectoryName);
            var existingDataPath1 = Path.Combine(existingDataDirectoryPath, "vix_contango.csv");
            var existingDataPath2 = Path.Combine(existingDataDirectoryPath, "vix_contago.csv");
            
            var outputData = new Dictionary<DateTime, string>();
            
            if (File.Exists(existingDataPath1))
            {
                Log.Trace($"VIXContangoProcessor.WriteToFile(): Reading existing data from: {existingDataPath1}");
                outputData = ReadExistingData(existingDataPath1);
            }
            else if (File.Exists(existingDataPath2))
            {
                Log.Trace($"VIXContangoProcessor.WriteToFile(): Reading existing data from: {existingDataPath2}");
                outputData = ReadExistingData(existingDataPath2);
            }
            else
            {
                Log.Trace($"VIXContangoProcessor.WriteToFile(): Creating data for the first time in {outputFilePath1} and {outputFilePath2}");
            }

            foreach (var contango in contangoEntries)
            {
                if (!_overwriteExistingData && outputData.ContainsKey(contango.Time))
                {
                    // Data already exists for this day, and this function is
                    // configured to only append/add missing data, so we skip.
                    continue;
                }
                if (_processOnlyDeploymentDateData && contango.Time.Date != _deploymentDate)
                {
                    // The time of the data does not match up with the deployment date, and
                    // since we want to only add data for the deployment date, then we'll
                    // skip and leave everything else as-is, only updating a single data
                    // point if there's new data and we're either adding new data or 
                    // have configured this to overwrite any existing data.
                    continue;
                }
                
                outputData[contango.Time] = string.Join(",", 
                    contango.Time.ToStringInvariant("yyyy-MM-dd"),
                    contango.FrontMonth,
                    contango.F1,
                    contango.F2,
                    contango.F3,
                    contango.F4,
                    contango.F5,
                    contango.F6,
                    contango.F7,
                    contango.F8,
                    contango.F9,
                    contango.F10,
                    contango.F11,
                    contango.F12,
                    // For maximum compatibility with VIXCentral, calculated digits are rounded to 4 decimal points
                    Math.Round(contango.Contango_F2_Minus_F1, 4, MidpointRounding.AwayFromZero),
                    Math.Round(contango.Contango_F7_Minus_F4, 4, MidpointRounding.AwayFromZero),
                    Math.Round(contango.Contango_F7_Minus_F4_Div_3, 4, MidpointRounding.AwayFromZero));
            }

            // Finalize ordering of data and output format
            lines.AddRange(outputData.OrderBy(kvp => kvp.Key).Select(kvp => kvp.Value));
            
            // Write to both files; we're done here.
            File.WriteAllText(outputFilePath1, string.Join("\n", lines));
            File.WriteAllText(outputFilePath2, string.Join("\n", lines));
            
            Log.Trace($"VIXContangoProcessor.WriteToFile(): Finished writing data to: {outputFilePath1} and {outputFilePath2}");
        }
        
        /// <summary>
        /// Reads existing contango data from the provided path
        /// </summary>
        /// <param name="filePath">File path to read contango data from</param>
        /// <returns>Dictionary keyed by trading date with value being the line itself</returns>
        public static Dictionary<DateTime, string> ReadExistingData(string filePath)
        {
            return File.ReadAllLines(filePath)
                .Where(x => !string.IsNullOrWhiteSpace(x) && char.IsNumber(x.FirstOrDefault()))
                .ToDictionary(
                    x => Parse.DateTimeExact(x.Split(',')[0], "yyyy-MM-dd", DateTimeStyles.None),
                    x => x);
        }
    }
}
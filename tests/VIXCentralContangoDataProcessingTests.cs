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
 *
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using NUnit.Framework;
using QuantConnect.Data.Market;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture, Category("TravisExclude")]
    public class VIXCentralContangoDataProcessingTests
    {
        [Test]
        public void FutureChainLoadedFromToday()
        {
            var now = DateTime.UtcNow.Date;
            var processor = new TestingVIXContangoProcessor(
                null,
                null,
                now,
                "contango",
                false,
                false);

            var chain = processor.GetFutureChain();
            
            // 12 contracts from today's front month contract
            Assert.AreEqual(12, chain.Count);
            Assert.IsTrue(chain.All(x => x.ID.Date >= now));
        }

        [Test]
        public void FutureChainLoadedFromPastDate()
        {
            var startDate = DateTime.UtcNow.Date.AddMonths(-3);
            var processor = new TestingVIXContangoProcessor(
                null,
                null,
                startDate,
                "contango",
                false,
                false);

            var chain = processor.GetFutureChain();
            
            Assert.AreNotEqual(12, chain.Count);
            Assert.Greater(chain.Count, 12);
            Assert.IsFalse(chain.All(x => x.ID.Date >= DateTime.UtcNow.Date));
        }

        [TestCase(0, false, true)]
        [TestCase(1, false, true)]
        [TestCase(2, false, true)]
        [TestCase(10, false, false)]
        [TestCase(0, true, false)]
        public void DownloadParsesSettlementData(int retriesBeforeSuccess, bool invalidData, bool successExpected)
        {
            var now = DateTime.UtcNow.Date;
            var processor = new TestingVIXContangoProcessor(
                null,
                null,
                now,
                "contango",
                false,
                false);


            var header = "Trade Date,Futures,Open,High,Low,Close,Settle,Change,Total Volume,EFP,Open Interest";
            var lines = new List<string>
            {
                "2021-04-26,F (Jan 2022),0.0000,23.5500,24.9500,0.0000,24.25,0,0,0,0",
                "2021-04-27,F (Jan 2022),24.4000,24.8500,24.4000,24.6500,24.675,0.425,10,0,8",
                "2021-04-28,F (Jan 2022),24.4500,24.5000,24.3500,24.4000,24.45,-0.225,8,0,11",
                "2021-04-29,F (Jan 2022),24.6000,24.6000,24.4000,24.6000,24.425,-0.025,1,0,10"
            };
            var expectedSettlements = new List<decimal>
            {
                24.25m,
                24.675m,
                24.45m,
                24.425m
            };

            if (invalidData)
            {
                lines = new List<string>
                {
                    "20202020202020-04-01111,,,,,,,,,,",
                    "2021-04-26,F (Jan 2022),-inf,000"
                };
            }

            var expectedSymbol = Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 1, 19));
            var expectedDates = new List<DateTime>
            {
                new DateTime(2021, 4, 26),
                new DateTime(2021, 4, 27),
                new DateTime(2021, 4, 28),
                new DateTime(2021, 4, 29)
            };

            var symbols = new List<Symbol> {expectedSymbol};
            
            processor.CBOESettlementData = string.Join("\n", new[] { header }.Concat(lines));
            processor.RetriesBeforeSuccess = retriesBeforeSuccess;

            if (invalidData)
            {
                Assert.Throws<FormatException>(() => processor.DownloadFuturesSettlement(symbols));
                return;
            }
            if (!successExpected)
            {
                if (!invalidData)
                {
                    var expected = processor.DownloadFuturesSettlement(symbols);
                    Assert.IsEmpty(expected);
                    return;
                }
                Assert.Throws<Exception>(() => processor.DownloadFuturesSettlement(symbols));
                return;
            }

            var settlement = processor.DownloadFuturesSettlement(symbols);

            Assert.AreEqual(expectedDates, settlement.Keys.OrderBy(x => x).ToList());
            for (var i = 0; i < lines.Count; i++)
            {
                Assert.AreEqual(expectedSettlements[i], settlement[expectedDates[i]][0].Close);
            }
        }

        [Test]
        public void GetsContango()
        {
            var now = DateTime.UtcNow.Date;
            var processor = new TestingVIXContangoProcessor(
                null,
                null,
                now,
                "contango",
                true,
                false);

            var symbols = new List<Symbol>
            {
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 1, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 2, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 3, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 4, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 5, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 6, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 7, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 8, 19)),
                Symbol.CreateFuture("VX", Market.CFE, new DateTime(2022, 9, 19)),
            };
            
            var lines = new List<string>
            {
                "2021-04-26,F (Jan 2022),0.0000,23.5500,24.9500,0.0000,24.25,0,0,0,0",
                "2021-04-27,F (Jan 2022),24.4000,24.8500,24.4000,24.6500,24.675,0.425,10,0,8",
                "2021-04-28,F (Jan 2022),24.4500,24.5000,24.3500,24.4000,24.45,-0.225,8,0,11",
                "2021-04-29,F (Jan 2022),24.6000,24.6000,24.4000,24.6000,24.425,-0.025,1,0,10"
            };

            var dates = new List<DateTime>
            {
                new DateTime(2021, 4, 26),
                new DateTime(2021, 4, 27),
                new DateTime(2021, 4, 28),
                new DateTime(2021, 4, 29),
            };

            var settlementData = new Dictionary<DateTime, List<TradeBar>>();
            var expectedContango = new List<VIXCentralContango>();
            var settlementValues = Enumerable.Range(10, symbols.Count).ToList();
            
            foreach (var date in dates)
            {
                var bars = new List<TradeBar>();
                settlementData[date] = bars;

                for (var i = 0; i < symbols.Count; i++)
                {
                    bars.Add(new TradeBar(date, symbols[i], 0m, 0m, 0m, 10m + i, 0m));
                }
                
                expectedContango.Add(new VIXCentralContango
                {
                    FrontMonth = bars[0].Symbol.ID.Date.Month,
                    F1 = bars[0].Close,
                    F2 = bars[1].Close,
                    F3 = bars[2].Close,
                    F4 = bars[3].Close,
                    F5 = bars[4].Close,
                    F6 = bars[5].Close,
                    F7 = bars[6].Close,
                    F8 = bars[7].Close,
                    F9 = bars[8].Close,
                    F10 = null,
                    F11 = null,
                    F12 = null,
                    Contango_F2_Minus_F1 = (bars[1].Close - bars[0].Close) / bars[0].Close,
                    Contango_F7_Minus_F4 = (bars[6].Close - bars[3].Close) / bars[3].Close,
                    Contango_F7_Minus_F4_Div_3 = ((bars[6].Close - bars[3].Close) / bars[3].Close) / 3m,
                    
                    Time = date
                });
            }

            var actualContango = processor.GetContango(settlementData, symbols);
         
            Assert.AreEqual(expectedContango.Count, actualContango.Count);
            for (var i = 0; i < expectedContango.Count; i++)
            {
                var expected = expectedContango[i];
                var actual = actualContango[i];
                
                Assert.AreEqual(expected.FrontMonth, actual.FrontMonth);
                Assert.AreEqual(expected.F1, actual.F1);
                Assert.AreEqual(expected.F2, actual.F2);
                Assert.AreEqual(expected.F3, actual.F3);
                Assert.AreEqual(expected.F4, actual.F4);
                Assert.AreEqual(expected.F5, actual.F5);
                Assert.AreEqual(expected.F6, actual.F6);
                Assert.AreEqual(expected.F7, actual.F7);
                Assert.AreEqual(expected.F8, actual.F8);
                Assert.AreEqual(expected.F9, actual.F9);
                Assert.AreEqual(expected.F10, actual.F10);
                Assert.AreEqual(expected.F11, actual.F11);
                Assert.AreEqual(expected.F12, actual.F12);
                Assert.AreEqual(expected.Contango_F2_Minus_F1, actual.Contango_F2_Minus_F1);
                Assert.AreEqual(expected.Contango_F7_Minus_F4, actual.Contango_F7_Minus_F4);
                Assert.AreEqual(expected.Contango_F7_Minus_F4_Div_3, actual.Contango_F7_Minus_F4_Div_3);
                Assert.AreEqual(expected.Time, actual.Time);
            }
        }

        private class TestingVIXContangoProcessor : VIXContangoProcessor
        {
            private int _currentRetries;
            
            public string CBOESettlementData { get; set; }
            public int RetriesBeforeSuccess { get; set; }
            
            public TestingVIXContangoProcessor(
                DirectoryInfo baseOutputDirectory,
                DirectoryInfo existingDataDirectory,
                DateTime deploymentDate,
                string outputVendorDirectoryName,
                bool overwriteExistingData,
                bool processOnlyDeploymentDateData) 
                : base(baseOutputDirectory, existingDataDirectory, deploymentDate, deploymentDate, outputVendorDirectoryName, overwriteExistingData, processOnlyDeploymentDateData)
            {
            }

            protected override string GetFutureMarket(string ticker)
            {
                return Market.CFE;
            }

            protected override string DownloadCBOESettlementData(HttpClient client, string url)
            {
                if (_currentRetries < RetriesBeforeSuccess)
                {
                    _currentRetries++;
                    throw new HttpRequestException("");
                }
                
                return CBOESettlementData;
            }

            protected override DateTime GetPreviousChainExpiry(List<Symbol> futureChain)
            {
                return DateTime.MinValue;
            }
        }
    }
}

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CSE.ImdbImport
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "By design")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Globalization", "CA1303:Do not pass literals as localized parameters", Justification = "not localized")]
    class Program
    {
        static int count = 0;
        static int retryCount = 0;
        static DocumentClient client;
        static readonly object statusLock = new object();

        // timers
        static readonly DateTime startTime = DateTime.Now;
        static DateTime batchTime = DateTime.Now;

        // limit the number of concurrent load tasks to 6 with a 400 RU collection
        // don't set either below 3
        static int maxLoaders = 6;
        static int minLoaders = 3;

        // set the batch size
        // keep this small or the last load threads will take a long time
        // 25 is optimal for most situations
        const int batchSize = 25;

        // worker tasks
        static readonly List<Task> tasks = new List<Task>();

        static async Task<int> Main(string[] args)
        {
            // This loader uses the single document upsert API for simplicity
            // Peak throughput is about 400 documents / sec
            // While the loader uses multiple concurrent tasks to speed up the loading,
            // the bulk load APIs should be used for large loads

            // make sure the args were passed in
            if (args.Length != 4)
            {
                Usage();
                return -1;
            }

            Console.WriteLine("Creating Composite Indices");

            // get the Cosmos values from args[]
            string cosmosUrl = string.Format(CultureInfo.InvariantCulture, "https://{0}.documents.azure.com:443/", args[0].Trim());
            string cosmosKey = args[1].Trim();
            string cosmosDatabase = args[2].Trim();
            string cosmosCollection = args[3].Trim();

            try
            {
                string path = GetDataFilesPath();

                client = await OpenCosmosClient(cosmosUrl, cosmosKey, cosmosDatabase, cosmosCollection).ConfigureAwait(false);

                // create composite indices if necessary
                await CreateCompositeIndicesAsync(client, cosmosDatabase, cosmosCollection).ConfigureAwait(false);

                Console.WriteLine("Loading Data ...");

                // load featured
                LoadFile(path + "featured.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, cosmosCollection));

                // load genres
                LoadFile(path + "genres.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, cosmosCollection));

                // load movies
                LoadFile(path + "movies.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, cosmosCollection));

                // load actors
                LoadFile(path + "actors.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, cosmosCollection));

                // wait for tasks to finish
                Task.WaitAll(tasks.ToArray());

                // done
                TimeSpan elapsed = DateTime.Now.Subtract(startTime);
                Console.WriteLine("\nDocuments Loaded: {0}", count);
                Console.Write("    Elasped Time: ");
                if (elapsed.TotalHours >= 1.0)
                {
                    Console.Write("{0:00':'}", elapsed.TotalHours);
                }
                Console.WriteLine(elapsed.ToString("mm':'ss", CultureInfo.InvariantCulture));
                Console.WriteLine("     rows/second: {0:0.00}", count / DateTime.Now.Subtract(startTime).TotalSeconds);
                Console.WriteLine("         Retries: {0}", retryCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -1;
            }

            return 0;
        }

        static async Task CreateCompositeIndicesAsync(DocumentClient client, string cosmosDatabase, string cosmosCollection)
        {
            // create composite indices if necessary
            var col = await client.ReadDocumentCollectionAsync(string.Format(CultureInfo.InvariantCulture, $"/dbs/{cosmosDatabase}/colls/{cosmosCollection}")).ConfigureAwait(false);
            var cur = col.Resource.IndexingPolicy.CompositeIndexes;

            if (cur == null || cur.Count != 2)
            {
                // create the composite indices
                var idx = new System.Collections.ObjectModel.Collection<System.Collections.ObjectModel.Collection<CompositePath>>
                    {
                        new Collection<CompositePath>
                        {
                            // actors - order by textSearch ASC, actorId ASC
                            new CompositePath { Order = CompositePathSortOrder.Ascending, Path = "/textSearch" },
                            new CompositePath { Order = CompositePathSortOrder.Ascending, Path = "/actorId" }
                        },

                        new Collection<CompositePath>
                        {
                            // movies - order by textSearch ASC, movieId ASC
                            new CompositePath { Order = CompositePathSortOrder.Ascending, Path = "/textSearch" },
                            new CompositePath { Order = CompositePathSortOrder.Ascending, Path = "/movieId" }
                        }
                    };

                // update the container
                col.Resource.IndexingPolicy.CompositeIndexes = idx;
                var repl = await client.ReplaceDocumentCollectionAsync(col).ConfigureAwait(false);

                // verify index
                idx = repl.Resource.IndexingPolicy.CompositeIndexes;

                if (idx == null || idx.Count != 2)
                {
                    throw new Exception("Index create failed");
                }
            }
        }

        static void WaitForLoader()
        {
            // only start a new loader task if there are < maxLoaders running
            if (tasks.Count < maxLoaders)
            {
                return;
            }

            // loop until a loader is available
            while (true)
            {
                // remove completed tasks
                for (int i = tasks.Count - 1; i >= 0; i--)
                {
                    if (tasks[i].IsCompleted)
                    {
                        tasks.RemoveAt(i);
                    }
                }

                if (tasks.Count < maxLoaders)
                {
                    return;
                }

                System.Threading.Thread.Sleep(100);
            }
        }

        static string GetDataFilesPath()
        {
            // find the data files (different with dotnet run and running in VS)

            string path = "../data/";

            if (!Directory.Exists(path))
            {
                path = "../../../" + path;

                if (!Directory.Exists(path))
                {
                    Console.WriteLine("Can't find data files");
                    Environment.Exit(-1);
                }
            }

            return path;
        }

        static async Task<DocumentClient> OpenCosmosClient(string cosmosUrl, string cosmosKey, string cosmosDatabase, string cosmosCollection)
        {
            // set min / max concurrent loaders
            SetLoaders();

            // increase timeout and number of retries
            ConnectionPolicy cp = new ConnectionPolicy
            {
                ConnectionProtocol = Protocol.Tcp,
                ConnectionMode = ConnectionMode.Direct,
                MaxConnectionLimit = maxLoaders * 2,
                RequestTimeout = TimeSpan.FromSeconds(120),
                RetryOptions = new RetryOptions
                {
                    MaxRetryAttemptsOnThrottledRequests = 10,
                    MaxRetryWaitTimeInSeconds = 120
                }
            };

            // open the Cosmos client
            client = new DocumentClient(new Uri(cosmosUrl), cosmosKey, cp);
            await client.OpenAsync().ConfigureAwait(false);

            // validate database and collection exist
            await client.ReadDatabaseAsync("/dbs/" + cosmosDatabase).ConfigureAwait(false);
            await client.ReadDocumentCollectionAsync("/dbs/" + cosmosDatabase + "/colls/" + cosmosCollection).ConfigureAwait(false);

            return client;
        }

        static void SetLoaders()
        {
            // at least 3 concurrent loaders for performance
            if (maxLoaders < 3)
            {
                maxLoaders = 3;
            }

            // set the minLoaders
            if (maxLoaders > 7)
            {
                minLoaders = maxLoaders / 2;
            }
            else
            {
                minLoaders = 3;
            }
        }

        static void LoadFile(string path, Uri collectionUri)
        {
            // Read the file and load in batches
            using System.IO.StreamReader file = new System.IO.StreamReader(path);
            int count = 0;

            string line;
            List<dynamic> list = new List<dynamic>();

            while ((line = file.ReadLine()) != null)
            {
                line = line.Trim();

                if (line.StartsWith("{", StringComparison.OrdinalIgnoreCase))
                {
                    if (line.EndsWith(",", StringComparison.OrdinalIgnoreCase))
                    {
                        line = line[0..^1];
                    }

                    if (line.EndsWith("}", StringComparison.OrdinalIgnoreCase))
                    {
                        count++;
                        list.Add(JsonConvert.DeserializeObject<dynamic>(line));
                    }
                }

                // load the batch
                if (list.Count >= batchSize)
                {
                    WaitForLoader();
                    tasks.Add(LoadData(collectionUri, list));
                    list = new List<dynamic>();
                }
            }

            // load any remaining docs
            if (list.Count > 0)
            {
                WaitForLoader();
                tasks.Add(LoadData(collectionUri, list));
            }
        }

        static async Task LoadData(Uri colLink, List<dynamic> list)
        {
            // load data worker

            foreach (var doc in list)
            {
                // loop to handle retry
                while (true)
                {
                    try
                    {
                        // this will throw an exception if we exceed RUs
                        await client.UpsertDocumentAsync(colLink, doc);
                        IncrementCounter();
                        break;
                    }
                    catch (DocumentClientException dce)
                    {
                        // catch the CosmosDB RU exceeded exception and retry
                        retryCount++;

                        // reduce the number of concurrent loaders
                        if (maxLoaders > minLoaders)
                        {
                            maxLoaders--;

                            // this will lock a slot in the semaphore
                            // effectively reducing the number of concurrent tasks
                            WaitForLoader();
                        }

                        // sleep the suggested amount of time
                        Thread.Sleep(dce.RetryAfter);
                    }

                    catch (Exception ex)
                    {
                        // log and exit

                        Console.WriteLine(ex);
                        Environment.Exit(-1);
                    }
                }
            }
        }

        static void IncrementCounter()
        {
            // lock the counter
            lock (statusLock)
            {
                count++;

                // update progress
                if (count % 100 == 0)
                {
                    Console.WriteLine("{0}\t{1}\t{2}\t{3}", count, maxLoaders, string.Format(CultureInfo.InvariantCulture, "{0:0.00}", DateTime.Now.Subtract(batchTime).TotalSeconds), DateTime.Now.Subtract(startTime).ToString("mm':'ss", CultureInfo.InvariantCulture));
                    batchTime = DateTime.Now;
                }
            }
        }

        static void Usage()
        {
            Console.WriteLine("Usage: imdb-import CosmosServer CosmosKey Database Collection");
        }
    }
}

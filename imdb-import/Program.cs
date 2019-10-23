using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ImdbImport
{
    class Program
    {
        static int count = 0;
        static int retryCount = 0;
        static DocumentClient client;
        static readonly object statusLock = new object();

        // timers
        static readonly DateTime startTime = DateTime.Now;
        static DateTime batchTime = DateTime.Now;

        // limit the number of concurrent load tasks to 3 with a 400 RU collection
        // don't set either below 3
        static int maxLoaders = 3;
        static int minLoaders = 3;

        // set the batch size
        // keep this small or the last load threads will take a long time
        // 20 is optimal for most situations
        const int batchSize = 20;

        // worker tasks
        static readonly List<Task> tasks = new List<Task>();

        // see WaitForLoader() - not used by default
        static Semaphore sem;

        static async Task Main(string[] args)
        {
            // This loader uses the single document upsert API for simplicity
            // Peak throughput is about 400 documents / sec
            // While the loader uses multiple concurrent tasks to speed up the loading,
            // the bulk load APIs should be used for large loads

            // make sure the args were passed in
            if (args.Length != 4 && args.Length != 6)
            {
                Usage();
                Environment.Exit(-1);
            }

            Console.WriteLine("Loading Data ...\n");

            // get the Cosmos values from args[]
            string cosmosUrl = string.Format("https://{0}.documents.azure.com:443/", args[0].Trim().ToLower());
            string cosmosKey = args[1].Trim();
            string cosmosDatabase = args[2].Trim();
            string actorsCollection = args[3].Trim();
            string genresCollection = args[3].Trim();
            string moviesCollection = args[3].Trim();
            string featuredCollection = args[3].Trim();

            if (args.Length > 4)
            {
                if (args.Length < 7)
                {
                    Console.WriteLine("Check the command line arguments");
                    Usage();
                    return;
                }

                featuredCollection = args[4].Trim();
                genresCollection = args[5].Trim();
                moviesCollection = args[6].Trim();
            }


            client = await OpenCosmosClient(cosmosUrl, cosmosKey);

            string path = GetFilePath();

            // load genres
            LoadFile(path + "genres.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, genresCollection));

            // load featured
            LoadFile(path + "featured.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, featuredCollection));

            // load movies
            LoadFile(path + "movies.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, moviesCollection));

            // load actors
            LoadFile(path + "actors.json", UriFactory.CreateDocumentCollectionUri(cosmosDatabase, actorsCollection));

            // wait for tasks to finish
            Task.WaitAll(tasks.ToArray());

            Console.WriteLine(tasks.Count);

            TimeSpan elapsed = DateTime.Now.Subtract(startTime);

            // done
            Console.WriteLine();
            Console.WriteLine("Documents Loaded: {0}", count);
            Console.Write("    Elasped Time: ");
            if (elapsed.TotalHours >= 1.0)
            {
                Console.Write("{0:00':'}", elapsed.TotalHours);
            }
            Console.WriteLine(elapsed.ToString("mm':'ss"));
            Console.WriteLine("     rows/second: {0:0.00}", count / DateTime.Now.Subtract(startTime).TotalSeconds);
            Console.WriteLine("         Retries: {0}", retryCount);
        }

        static void WaitForLoader()
        {
            // semaphores are about 30% slower
            // we don't need thread sync as one thread starts all the jobs
            // so the default is false
            bool useSemaphore = false;

            if (useSemaphore)
            {
                if (sem == null)
                {
                    sem = new Semaphore(0, maxLoaders);
                }

                sem.WaitOne();
            }

            else
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
        }

        static string GetFilePath()
        {
            // find the data files (different with dotnet run and running in VS)

            string path = "data/";

            if (!File.Exists(path + "genres.json"))
            {
                path = "../../../data/";

                if (!File.Exists(path + "genres.json"))
                {
                    Console.WriteLine("Can't find data files");
                    Environment.Exit(-1);
                }
            }

            return path;
        }

        static async Task<DocumentClient> OpenCosmosClient(string cosmosUrl, string cosmosKey)
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
            await client.OpenAsync();

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
            using (System.IO.StreamReader file = new System.IO.StreamReader(path))
            {
                int count = 0;
                int maxLines = int.MaxValue;

                string line;
                List<dynamic> list = new List<dynamic>();

                while ((line = file.ReadLine()) != null && count < maxLines)
                {
                    line = line.Trim();

                    if (line.StartsWith("{"))
                    {
                        if (line.EndsWith(","))
                        {
                            line = line.Substring(0, line.Length - 1);
                        }

                        if (line.EndsWith("}"))
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

            // release the semaphore
            if (sem != null) sem.Release();
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
                    Console.WriteLine("{0}\t{1}\t{2}\t{3}", count, maxLoaders, string.Format("{0:0.00}", DateTime.Now.Subtract(batchTime).TotalSeconds), DateTime.Now.Subtract(startTime).ToString("mm':'ss"));
                    batchTime = DateTime.Now;
                }
            }
        }

        static void Usage()
        {
            Console.WriteLine("Usage: imdb-import Name Key Database [SingleCollection][ActorCollection FeaturedCollection GenreCollection MovieCollection]");
        }
    }
}

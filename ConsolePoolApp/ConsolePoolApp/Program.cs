using System;
using System.IO;
using System.Collections.Generic;
using System.Timers;
using System.Diagnostics;
using System.Linq;
using System.Threading;
//using Microsoft.Azure; //Namespace for CloudConfigurationManager
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using CommandLine;
using CommandLine.Text;




//Objective:
//Create a console app in C# to showcase Azure SQL server updates using Azure Event Hubs.  Only need to trigger event to send data, no need to listen to events.
//All work is done on customer's RDP.

namespace ConsolePoolApp
{
    public static class Program
    {

        //commandline parser error messages
        public static ParserResult<T> ThrowOnParseError<T>(this ParserResult<T> result)
        {
            if (!(result is NotParsed<T>))
            {
                // Case with no errors needs to be detected explicitly, otherwise the .Select line will throw an InvalidCastException
                return result;
            }

            var builder = SentenceBuilder.Create();
            var errorMessages = HelpText.RenderParsingErrorsTextAsLines(result, builder.FormatError, builder.FormatMutuallyExclusiveSetErrors, 1);
            var excList = errorMessages.Select(msg => new ArgumentException(msg)).ToList();

            if (excList.Any())
            {

                throw new AggregateException(excList);
            }

            return result;
        }

        static async Task Main(string[] args)
        {
            Console.WriteLine("Event Hub Data Post");
            Console.WriteLine("Copyright (c) 2021 Onsystex Inc.");

            var watch = new Stopwatch();
            watch.Start();

            var options = new Options();
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(parsed => options = parsed)
                .ThrowOnParseError();

            var poolEvents = new PoolEvents(options);

            try
            {
                await poolEvents.MakeConnection();
                poolEvents.Monitor(); 
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.ExitCode = ex.HResult;
                return;
            }


            //reader.Close();
            watch.Stop();
            Console.WriteLine($"Elapsed Time: {watch.ElapsedMilliseconds} ms");
            Console.WriteLine($"Bytes Sent  : {poolEvents.bytesSent} bytes");

        }



        class PoolEvents
        {
            public EventDataBatch eventBatch;
            public EventHubProducerClient producerClient;
            public Options options;
            public long bytesSent;
            public SortedSet<string> AllFiles = new SortedSet<string>();
            public System.Timers.Timer _notificationTimer;
            public bool continuous;
            private static ManualResetEvent mre = new ManualResetEvent(false);
            // constructor
            public PoolEvents(Options options)
            {
                this.options = options;
            }

            public async Task MakeConnection()
            {
                // get the file attributes for file or directory
                FileAttributes attr = File.GetAttributes(this.options.Path);
                if (attr.HasFlag(FileAttributes.Directory))
                    continuous = true;
                else
                    continuous = false;

                Console.WriteLine("Found Path.");
                // check if Connection string is good
                var builder = new Microsoft.Azure.EventHubs.EventHubsConnectionStringBuilder(this.options.ConnectionString);
                Uri endpointAddress = builder.Endpoint;
                // check for connection, using legacy EventHubs
                Microsoft.Azure.EventHubs.EventHubClient.CreateWithManagedServiceIdentity(endpointAddress, this.options.HubName);

                // Create a producer client that you can use to send events to an event hub
                var clientOptions = new EventHubProducerClientOptions();
                clientOptions.ConnectionOptions.TransportType = EventHubsTransportType.AmqpWebSockets;

                this.producerClient = new EventHubProducerClient(this.options.ConnectionString, this.options.HubName, clientOptions);
                Console.WriteLine("Made Connection");
                if (continuous)  // add all files already present to be sent
                {                    
                    //foreach (string f in Directory.GetFiles(this.options.Path, "*.json"))
                    //{
                    //    AllFiles.Add(f);
                    //}
                }
                else
                {
                    using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                    var reader = new StreamReader(this.options.Path);
                    var myJson = reader.ReadToEnd();

                    reader.Close();
                    Console.WriteLine("Read File.");
                    //create and send event
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(myJson)));
                    this.bytesSent = eventBatch.SizeInBytes;

                    await this.producerClient.SendAsync(eventBatch);
                    Console.WriteLine($"Sent {this.options.Path}");

                    File.Delete(this.options.Path);
                    Environment.Exit(1);
                }
            }

            public static void ThreadDelete(string file)
            {
                File.Delete(file);
            }
            public async Task StartTheThread(string param)
            {
                await Task.Run(() => ThreadDelete(param));
            }



            private async void Timer_Elapsed(object s, ElapsedEventArgs e)
            {
                
                Console.WriteLine("The Sending Event was raised at {0}", e.SignalTime);
                foreach (string f in Directory.GetFiles(this.options.Path ))
                {
                    AllFiles.Add(f);
                }
                
                const int NumberOfRetries = 5;
                const int DelayOnRetry = 100;
                for (int i = 1; i <= NumberOfRetries; ++i)
                {
                    if (i > 1) Console.WriteLine(i);
                    try
                    {
                        while (AllFiles.Count > 0)
                        {
                            Console.WriteLine($"File: {AllFiles.Min()} collected.");
                            using (EventDataBatch eventBatch = await producerClient.CreateBatchAsync())
                            {
                                if (AllFiles.Min() == this.options.Path + "Monitor.stop")
                                {
                                    Console.WriteLine("Stopping..");
                                    await StartTheThread(AllFiles.Min());
                                    Environment.Exit(1);
                                }

                                byte[] myJson = null;
                                //string json2;
                                try
                                {
                                    myJson = System.IO.File.ReadAllBytes(AllFiles.Min());
                                    //json2 = System.IO.File.ReadAllText(AllFiles.Min());
                                }
                                catch (Exception ex)
                                {   
                                    Console.WriteLine($"File: {AllFiles.Min()} EX:" + ex.Message);
                                    continue;
                                }
                                if (eventBatch.TryAdd(new EventData(myJson)))
                                //if (eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(json2))))
                                    {
                                    Console.WriteLine($"Sent {AllFiles.Min()}");
                                    await StartTheThread(AllFiles.Min());
                                    AllFiles.Remove(AllFiles.Min());
                                }
                                else
                                {
                                    Console.WriteLine($"File: {AllFiles.Min()} send Error");
                                }

                                this.bytesSent = eventBatch.SizeInBytes;                                
                                await this.producerClient.SendAsync(eventBatch);
                            }
                        }
                        break;
                    }
                    catch (IOException ex) when (i <= NumberOfRetries)
                    {
                        Thread.Sleep(DelayOnRetry);
                    }
                    catch (InvalidOperationException ex) when (i <= NumberOfRetries)
                    {
                        Thread.Sleep(DelayOnRetry);
                    }
                    finally
                    {
                        _notificationTimer.Enabled = true;
                    }
                }
            }
            

            public async void OnCreated(object source, FileSystemEventArgs fileEvent)
            {
                Console.WriteLine($"File: {fileEvent.FullPath} detected.");
                this.AllFiles.Add(fileEvent.FullPath);

                
                if (this.options.Interval == 0)
                {
                    using (EventDataBatch eventBatch = await producerClient.CreateBatchAsync())

                    {
                        this.eventBatch = eventBatch;
                        using (var reader = new StreamReader(fileEvent.FullPath))
                        {
                            var myJson = reader.ReadToEnd();
                            File.Delete(fileEvent.FullPath);
                            //create and send event
                            this.eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(myJson)));
                        }

                        //reader.Close();

                        //await producerClient.SendAsync(eventBatch);
                        this.bytesSent = this.eventBatch.SizeInBytes;

                        await this.producerClient.SendAsync(this.eventBatch);
                        Console.WriteLine($"Sent {fileEvent.FullPath}");
                    }
                }
            }


            public void Monitor()
            {
                //if (this.options.Interval != 0)
                //{
                    _notificationTimer = new System.Timers.Timer();
                    _notificationTimer.Elapsed += Timer_Elapsed;
                    _notificationTimer.AutoReset = false;
                    _notificationTimer.Interval = this.options.Interval; // in ms
                    _notificationTimer.Start();
                //}

                /*
                FileSystemWatcher watcher = new FileSystemWatcher(this.options.Path);
                watcher.Path = this.options.Path;

                // Watch for changes in LastAccess and LastWrite times, and
                // the renaming of files or directories.
                //watcher.NotifyFilter = NotifyFilters.LastWrite;
                Console.WriteLine(watcher.Path);
                // Only watch text files.
                watcher.Filters.Add("*.json");
                watcher.Filters.Add("Monitor.stop");

                // Add event handlers.
                watcher.Created += OnCreated;

                // Begin watching.
                watcher.EnableRaisingEvents = true;
                */

                // Wait for the user to quit the program.
                Console.WriteLine("Press 'q' to quit the sample.");
                while (Console.Read() != 'q') ;
            }
        }

        // commandline arguments
        public class Options
        {
            //[Value(0, Required = true, HelpText = "Set the file path to display information for.")] // first position is the filename
            [Option('f', "file or folder", Required=true)]
            public string Path { get; set; }

            [Option('h', "hub", Required = true, HelpText = "Set Event Hub Name")]
            public string HubName { get; set; }

            [Option('c', "connectionString", Required = true, HelpText = "Set Connection String")]
            public string ConnectionString { get; set; }

            [Option('t', "time interval", Default = 5000, HelpText = "Set sending intervals in ms")]
            public int Interval { get; set; }
            
        }
    }
}

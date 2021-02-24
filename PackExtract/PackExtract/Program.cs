using System;
using System.IO;
using System.Security.Permissions;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Timers;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft;
//using Microsoft.Azure; //Namespace for CloudConfigurationManager
using System.Text;
//using System.Windows.Forms;
using CommandLine;
using CommandLine.Text;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace PackExtract
{
    public static class Program
    {
        static async Task Main(string[] args)
        {

            var options = new Options();
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed(parsed => options = parsed)
                .ThrowOnParseError();

            // get the file attributes for file or directory
            try
            {
                FileAttributes attr = File.GetAttributes(options.Target);
                if (attr.HasFlag(FileAttributes.Directory))
                {
                    Console.WriteLine("Target should be a file not directory.");
                    throw new TypeLoadException();
                }
                attr = File.GetAttributes(options.Destination);
                if (!attr.HasFlag(FileAttributes.Directory)) 
                { 
                    Console.WriteLine("Destination should be a directory not file.");
                    throw new TypeLoadException();
                }

                //            https://stackoverflow.com/questions/5132890/c-sharp-replace-bytes-in-byte

                var timer = new Stopwatch();
                timer.Start();

                byte[] all_bytes = File.ReadAllBytes(options.Target);

                //int cntFS = 0;
                List<int> FSindexes = new List<int>();

                for (int i = 0; i < all_bytes.Length; i++)
                {
                    if ((char)all_bytes[i] == (char)0x1d) // replace GS to b things
                        all_bytes[i] = (byte)(char)0xfe;

                    if ((char)all_bytes[i] == (char)0x1c)
                    {
                        //cntFS += 1;
                        FSindexes.Add(i);
                    }
                }
                Console.WriteLine("Records: " + (FSindexes.Count-2).ToString() + "\nCommencing Extraction...");

                List<byte> id_collect = new List<byte>();
                string id_send = "";
                bool pause = false;
                int r = 3;
                for (int id = FSindexes[1]+1; id < FSindexes[2]; id++)
                {

                    // make ids
                    if ((char)all_bytes[id] == (char)0xfd)
                    {
                        byte[] send_id = id_collect.ToArray();
                        pause = true;
                        id_send = System.Text.Encoding.Default.GetString(send_id);
                        id_collect.Clear();
                    }
                    else 
                    {
                        id_collect.Add(all_bytes[id]);
                    }

                    while (pause)
                    {
                        
                        using (var fs = new FileStream(options.Destination + id_send + ".txt", FileMode.Create, FileAccess.Write))
                        {
                            fs.Write(all_bytes, FSindexes[r - 1] + 1, FSindexes[r] - FSindexes[r - 1]-1);
                        }

                        r++;
                        pause = false;
                    }
                }

                // last id and record

                byte[] last_id = id_collect.ToArray();
                id_send = System.Text.Encoding.Default.GetString(last_id);

                using (var fs = new FileStream(options.Destination + id_send + ".txt", FileMode.Create, FileAccess.Write))
                {
                    fs.Write(all_bytes, FSindexes[r - 1] + 1, all_bytes.Length - FSindexes[r - 1] - 1);
                }

                timer.Stop();
                Console.WriteLine($"\nElapsed Time: {timer.ElapsedMilliseconds} ms");
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Environment.ExitCode = ex.HResult;
            }

        }

        

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
    }

    public class Options
    {
        [Value(0, Required = true, HelpText = "Set the file path to parse")] // first position is the filename
        public string Target { get; set; }

        [Value(1, Required = true, HelpText = "Set the destination path")]
        public string Destination { get; set; }
    }
}

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Security;
using WebHDFS;

namespace WebHDFSConsole
{
    class Program
    {
        static string ReadFilename()
        {
            Console.WriteLine("File to upload: ");
            return Console.ReadLine();
        }

        static string ReadWebHDFSAPI() {
            Console.WriteLine("Enter WebHDFS API: ");
            return Console.ReadLine();
        }

        static string ReadUsername() {
            Console.WriteLine("Enter username: ");
            return Console.ReadLine();
        }

        static SecureString ReadPassword() {
            SecureString password = new SecureString();
            Console.WriteLine("Enter password: ");

            ConsoleKeyInfo nextKey = Console.ReadKey(true);

            while (nextKey.Key != ConsoleKey.Enter)
            {
                if (nextKey.Key == ConsoleKey.Backspace)
                {
                    if (password.Length > 0)
                    {
                        password.RemoveAt(password.Length - 1);
                        // erase the last * as well
                        Console.Write(nextKey.KeyChar);
                        Console.Write(" ");
                        Console.Write(nextKey.KeyChar);
                    }
                }
                else
                {
                    password.AppendChar(nextKey.KeyChar);
                    Console.Write("*");
                }
                nextKey = Console.ReadKey(true);
            }
            Console.WriteLine();
            password.MakeReadOnly();
            return password;
        }

        static void Main(string[] args)
        {
            string filename = ReadFilename();
            string hdfsFilename = "/tmp/" + Path.GetFileName(filename) + ".dotnet";

            var webhdfs = new WebHDFSClient(ReadWebHDFSAPI())
            {
                Credentials = new NetworkCredential(ReadUsername(), ReadPassword())
            };

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            Console.WriteLine("Starting to upload file - " + filename);

            var result = webhdfs.UploadFile(
                filename,
                hdfsFilename,
                overwrite: true
            ).Result;

            Console.WriteLine("Upload successful?: " + result);
            Console.WriteLine("Finished uploading file - " + filename);

            stopWatch.Stop();
            Console.WriteLine("Time Elapsed: " + stopWatch.Elapsed);

            Console.WriteLine("File info: " + webhdfs.GetFileStatus(hdfsFilename).Result);
        }
    }
}

using System;
using System.Net;
using Microsoft.Spark.Sql;
using System.IO;

namespace dotnet_spark
{
    class Program
    {
        static void Main(string[] args)
        {
            /*Download a file from Internet*/
            var dataUrl = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv";
            var localFileName = "stats.csv";
            using (var client = new WebClient())
            {
                client.DownloadFile(dataUrl, localFileName);
            }
            var localFilePath = Path.GetFullPath(localFileName);
            Console.WriteLine("Local CSV file path is: {0}", localFilePath);

            /*Create Spark session*/
            var spark = SparkSession.Builder().GetOrCreate();
            var df = spark.Read().Csv($"file:///{localFilePath}");
            df.Select("_c0", "_c1", "_c2", "_c3").Show(10);
            df.PrintSchema();
        }
    }
}
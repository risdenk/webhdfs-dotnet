using System;
namespace knoxdotnetdsl
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileChecksum_JSON_Schema
    /// </summary>
    public class FileChecksumClass
    {
        public FileChecksum FileChecksum { get; set; }
    }

    public class FileChecksum
    {
        public string algorithm { get; set; }
        public string bytes { get; set; }
        public int length { get; set; }
    }
}

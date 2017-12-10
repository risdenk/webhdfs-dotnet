using System;
namespace knoxdotnetdsl
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_Properties
    /// </summary>
    public class FileStatusClass
    {
        public FileStatus FileStatus { get; set; }
    }

    public class FileStatus
    {
        public long accessTime { get; set; }
        public int blockSize { get; set; }
        public string group { get; set; }
        public long length { get; set; }
        public long modificationTime { get; set; }
        public string owner { get; set; }
        public string pathSuffix { get; set; }
        public string permission { get; set; }
        public int replication { get; set; }
        public string type { get; set; }
    }
}

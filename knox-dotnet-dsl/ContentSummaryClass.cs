using System;
namespace knoxdotnetdsl
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
    /// </summary>
    public class ContentSummaryClass
    {
        public ContentSummary ContentSummary { get; set; }
    }

    public class ARCHIVE
    {
        public int consumed { get; set; }
        public int quota { get; set; }
    }

    public class DISK
    {
        public int consumed { get; set; }
        public int quota { get; set; }
    }

    public class SSD
    {
        public int consumed { get; set; }
        public int quota { get; set; }
    }

    public class TypeQuota
    {
        public ARCHIVE ARCHIVE { get; set; }
        public DISK DISK { get; set; }
        public SSD SSD { get; set; }
    }

    public class ContentSummary
    {
        public int directoryCount { get; set; }
        public int fileCount { get; set; }
        public int length { get; set; }
        public int quota { get; set; }
        public int spaceConsumed { get; set; }
        public int spaceQuota { get; set; }
        public TypeQuota typeQuota { get; set; }
    }
}

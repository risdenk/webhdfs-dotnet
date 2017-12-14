using System;
namespace WebHDFS
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
    /// </summary>
    public class ContentSummaryClass
    {
        /// <summary>
        /// Gets or sets the content summary.
        /// </summary>
        /// <value>The content summary.</value>
        public ContentSummary ContentSummary { get; set; }
    }

    /// <summary>
    /// Archive.
    /// </summary>
    public class ARCHIVE
    {
        /// <summary>
        /// Gets or sets the consumed.
        /// </summary>
        /// <value>The consumed.</value>
        public int consumed { get; set; }

        /// <summary>
        /// Gets or sets the quota.
        /// </summary>
        /// <value>The quota.</value>
        public int quota { get; set; }
    }

    /// <summary>
    /// Disk.
    /// </summary>
    public class DISK
    {
        /// <summary>
        /// Gets or sets the consumed.
        /// </summary>
        /// <value>The consumed.</value>
        public int consumed { get; set; }

        /// <summary>
        /// Gets or sets the quota.
        /// </summary>
        /// <value>The quota.</value>
        public int quota { get; set; }
    }

    /// <summary>
    /// SSD.
    /// </summary>
    public class SSD
    {
        /// <summary>
        /// Gets or sets the consumed.
        /// </summary>
        /// <value>The consumed.</value>
        public int consumed { get; set; }

        /// <summary>
        /// Gets or sets the quota.
        /// </summary>
        /// <value>The quota.</value>
        public int quota { get; set; }
    }

    /// <summary>
    /// Type quota.
    /// </summary>
    public class TypeQuota
    {
        /// <summary>
        /// Gets or sets the archive.
        /// </summary>
        /// <value>The archive.</value>
        public ARCHIVE ARCHIVE { get; set; }

        /// <summary>
        /// Gets or sets the disk.
        /// </summary>
        /// <value>The disk.</value>
        public DISK DISK { get; set; }

        /// <summary>
        /// Gets or sets the ssd.
        /// </summary>
        /// <value>The ssd.</value>
        public SSD SSD { get; set; }
    }

    /// <summary>
    /// Content summary.
    /// </summary>
    public class ContentSummary
    {
        /// <summary>
        /// Gets or sets the directory count.
        /// </summary>
        /// <value>The directory count.</value>
        public int directoryCount { get; set; }

        /// <summary>
        /// Gets or sets the file count.
        /// </summary>
        /// <value>The file count.</value>
        public int fileCount { get; set; }

        /// <summary>
        /// Gets or sets the length.
        /// </summary>
        /// <value>The length.</value>
        public int length { get; set; }

        /// <summary>
        /// Gets or sets the quota.
        /// </summary>
        /// <value>The quota.</value>
        public int quota { get; set; }

        /// <summary>
        /// Gets or sets the space consumed.
        /// </summary>
        /// <value>The space consumed.</value>
        public int spaceConsumed { get; set; }

        /// <summary>
        /// Gets or sets the space quota.
        /// </summary>
        /// <value>The space quota.</value>
        public int spaceQuota { get; set; }

        /// <summary>
        /// Gets or sets the type quota.
        /// </summary>
        /// <value>The type quota.</value>
        public TypeQuota typeQuota { get; set; }
    }
}

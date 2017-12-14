using System;
using System.Collections.Generic;

namespace WebHDFS
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatuses_JSON_Schema
    /// </summary>
    public class FileStatusesClass
    {
        /// <summary>
        /// Gets or sets the file statuses.
        /// </summary>
        /// <value>The file statuses.</value>
        public FileStatuses FileStatuses { get; set; }
    }

    /// <summary>
    /// File statuses.
    /// </summary>
    public class FileStatuses
    {
        /// <summary>
        /// Gets or sets the file status.
        /// </summary>
        /// <value>The file status.</value>
        public List<FileStatus> FileStatus { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileStatuses"/>.
        /// </summary>
        /// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileStatuses"/>.</returns>
        public override string ToString()
        {
            return string.Join("\n", FileStatus);
        }
    }
}

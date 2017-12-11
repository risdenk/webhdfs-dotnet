using System;
using System.Collections.Generic;

namespace knoxdotnetdsl
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatuses_JSON_Schema
    /// </summary>
    public class FileStatusesClass
    {
        public FileStatuses FileStatuses { get; set; }
    }

    public class FileStatuses
    {
        public List<FileStatus> FileStatus { get; set; }

        public override string ToString()
        {
            return string.Join("\n", FileStatus);
        }
    }
}

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

        public override string ToString(){
            return string.Format(
                "accesstime={0};blockSize={1};group={2};length={3};modificationtime={4};owner={5};" +
                "pathSuffix={6};permission={7};replication={8};type={9}",
                new object[] { accessTime, blockSize, group, length, modificationTime, owner,
                pathSuffix, permission, replication, type }
            );
        }  
    }
}

using System;
namespace WebHDFS
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_Properties
    /// </summary>
    public class FileStatusClass
    {
        /// <summary>
        /// Gets or sets the file status.
        /// </summary>
        /// <value>The file status.</value>
        public FileStatus FileStatus { get; set; }
    }

    /// <summary>
    /// File status.
    /// </summary>
    public class FileStatus
    {
        /// <summary>
        /// Gets or sets the access time.
        /// </summary>
        /// <value>The access time.</value>
        public long accessTime { get; set; }

        /// <summary>
        /// Gets or sets the size of the block.
        /// </summary>
        /// <value>The size of the block.</value>
        public int blockSize { get; set; }

        /// <summary>
        /// Gets or sets the group.
        /// </summary>
        /// <value>The group.</value>
        public string group { get; set; }

        /// <summary>
        /// Gets or sets the length.
        /// </summary>
        /// <value>The length.</value>
        public long length { get; set; }

        /// <summary>
        /// Gets or sets the modification time.
        /// </summary>
        /// <value>The modification time.</value>
        public long modificationTime { get; set; }

        /// <summary>
        /// Gets or sets the owner.
        /// </summary>
        /// <value>The owner.</value>
        public string owner { get; set; }

        /// <summary>
        /// Gets or sets the path suffix.
        /// </summary>
        /// <value>The path suffix.</value>
        public string pathSuffix { get; set; }

        /// <summary>
        /// Gets or sets the permission.
        /// </summary>
        /// <value>The permission.</value>
        public string permission { get; set; }

        /// <summary>
        /// Gets or sets the replication.
        /// </summary>
        /// <value>The replication.</value>
        public int replication { get; set; }

        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        public string type { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileStatus"/>.
        /// </summary>
        /// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileStatus"/>.</returns>
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

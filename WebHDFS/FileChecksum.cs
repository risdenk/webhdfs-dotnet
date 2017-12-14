using System;
namespace WebHDFS
{
    /// <summary>
    /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileChecksum_JSON_Schema
    /// </summary>
    public class FileChecksumClass
    {
        /// <summary>
        /// Gets or sets the file checksum.
        /// </summary>
        /// <value>The file checksum.</value>
        public FileChecksum FileChecksum { get; set; }
    }

    /// <summary>
    /// File checksum.
    /// </summary>
    public class FileChecksum
    {
        /// <summary>
        /// Gets or sets the algorithm.
        /// </summary>
        /// <value>The algorithm.</value>
        public string algorithm { get; set; }

        /// <summary>
        /// Gets or sets the bytes.
        /// </summary>
        /// <value>The bytes.</value>
        public string bytes { get; set; }

        /// <summary>
        /// Gets or sets the length.
        /// </summary>
        /// <value>The length.</value>
        public int length { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileChecksum"/>.
        /// </summary>
        /// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.FileChecksum"/>.</returns>
        public override string ToString()
        {
            return string.Format("algorithm={0};bytes={1};length={2}", 
                                 new object[] {algorithm, bytes, length});
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text.RegularExpressions;

namespace knoxdotnetdsl
{
    public class WebHDFSHttpQueryParameter
    {
        // Check if octal pattern for permissions
        private static readonly string permissionPattern = "^[0-1]?[0-7]?[0-7]?[0-7]$";

        public sealed class Op
        {
            // GET
            public static readonly Op OPEN = new Op("OPEN");
            public static readonly Op GETFILESTATUS = new Op("GETFILESTATUS");
            public static readonly Op LISTSTATUS = new Op("LISTSTATUS");

            // PUT
            public static readonly Op CREATE = new Op("CREATE");

            private Op(string value)
            {
                Value = value;
            }

            public string Value { get; private set; }
        }

        /// <summary>
        /// Sets the op paramter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Op
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="operation">Operation.</param>
        public static NameValueCollection setOp(NameValueCollection query, Op operation)
        {
            query.Set("op", operation.Value);
            return query;
        }

        /// <summary>
        /// Sets the overwrite parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Overwrite
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="overwrite">Overwrite.</param>
        public static NameValueCollection setOverwrite(NameValueCollection query, Nullable<bool> overwrite)
        {
            query.Set("overwrite", overwrite.GetValueOrDefault(false).ToString().ToLower());
            return query;
        }

        /// <summary>
        /// Sets the blocksize parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Block_Size
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="blocksize">Blocksize.</param>
        public static NameValueCollection setBlocksize(NameValueCollection query, Nullable<long> blocksize)
        {
            if (blocksize != null)
            {
                query.Set("blocksize", blocksize.ToString());
            }
            return query;
        }

        /// <summary>
        /// Sets the replication parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Replication
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="replication">Replication.</param>
        public static NameValueCollection setReplication(NameValueCollection query, Nullable<short> replication)
        {
            if (replication != null)
            {
                query.Set("replication", replication.ToString());
            }
            return query;
        }

        /// <summary>
        /// Sets the permission parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="permission">Permission.</param>
        public static NameValueCollection setPermission(NameValueCollection query, String permission)
        {
            if (permission != null)
            {
                if (Regex.Match(permission, permissionPattern).Success)
                {
                    query.Set("permission", permission);
                } else {
                    throw new ArgumentException(
                        "Invalid permission specified. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Permission"
                    );
                }
            }
            return query;
        }

        /// <summary>
        /// Sets the buffersize parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="buffersize">Buffer Size.</param>
        public static NameValueCollection setBuffersize(NameValueCollection query, Nullable<int> buffersize)
        {
            if (buffersize != null)
            {
                if (buffersize > 0)
                {
                    query.Set("buffersize", buffersize.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "Buffersize must be greater than 0" +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size"
                    );
                }
            }
            return query;
        }
    }
}

using System;
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
            public static readonly Op GETCONTENTSUMMARY = new Op("GETCONTENTSUMMARY");
            public static readonly Op GETFILECHECKSUM = new Op("GETFILECHECKSUM");
            public static readonly Op GETHOMEDIRECTORY = new Op("GETHOMEDIRECTORY");
            public static readonly Op GETDELEGATIONTOKEN = new Op("GETDELEGATIONTOKEN");
            public static readonly Op GETXATTRS = new Op("GETXATTRS");
            public static readonly Op LISTXATTRS = new Op("LISTXATTRS");
            public static readonly Op CHECKACCESS = new Op("CHECKACCESS");
            public static readonly Op GETALLSTORAGEPOLICY = new Op("GETALLSTORAGEPOLICY");
            public static readonly Op GETSTORAGEPOLICY = new Op("GETSTORAGEPOLICY");

            // PUT
            public static readonly Op CREATE = new Op("CREATE");
            public static readonly Op MKDIRS = new Op("MKDIRS");
            public static readonly Op CREATESYMLINK = new Op("CREATESYMLINK");
            public static readonly Op RENAME = new Op("RENAME");
            public static readonly Op SETREPLICATION = new Op("SETREPLICATION");
            public static readonly Op SETOWNER = new Op("SETOWNER");
            public static readonly Op SETPERMISSION = new Op("SETPERMISSION");
            public static readonly Op SETTIMES = new Op("SETTIMES");
            public static readonly Op RENEWDELEGATIONTOKEN = new Op("RENEWDELEGATIONTOKEN");
            public static readonly Op CANCELDELEGATIONTOKEN = new Op("CANCELDELEGATIONTOKEN");
            public static readonly Op CREATESNAPSHOT = new Op("CREATESNAPSHOT");
            public static readonly Op RENAMESNAPSHOT = new Op("RENAMESNAPSHOT");
            public static readonly Op SETXATTR = new Op("SETXATTR");
            public static readonly Op REMOVEXATTR = new Op("REMOVEXATTR");
            public static readonly Op SETSTORAGEPOLICY = new Op("SETSTORAGEPOLICY");

            // POST
            public static readonly Op APPEND = new Op("APPEND");
            public static readonly Op CONCAT = new Op("CONCAT");
            public static readonly Op TRUNCATE = new Op("TRUNCATE");
            public static readonly Op UNSETSTORAGEPOLICY = new Op("UNSETSTORAGEPOLICY");

            // DELETE
            public static readonly Op DELETE = new Op("DELETE");
            public static readonly Op DELETESNAPSHOT = new Op("DELETESNAPSHOT");

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
                        "Buffersize must be greater than 0. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Buffer_Size"
                    );
                }
            }
            return query;
        }

        /// <summary>
        /// Sets the offset parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="offset">Offset.</param>
        public static NameValueCollection setOffset(NameValueCollection query, Nullable<long> offset)
        {
            if ( offset == null) 
            {
                query.Set("offset", "null");
            } 
            else if (offset >= 0) 
            {
                query.Set("offset", offset.ToString());
            } 
            else 
            {
                throw new ArgumentException(
                    "Offset must be greater than or equal to 0 or null. " +
                    "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset"
                );
            }
            return query;
        }

        /// <summary>
        /// Sets the length parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Length
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="offset">Length.</param>
        public static NameValueCollection setLength(NameValueCollection query, Nullable<long> length)
        {
            if (length != null)
            {
                if (length >= 0)
                {
                    query.Set("length", length.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "Length must be greater than or equal to 0. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Offset"
                    );
                }
            }
            return query;
        }
    }
}

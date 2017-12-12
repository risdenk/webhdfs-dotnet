using System;
using System.Collections.Specialized;
using System.Text.RegularExpressions;

namespace WebHDFS
{
    public static class WebHDFSHttpQueryParameter
    {
        // Check if octal pattern for permissions
        static readonly string permissionPattern = "^[0-1]?[0-7]?[0-7]?[0-7]$";

        // Check if octal pattern for permissions
        static readonly string fsactionPattern = "^[r-][w-][x-]$";

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

            Op(string value)
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
        public static NameValueCollection SetOp(NameValueCollection query, Op operation)
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
        public static NameValueCollection SetOverwrite(NameValueCollection query, bool? overwrite)
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
        public static NameValueCollection setBlocksize(NameValueCollection query, long? blocksize)
        {
            if (blocksize != null)
            {
                if (blocksize > 0)
                {
                    query.Set("blocksize", blocksize.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "Blocksize must be greater than 0. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Block_Size"
                    );
                }
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
        public static NameValueCollection SetReplication(NameValueCollection query, short? replication)
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
        public static NameValueCollection SetPermission(NameValueCollection query, String permission)
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
        public static NameValueCollection SetBuffersize(NameValueCollection query, int? buffersize)
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
        public static NameValueCollection SetOffset(NameValueCollection query, long? offset)
        {
            if (offset != null)
            {
                if (offset >= 0)
                {
                    query.Set("offset", offset.ToString());
                    return query;
                }
                throw new ArgumentException(
                    "Offset must be greater than or equal to 0. " +
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
        /// <param name="length">Length.</param>
        public static NameValueCollection SetLength(NameValueCollection query, long? length)
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

        /// <summary>
        /// Sets the destination parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Destination
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="destination">Destination.</param>
        public static NameValueCollection SetDestination(NameValueCollection query, string destination)
        {
            if (!string.IsNullOrEmpty(destination))
            {
                query.Set("destination", destination);
            }
            return query;
        }

        /// <summary>
        /// Sets the recursive parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Recursive
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="recursive">Recursive.</param>
        public static NameValueCollection SetRecursive(NameValueCollection query, bool? recursive)
        {
            if (recursive != null)
            {
                query.Set("recursive", recursive.ToString().ToLower());
            }
            return query;
        }

        /// <summary>
        /// Sets the newlength parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#New_Length
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="newlength">newlength.</param>
        public static NameValueCollection SetNewLength(NameValueCollection query, long? newlength)
        {
            if (newlength != null)
            {
                if (newlength >= 0)
                {
                    query.Set("newlength", newlength.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "newlength must be greater than or equal to 0. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#New_Length"
                    );
                }
            }
            return query;
        }

        /// <summary>
        /// Sets the sources parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Sources
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="sources">Sources.</param>
        public static NameValueCollection SetSources(NameValueCollection query, string sources)
        {
            if (!string.IsNullOrEmpty(sources))
            {
                query.Set("sources", sources);
            }
            return query;
        }

        /// <summary>
        /// Sets the createParent parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_Parent
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="createParent">createParent.</param>
        public static NameValueCollection SetCreateParent(NameValueCollection query, bool? createParent)
        {
            if (createParent != null)
            {
                query.Set("createParent", createParent.ToString().ToLower());
            }
            return query;
        }

        /// <summary>
        /// Sets the owner parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Owner
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="owner">Owner.</param>
        public static NameValueCollection SetOwner(NameValueCollection query, string owner)
        {
            if (!string.IsNullOrEmpty(owner))
            {
                query.Set("owner", owner);
            }
            return query;
        }

        /// <summary>
        /// Sets the group parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Group
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="group">Group.</param>
        public static NameValueCollection SetGroup(NameValueCollection query, string group)
        {
            if (!string.IsNullOrEmpty(group))
            {
                query.Set("group", group);
            }
            return query;
        }

        /// <summary>
        /// Sets the modificationtime parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Modification_Time
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="modificationtime">modificationtime.</param>
        public static NameValueCollection SetModificationTime(NameValueCollection query, long? modificationtime)
        {
            if (modificationtime != null)
            {
                if (modificationtime >= -1)
                {
                    query.Set("modificationtime", modificationtime.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "modificationtime must be greater than or equal to -1. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Modification_Time"
                    );
                }
            }
            return query;
        }

        /// <summary>
        /// Sets the accesstime parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Access_Time
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="accesstime">accesstime.</param>
        public static NameValueCollection SetAccessTime(NameValueCollection query, long? accesstime)
        {
            if (accesstime != null)
            {
                if (accesstime >= -1)
                {
                    query.Set("accesstime", accesstime.ToString());
                }
                else
                {
                    throw new ArgumentException(
                        "accesstime must be greater than or equal to -1. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Access_Time"
                    );
                }
            }
            return query;
        }

        /// <summary>
        /// Sets the fsaction parameter on the query.
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Check_access
        /// </summary>
        /// <returns>The query.</returns>
        /// <param name="query">Query.</param>
        /// <param name="fsaction">fsaction.</param>
        public static NameValueCollection SetFSAction(NameValueCollection query, String fsaction)
        {
            if (fsaction != null)
            {
                if (Regex.Match(fsaction, fsactionPattern).Success)
                {
                    query.Set("fsaction", fsaction);
                }
                else
                {
                    throw new ArgumentException(
                        "Invalid fsaction specified. " +
                        "See https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Check_access"
                    );
                }
            }
            return query;
        }
    }
}

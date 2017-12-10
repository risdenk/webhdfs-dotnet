using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization.Json;
using System.Web;

namespace knoxdotnetdsl
{
    public class WebHDFS
    {
        private string _baseAPI;

        public WebHDFS(string BaseAPI) {
            _baseAPI = BaseAPI;
        }

        public NetworkCredential Credentials { get; set; }

        private HttpClientHandler getHttpClientHandler(bool AllowRedirect=true) {
            return new HttpClientHandler() 
            {
                AllowAutoRedirect = AllowRedirect,
                Credentials = Credentials,
                PreAuthenticate = true
            };
        }

        private HttpClient getHttpClient(HttpClientHandler handler) {
            return new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(10)
            };
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
        /// </summary>
        public bool UploadFile(
            string filePath,
            string path,
            bool overwrite = false,
            Nullable<long> blocksize = null,
            Nullable<short> replication = null,
            string permission = null,
            Nullable<int> buffersize = null)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Open))
            {
                return WriteStream(fileStream, path, overwrite, blocksize, replication, permission, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool WriteStream(
            Stream stream,
            string path,
            bool overwrite = false,
            Nullable<long> blocksize = null,
            Nullable<short> replication = null,
            string permission = null,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.CREATE);
            WebHDFSHttpQueryParameter.SetOverwrite(query, overwrite);
            WebHDFSHttpQueryParameter.setBlocksize(query, blocksize);
            WebHDFSHttpQueryParameter.SetReplication(query, replication);
            WebHDFSHttpQueryParameter.SetPermission(query, permission);
            WebHDFSHttpQueryParameter.SetBuffersize(query, buffersize);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] {})).Result;
                    if(response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect)) {
                        var response2 = client.PutAsync(response.Headers.Location, new StreamContent(stream)).Result;
                        return response2.IsSuccessStatusCode;
                    } else {
                        throw new InvalidOperationException("Should get a 307. Instead we got: " + 
                                                            response.StatusCode + " " + response.ReasonPhrase);
                    }
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
        /// </summary>
        public bool AppendFile(
            string filePath,
            string path,
            Nullable<int> buffersize = null)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Open))
            {
                return AppendStream(fileStream, path, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool AppendStream(
            Stream stream,
            string path,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.APPEND);
            WebHDFSHttpQueryParameter.SetBuffersize(query, buffersize);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PostAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = client.PostAsync(response.Headers.Location, new StreamContent(stream)).Result;
                        return response2.IsSuccessStatusCode;
                    }
                    else
                    {
                        throw new InvalidOperationException("Should get a 307. Instead we got: " +
                                                            response.StatusCode + " " + response.ReasonPhrase);
                    }
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File
        /// </summary>
        public bool DownloadFile(
            string filePath,
            string path,
            Nullable<long> offset = null,
            Nullable<long> length = null,
            Nullable<int> buffersize = null)
        {
            string pathname = Path.GetFullPath(filePath);
            if (File.Exists(pathname))
            {
                throw new InvalidOperationException(string.Format("File {0} already exists.", pathname));
            }

            using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                return ReadStream(fileStream, path, offset, length, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool ReadStream(
            Stream stream,
            string path,
            Nullable<long> offset = null,
            Nullable<long> length = null,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.OPEN);
            WebHDFSHttpQueryParameter.SetOffset(query, offset);
            WebHDFSHttpQueryParameter.SetLength(query, length);
            WebHDFSHttpQueryParameter.SetBuffersize(query, buffersize);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = client.GetAsync(response.Headers.Location).Result;
                        if (response2.IsSuccessStatusCode)
                        {
                            response2.Content.CopyToAsync(stream).Wait();
                            return true;
                        }
                        return false;
                    }
                    else
                    {
                        throw new InvalidOperationException("Should get a 307. Instead we got: " +
                                                            response.StatusCode + " " + response.ReasonPhrase);
                    }
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Make_a_Directory
        /// </summary>
        public Boolean MakeDirectory(
            string path,
            string permission = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.MKDIRS);
            WebHDFSHttpQueryParameter.SetPermission(query, permission);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Status_of_a_FileDirectory
        /// </summary>
        public FileStatus GetFileStatus(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.GETFILESTATUS);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(FileStatusClass));
                    return ((FileStatusClass)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result)).FileStatus;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory
        /// </summary>
        public FileStatuses ListStatus(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.LISTSTATUS);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(FileStatusesClass));
                    return ((FileStatusesClass)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result)).FileStatuses;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_Content_Summary_of_a_Directory
        /// </summary>
        public ContentSummary GetContentSummary(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.GETCONTENTSUMMARY);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(ContentSummaryClass));
                    return ((ContentSummaryClass)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result)).ContentSummary;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Get_File_Checksum
        /// </summary>
        public FileChecksum GetFileChecksum(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.GETFILECHECKSUM);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = client.GetAsync(response.Headers.Location).Result;
                        response2.EnsureSuccessStatusCode();
                        var serializer = new DataContractJsonSerializer(typeof(FileChecksumClass));
                        return ((FileChecksumClass)serializer.ReadObject(response2.Content.ReadAsStreamAsync().Result)).FileChecksum;
                    }
                    else
                    {
                        throw new InvalidOperationException("Should get a 307. Instead we got: " +
                                                            response.StatusCode + " " + response.ReasonPhrase);
                    }
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Rename_a_FileDirectory
        /// </summary>
        public Boolean Rename(
            string path,
            string destination)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.RENAME);
            WebHDFSHttpQueryParameter.SetDestination(query, destination);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Delete_a_FileDirectory
        /// </summary>
        public Boolean Delete(
            string path,
            Nullable<bool> recursive = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.DELETE);
            WebHDFSHttpQueryParameter.SetRecursive(query, recursive);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.DeleteAsync(requestPath).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Truncate_a_File
        /// </summary>
        public Boolean Truncate(
            string path,
            long newlength)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.TRUNCATE);
            WebHDFSHttpQueryParameter.SetNewLength(query, newlength);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PostAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Concat_Files
        /// </summary>
        public bool Concat(
            string path,
            string sources)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.CONCAT);
            WebHDFSHttpQueryParameter.SetSources(query, sources);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PostAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    return response.IsSuccessStatusCode;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_a_Symbolic_Link
        /// </summary>
        public bool CreateSymlink(
            string path,
            string destination,
            Nullable<bool> createParent)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.CREATESYMLINK);
            WebHDFSHttpQueryParameter.SetDestination(query, destination);
            WebHDFSHttpQueryParameter.SetCreateParent(query, createParent);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    return response.IsSuccessStatusCode;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Status_of_a_FileDirectory
        /// </summary>
        public string GetHomeDirectory()
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.GETHOMEDIRECTORY);

            string requestPath = _baseAPI + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(PathClass));
                    return ((PathClass)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result)).Path;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Permission
        /// </summary>
        public bool SetPermission(
            string path,
            string permission = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.SETPERMISSION);
            WebHDFSHttpQueryParameter.SetPermission(query, permission);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    return response.IsSuccessStatusCode;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Owner
        /// </summary>
        public bool SetOwner(
            string path,
            string owner = null,
            string group = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.SETOWNER);
            WebHDFSHttpQueryParameter.SetOwner(query, owner);
            WebHDFSHttpQueryParameter.SetGroup(query, group);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    return response.IsSuccessStatusCode;
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Replication_Factor
        /// </summary>
        public Boolean SetReplication(
            string path,
            Nullable<short> replication = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.SETREPLICATION);
            WebHDFSHttpQueryParameter.SetReplication(query, replication);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Set_Replication_Factor
        /// </summary>
        public Boolean SetTimes(
            string path,
            Nullable<short> modificationtime = null,
            Nullable<short> accesstime = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.SETTIMES);
            WebHDFSHttpQueryParameter.SetModificationTime(query, modificationtime);
            WebHDFSHttpQueryParameter.SetAccessTime(query, accesstime);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    var serializer = new DataContractJsonSerializer(typeof(Boolean));
                    return ((Boolean)serializer.ReadObject(response.Content.ReadAsStreamAsync().Result));
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Check_access
        /// </summary>
        public bool CheckAccess(
            string path,
            string fsaction)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.CHECKACCESS);
            WebHDFSHttpQueryParameter.SetFSAction(query, fsaction);

            string requestPath = _baseAPI + path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    return response.IsSuccessStatusCode;
                }
            }
        }
    }
}

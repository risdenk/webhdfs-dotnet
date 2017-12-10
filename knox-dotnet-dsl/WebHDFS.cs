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
        private Uri _baseAPI;
        private NetworkCredential _credential;

        public WebHDFS(Uri BaseAPI, NetworkCredential Credential = null) {
            _baseAPI = BaseAPI;
            _credential = Credential;
        }

        private HttpClientHandler getHttpClientHandler(bool AllowRedirect=true) {
            return new HttpClientHandler() 
            {
                AllowAutoRedirect = AllowRedirect,
                Credentials = _credential,
                PreAuthenticate = true
            };
        }

        private HttpClient getHttpClient(HttpClientHandler handler) {
            return new HttpClient(handler)
            {
                BaseAddress = _baseAPI,
                Timeout = TimeSpan.FromMinutes(10)
            };
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
        /// </summary>
        public HttpResponseMessage CreateFile(
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
                return CreateFile(fileStream, path, overwrite, blocksize, replication, permission, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public HttpResponseMessage CreateFile(
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

            string requestPath = path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] {})).Result;
                    if(response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect)) {
                        var response2 = client.PutAsync(response.Headers.Location, new StreamContent(stream)).Result;
                        return response2.EnsureSuccessStatusCode();
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
                return AppendFile(fileStream, path, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool AppendFile(
            Stream stream,
            string path,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.APPEND);
            WebHDFSHttpQueryParameter.SetBuffersize(query, buffersize);

            string requestPath = path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PostAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = client.PostAsync(response.Headers.Location, new StreamContent(stream)).Result;
                        response2.EnsureSuccessStatusCode();
                        return true;
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
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool ReadFile(
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
                return ReadFile(fileStream, path, offset, length, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public bool ReadFile(
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

            string requestPath = path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.GetAsync(requestPath).Result;
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = client.GetAsync(response.Headers.Location).Result;
                        response2.EnsureSuccessStatusCode();
                        response2.Content.CopyToAsync(stream).RunSynchronously();
                        return true;
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
        public string MakeDirectory(
            string path,
            string permission = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.MKDIRS);
            WebHDFSHttpQueryParameter.SetPermission(query, permission);

            string requestPath = path + '?' + query;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = client.PutAsync(requestPath, new ByteArrayContent(new byte[] { })).Result;
                    response.EnsureSuccessStatusCode();
                    return response.Content.ReadAsStringAsync().Result;
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

            string requestPath = path + '?' + query;

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

            string requestPath = path + '?' + query;

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

            string requestPath = path + '?' + query;

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

            string requestPath = path + '?' + query;

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
    }
}

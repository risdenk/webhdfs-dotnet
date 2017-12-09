using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;

namespace knoxdotnetdsl
{
    public class WebHDFS
    {
        private Uri _baseAPI;
        private NetworkCredential _credential;

        public WebHDFS(Uri BaseAPI, String Username=null, String Password=null) {
            _baseAPI = BaseAPI;
            if (Username != null && Password != null)
            {
                _credential = new NetworkCredential(Username, Password);
            }
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
        public async Task<HttpResponseMessage> CreateFileAsync(
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
                return await CreateFileAsync(fileStream, path, overwrite, blocksize, replication, permission, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public async Task<HttpResponseMessage> CreateFileAsync(
            Stream stream,
            string path,
            bool overwrite = false,
            Nullable<long> blocksize = null,
            Nullable<short> replication = null,
            string permission = null,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.CREATE);
            WebHDFSHttpQueryParameter.setOverwrite(query, overwrite);
            WebHDFSHttpQueryParameter.setBlocksize(query, blocksize);
            WebHDFSHttpQueryParameter.setReplication(query, replication);
            WebHDFSHttpQueryParameter.setPermission(query, permission);
            WebHDFSHttpQueryParameter.setBuffersize(query, buffersize);

            string requestPath = new Uri(path + '?' + query).PathAndQuery;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = await client.PutAsync(requestPath, new ByteArrayContent(new byte[] {}));
                    if(response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect)) {
                        var response2 = await client.PutAsync(response.Headers.Location, new StreamContent(stream));
                        return response2.EnsureSuccessStatusCode();
                    } else {
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
        public async Task<bool> ReadFileAsync(
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
                return await ReadFileAsync(fileStream, path, offset, length, buffersize);
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File
        /// Note: You are responsible for closing the stream you send in.
        /// </summary>
        public async Task<bool> ReadFileAsync(
            Stream stream,
            string path,
            Nullable<long> offset = null,
            Nullable<long> length = null,
            Nullable<int> buffersize = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.OPEN);
            WebHDFSHttpQueryParameter.setOffset(query, offset);
            WebHDFSHttpQueryParameter.setLength(query, length);
            WebHDFSHttpQueryParameter.setBuffersize(query, buffersize);

            string requestPath = new Uri(path + '?' + query).PathAndQuery;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = await client.GetAsync(requestPath);
                    if (response.StatusCode.Equals(HttpStatusCode.TemporaryRedirect))
                    {
                        var response2 = await client.GetAsync(response.Headers.Location);
                        response2.EnsureSuccessStatusCode();
                        await response2.Content.CopyToAsync(stream);
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
        public async Task<string> MakeDirectoryAsync(
            string path,
            string permission = null)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.MKDIRS);
            WebHDFSHttpQueryParameter.setPermission(query, permission);

            string requestPath = new Uri(path + '?' + query).PathAndQuery;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = await client.PutAsync(requestPath, new ByteArrayContent(new byte[] { }));
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadAsStringAsync();
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Status_of_a_FileDirectory
        /// </summary>
        public async Task<string> GetFileStatusAsync(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.GETFILESTATUS);

            string requestPath = new Uri(path + '?' + query).PathAndQuery;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = await client.GetAsync(requestPath);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadAsStringAsync();
                }
            }
        }

        /// <summary>
        /// https://hadoop.apache.org/docs/r2.8.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory
        /// </summary>
        public async Task<string> ListStatusAsync(
            string path)
        {
            var query = HttpUtility.ParseQueryString(string.Empty);
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.LISTSTATUS);

            string requestPath = new Uri(path + '?' + query).PathAndQuery;

            using (var handler = getHttpClientHandler(false))
            {
                using (var client = getHttpClient(handler))
                {
                    var response = await client.GetAsync(requestPath);
                    response.EnsureSuccessStatusCode();
                    return await response.Content.ReadAsStringAsync();
                }
            }
        }
    }
}

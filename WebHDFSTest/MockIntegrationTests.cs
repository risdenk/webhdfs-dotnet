using Xunit;
using System;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.Hosting;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http;
using Xunit.Abstractions;
using System.Runtime.Serialization.Json;
using System.IO;
using System.Text;
using System.Security.Claims;
using System.Threading.Tasks;
using System.Threading;
using System.Net.Http.Headers;
using Microsoft.Net.Http.Headers;
using System.Net;
using System.Collections.Generic;

namespace WebHDFS.Test
{
    public class MockIntegrationTests
    {
        static readonly Random _random = new Random();

        readonly string username = GenerateRandomString();
        readonly string password = GenerateRandomString();

        readonly ITestOutputHelper output;
        readonly TestServer _server;
        readonly WebHDFSClient _webhdfs;

        readonly ContentSummary ExpectedContentSummary = new ContentSummary
        {
            directoryCount = _random.Next(),
            fileCount = _random.Next(),
            length = _random.Next(),
            quota = _random.Next(),
            spaceConsumed = _random.Next(),
            spaceQuota = _random.Next(),
            typeQuota = new TypeQuota
            {
                ARCHIVE = new ARCHIVE
                {
                    consumed = _random.Next(),
                    quota = _random.Next()
                },
                DISK = new DISK
                {
                    consumed = _random.Next(),
                    quota = _random.Next()
                },
                SSD = new SSD
                {
                    consumed = _random.Next(),
                    quota = _random.Next()
                }
            }
        };

        readonly FileChecksum ExpectedFileChecksum = new FileChecksum
        {
            algorithm = GenerateRandomString(),
            bytes = GenerateRandomString(),
            length = _random.Next()
        };

        readonly FileStatus ExpectedFileStatus = new FileStatus
        {
            accessTime = _random.Next(),
            blockSize = _random.Next(),
            group = GenerateRandomString(),
            length = _random.Next(),
            modificationTime = _random.Next(),
            owner = GenerateRandomString(),
            pathSuffix = GenerateRandomString(),
            permission = GenerateRandomString(),
            replication = _random.Next(5),
            type = "DIRECTORY"
        };

        readonly string ExpectedHomeDirectory = GenerateRandomString();

        readonly FileStatuses ExpectedListStatus = new FileStatuses
        {
            FileStatus = new List<FileStatus> {
                new FileStatus {
                    accessTime = _random.Next(),
                    blockSize = _random.Next(),
                    group = GenerateRandomString(),
                    length = _random.Next(),
                    modificationTime = _random.Next(),
                    owner = GenerateRandomString(),
                    pathSuffix = GenerateRandomString(),
                    permission = GenerateRandomString(),
                    replication = _random.Next(5),
                    type = "FILE"
                },
                new FileStatus {
                    accessTime = _random.Next(),
                    blockSize = _random.Next(),
                    group = GenerateRandomString(),
                    length = _random.Next(),
                    modificationTime = _random.Next(),
                    owner = GenerateRandomString(),
                    pathSuffix = GenerateRandomString(),
                    permission = GenerateRandomString(),
                    replication = _random.Next(5),
                    type = "DIRECTORY"
                }
            }
        };

        public MockIntegrationTests(ITestOutputHelper output)
        {
            this.output = output;

            _server = new TestServer(new WebHostBuilder()
                                     .Configure(Configure)
                                     .ConfigureServices(ConfigureServices));

            var authenticationHeaderValue = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(
                Encoding.UTF8.GetBytes(string.Format("{0}:{1}", username, password))));

            _webhdfs = new WebHDFSClient(_server.BaseAddress.AbsoluteUri)
            {
                CustomHttpMessageHandler = new AuthenticatedHttpMessageHandler(
                    authenticationHeaderValue,
                    _server.CreateHandler())
            };
        }

        [Fact]
        public void TestContentSummary()
        {
            var ActualContentSummary = _webhdfs.GetContentSummary("/ContentSummary").Result;
            Assert.Equal(ExpectedContentSummary.directoryCount, ActualContentSummary.directoryCount);
            Assert.Equal(ExpectedContentSummary.fileCount, ActualContentSummary.fileCount);
            Assert.Equal(ExpectedContentSummary.length, ActualContentSummary.length);
            Assert.Equal(ExpectedContentSummary.quota, ActualContentSummary.quota);
            Assert.Equal(ExpectedContentSummary.spaceConsumed, ActualContentSummary.spaceConsumed);
            Assert.Equal(ExpectedContentSummary.spaceQuota, ActualContentSummary.spaceQuota);
            Assert.Equal(ExpectedContentSummary.typeQuota.ARCHIVE.consumed,
                         ActualContentSummary.typeQuota.ARCHIVE.consumed);
            Assert.Equal(ExpectedContentSummary.typeQuota.ARCHIVE.quota,
                         ActualContentSummary.typeQuota.ARCHIVE.quota);
            Assert.Equal(ExpectedContentSummary.typeQuota.DISK.consumed,
                         ActualContentSummary.typeQuota.DISK.consumed);
            Assert.Equal(ExpectedContentSummary.typeQuota.DISK.quota,
                         ActualContentSummary.typeQuota.DISK.quota);
            Assert.Equal(ExpectedContentSummary.typeQuota.SSD.consumed,
                         ActualContentSummary.typeQuota.SSD.consumed);
            Assert.Equal(ExpectedContentSummary.typeQuota.SSD.quota,
                         ActualContentSummary.typeQuota.SSD.quota);
        }

        [Fact]
        public void TestFileChecksum()
        {
            var ActualFileChecksum = _webhdfs.GetFileChecksum("/FileChecksum").Result;
            Assert.Equal(ExpectedFileChecksum.algorithm, ActualFileChecksum.algorithm);
            Assert.Equal(ExpectedFileChecksum.bytes, ActualFileChecksum.bytes);
            Assert.Equal(ExpectedFileChecksum.length, ActualFileChecksum.length);
        }

        [Fact]
        public void TestFileStatus()
        {
            var ActualFileStatus = _webhdfs.GetFileStatus("/FileStatus").Result;
            Assert.Equal(ExpectedFileStatus.accessTime, ActualFileStatus.accessTime);
            Assert.Equal(ExpectedFileStatus.blockSize, ActualFileStatus.blockSize);
            Assert.Equal(ExpectedFileStatus.group, ActualFileStatus.group);
            Assert.Equal(ExpectedFileStatus.length, ActualFileStatus.length);
            Assert.Equal(ExpectedFileStatus.modificationTime, ActualFileStatus.modificationTime);
            Assert.Equal(ExpectedFileStatus.owner, ActualFileStatus.owner);
            Assert.Equal(ExpectedFileStatus.pathSuffix, ActualFileStatus.pathSuffix);
            Assert.Equal(ExpectedFileStatus.permission, ActualFileStatus.permission);
            Assert.Equal(ExpectedFileStatus.replication, ActualFileStatus.replication);
            Assert.Equal(ExpectedFileStatus.type, ActualFileStatus.type);
        }

        [Fact]
        public void TestHomeDirectory()
        {
            var ActualHomeDirectory = _webhdfs.GetHomeDirectory().Result;
            Assert.Equal(ExpectedHomeDirectory, ActualHomeDirectory);
        }

        [Fact]
        public void TestListStatus()
        {
            var ActualListStatus = _webhdfs.ListStatus("/ListStatus").Result;
            Assert.Equal(ExpectedListStatus.FileStatus.Count, ActualListStatus.FileStatus.Count);
            Assert.Equal(ExpectedListStatus.FileStatus[0].accessTime, ActualListStatus.FileStatus[0].accessTime);
            Assert.Equal(ExpectedListStatus.FileStatus[0].blockSize, ActualListStatus.FileStatus[0].blockSize);
            Assert.Equal(ExpectedListStatus.FileStatus[0].group, ActualListStatus.FileStatus[0].group);
            Assert.Equal(ExpectedListStatus.FileStatus[0].length, ActualListStatus.FileStatus[0].length);
            Assert.Equal(ExpectedListStatus.FileStatus[0].modificationTime, ActualListStatus.FileStatus[0].modificationTime);
            Assert.Equal(ExpectedListStatus.FileStatus[0].owner, ActualListStatus.FileStatus[0].owner);
            Assert.Equal(ExpectedListStatus.FileStatus[0].pathSuffix, ActualListStatus.FileStatus[0].pathSuffix);
            Assert.Equal(ExpectedListStatus.FileStatus[0].permission, ActualListStatus.FileStatus[0].permission);
            Assert.Equal(ExpectedListStatus.FileStatus[0].replication, ActualListStatus.FileStatus[0].replication);
            Assert.Equal(ExpectedListStatus.FileStatus[0].type, ActualListStatus.FileStatus[0].type);
            Assert.Equal(ExpectedListStatus.FileStatus[1].accessTime, ActualListStatus.FileStatus[1].accessTime);
            Assert.Equal(ExpectedListStatus.FileStatus[1].blockSize, ActualListStatus.FileStatus[1].blockSize);
            Assert.Equal(ExpectedListStatus.FileStatus[1].group, ActualListStatus.FileStatus[1].group);
            Assert.Equal(ExpectedListStatus.FileStatus[1].length, ActualListStatus.FileStatus[1].length);
            Assert.Equal(ExpectedListStatus.FileStatus[1].modificationTime, ActualListStatus.FileStatus[1].modificationTime);
            Assert.Equal(ExpectedListStatus.FileStatus[1].owner, ActualListStatus.FileStatus[1].owner);
            Assert.Equal(ExpectedListStatus.FileStatus[1].pathSuffix, ActualListStatus.FileStatus[1].pathSuffix);
            Assert.Equal(ExpectedListStatus.FileStatus[1].permission, ActualListStatus.FileStatus[1].permission);
            Assert.Equal(ExpectedListStatus.FileStatus[1].replication, ActualListStatus.FileStatus[1].replication);
            Assert.Equal(ExpectedListStatus.FileStatus[1].type, ActualListStatus.FileStatus[1].type);
        }

        void Configure(IApplicationBuilder app)
        {
            app.Use(async (context, next) =>
            {
                var authHeader = context.Request.Headers[HeaderNames.Authorization].ToString();
                if (authHeader != null && authHeader.StartsWith("basic", StringComparison.OrdinalIgnoreCase))
                {
                    var token = authHeader.Substring("Basic ".Length).Trim();
                    var credentialstring = Encoding.UTF8.GetString(Convert.FromBase64String(token));
                    var credentials = credentialstring.Split(':');
                    if (credentials[0] == username && credentials[1] == password)
                    {
                        var claims = new[] { new Claim("name", credentials[0]), new Claim(ClaimTypes.Role, "user") };
                        context.User = new ClaimsPrincipal(new ClaimsIdentity(claims, "Basic"));
                    }
                }
                else
                {
                    context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                    context.Response.Headers[HeaderNames.WWWAuthenticate] = "Basic realm=\"testing\"";
                }

                await next.Invoke();
            });

            var routeBuilder = new RouteBuilder(app);

            routeBuilder.MapGet("", (context) =>
            {
                var homeDirectoryClass = new PathClass
                {
                    Path = ExpectedHomeDirectory
                };
                return context.Response.WriteAsync(ObjectToJsonString(homeDirectoryClass));
            });

            routeBuilder.MapGet("ContentSummary", (context) =>
            {
                var contentSummaryClass = new ContentSummaryClass
                {
                    ContentSummary = ExpectedContentSummary
                };
                return context.Response.WriteAsync(ObjectToJsonString(contentSummaryClass));
            });

            routeBuilder.MapGet("FileChecksum", (context) =>
            {
                context.Response.Redirect(_server.BaseAddress + "FileChecksumData");
                context.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
                return context.Response.WriteAsync("");
            });

            routeBuilder.MapGet("FileChecksumData", (context) =>
            {
                var fileChecksumClass = new FileChecksumClass
                {
                    FileChecksum = ExpectedFileChecksum
                };
                return context.Response.WriteAsync(ObjectToJsonString(fileChecksumClass));
            });

            routeBuilder.MapGet("FileStatus", (context) =>
            {
                var fileStatusClass = new FileStatusClass
                {
                    FileStatus = ExpectedFileStatus
                };
                return context.Response.WriteAsync(ObjectToJsonString(fileStatusClass));
            });

            routeBuilder.MapGet("ListStatus", (context) =>
            {
                var listStatusClass = new FileStatusesClass
                {
                    FileStatuses = ExpectedListStatus
                };
                return context.Response.WriteAsync(ObjectToJsonString(listStatusClass));
            });

            app.UseRouter(routeBuilder.Build());
        }

        void ConfigureServices(IServiceCollection services)
        {
            services.AddRouting();
        }

        static string GenerateRandomString()
        {
            var bytes = new Byte[_random.Next(20)];
            _random.NextBytes(bytes);
            return Convert.ToBase64String(bytes);
        }

        static string ObjectToJsonString(Object obj)
        {
            MemoryStream memstream = new MemoryStream();
            var ser = new DataContractJsonSerializer(obj.GetType());
            ser.WriteObject(memstream, obj);
            return Encoding.ASCII.GetString(memstream.ToArray());
        }
    }

    class AuthenticatedHttpMessageHandler : DelegatingHandler
    {
        readonly AuthenticationHeaderValue authenticationHeaderValue;

        public AuthenticatedHttpMessageHandler(AuthenticationHeaderValue authenticationHeaderValue,
                                               HttpMessageHandler httpMessageHandler)
        {
            this.authenticationHeaderValue = authenticationHeaderValue;
            InnerHandler = httpMessageHandler;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                                     CancellationToken cancellationToken)
        {
            request.Headers.Authorization = authenticationHeaderValue;
            return await base.SendAsync(request, cancellationToken);
        }
    }
}
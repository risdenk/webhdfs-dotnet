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

namespace WebHDFS.Test
{
    public class MockIntegrationTests
    {
        readonly ITestOutputHelper output;
        private readonly TestServer _server;
        private readonly HttpClient _client;
        private readonly WebHDFSClient _webhdfs;

        readonly FileStatus ExpectedFileStatus = new FileStatus()
        {
            accessTime = 0,
            blockSize = 0,
            group = "supergroup",
            length = 0,
            modificationTime = 1320173277227,
            owner = "webuser",
            pathSuffix = "",
            permission = "777",
            replication = 0,
            type = "DIRECTORY"
        };

        public MockIntegrationTests(ITestOutputHelper output)
        {
            this.output = output;
            _server = new TestServer(new WebHostBuilder()
                                     .Configure(app => Configure(app))
                                     .ConfigureServices(services => services.AddRouting()));
            _client = _server.CreateClient();
            _webhdfs = new WebHDFSClient(_server.BaseAddress.AbsoluteUri)
            {
                CustomHttpMessageHandler = _server.CreateHandler()
            };
        }

        [Fact]
        public void TestWebHostAsync()
        {
            var response = _client.GetAsync("/hello/webhdfs").Result;
            response.EnsureSuccessStatusCode();
            var data = response.Content.ReadAsStringAsync().Result;
            Assert.Equal("Hi, webhdfs!", data);
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

        void Configure(IApplicationBuilder app)
        {
            var routeBuilder = new RouteBuilder(app);

            routeBuilder.MapGet("hello/{name}", context =>
            {
                var name = context.GetRouteValue("name");
                return context.Response.WriteAsync($"Hi, {name}!");
            });

            routeBuilder.MapGet("FileStatus", (context) =>
            {
                var fileStatusClass = new FileStatusClass()
                {
                    FileStatus = ExpectedFileStatus
                };
                return context.Response.WriteAsync(ObjectToJsonString(fileStatusClass));
            });

            app.UseRouter(routeBuilder.Build());
        }

        private static string ObjectToJsonString(Object obj)
        {
            MemoryStream memstream = new MemoryStream();
            var ser = new DataContractJsonSerializer(obj.GetType());
            ser.WriteObject(memstream, obj);
            return Encoding.ASCII.GetString(memstream.ToArray());
        }
    }
}
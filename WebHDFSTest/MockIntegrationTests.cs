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

namespace WebHDFS.Test
{
    public class MockIntegrationTests
    {
        readonly ITestOutputHelper output;
        private readonly TestServer _server;
        private readonly HttpClient _client;
        private readonly WebHDFSClient _webhdfs;

        public MockIntegrationTests(ITestOutputHelper output)
        {
            this.output = output;
            _server = new TestServer(new WebHostBuilder()
                                     .Configure(app => Configure(app))
                                     .ConfigureServices(services => services.AddRouting()));
            _client = _server.CreateClient();
            _webhdfs = new WebHDFSClient(_server.BaseAddress.AbsoluteUri)
            {
                CustomHttpClientHandler = (HttpClientHandler)_server.CreateHandler()
            };
        }

        [Fact]
        public void TestWebHostAsync()
        {
            var response = _client.GetAsync("/hello/kevin").Result;
            response.EnsureSuccessStatusCode();
            var data = response.Content.ReadAsStringAsync().Result;
            Assert.Equal("Hi, kevin!", data);

            Assert.Equal("abc", _webhdfs.GetFileStatus("hello/kevin").Result.owner);
        }

        void Configure(IApplicationBuilder app)
        {
            var routeBuilder = new RouteBuilder(app);

            routeBuilder.MapGet("hello/{name}", context =>
            {
                var name = context.GetRouteValue("name");
                // This is the route handler when HTTP GET "hello/<anything>"  matches
                // To match HTTP GET "hello/<anything>/<anything>,
                // use routeBuilder.MapGet("hello/{*name}"
                return context.Response.WriteAsync($"Hi, {name}!");
            });

            app.UseRouter(routeBuilder.Build());
        }

    }
}
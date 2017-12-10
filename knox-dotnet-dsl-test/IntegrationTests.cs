using System;
using System.Net;
using knoxdotnetdsl;
using Xunit;
using Xunit.Abstractions;

namespace knox_dotnet_dsl_test
{
    public class IntegrationTests
    {
        private readonly ITestOutputHelper output;

        private readonly ContentSummary ExpectedContentSummary = new ContentSummary()
        {
            directoryCount = 2,
            fileCount = 1,
            length = 24930,
            quota = -1,
            spaceConsumed = 24930,
            spaceQuota = -1,
            typeQuota = new TypeQuota()
            {
                ARCHIVE = new ARCHIVE()
                {
                    consumed = 500,
                    quota = 10000
                },
                DISK = new DISK()
                {
                    consumed = 500,
                    quota = 10000
                },
                SSD = new SSD()
                {
                    consumed = 500,
                    quota = 10000
                }
            }
        };

        private readonly FileChecksum ExpectedFileChecksum = new FileChecksum()
        {
            algorithm = "MD5-of-1MD5-of-512CRC32",
            bytes = "eadb10de24aa315748930df6e185c0d",
            length = 28
        };

        public IntegrationTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestWebHDFS()
        {
            var webhdfs = new WebHDFS(
                new Uri("http://localhost:3000/")
            );

            var ActualContentSummary = webhdfs.GetContentSummary("/ContentSummary");
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

            var ActualFileChecksum = webhdfs.GetFileChecksum("/FileChecksum");
            Assert.Equal(ExpectedFileChecksum.algorithm, ActualFileChecksum.algorithm);
            Assert.Equal(ExpectedFileChecksum.bytes, ActualFileChecksum.bytes);
            Assert.Equal(ExpectedFileChecksum.length, ActualFileChecksum.length);

            var ActualFileStatus = webhdfs.GetFileStatus("/FileStatus");


            var ActualListStatus = webhdfs.ListStatus("/ListStatus");

        }
         
        [Fact]
        public void TestWebHDFSAuth()
        {
            var webhdfsAuth = new WebHDFS(
                new Uri("http://localhost:3000/auth"), new NetworkCredential("admin", "admin")
            );

            var ActualContentSummary = webhdfsAuth.GetContentSummary("/ContentSummary");
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

            var ActualFileChecksum = webhdfsAuth.GetFileChecksum("/FileChecksum");
            Assert.Equal(ExpectedFileChecksum.algorithm, ActualFileChecksum.algorithm);
            Assert.Equal(ExpectedFileChecksum.bytes, ActualFileChecksum.bytes);
            Assert.Equal(ExpectedFileChecksum.length, ActualFileChecksum.length);
        }
    }
}

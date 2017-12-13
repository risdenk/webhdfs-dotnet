using System;
using System.Collections.Generic;
using System.Net;
using Xunit;
using Xunit.Abstractions;

namespace WebHDFS.Test
{
    public class IntegrationTests
    {
        readonly ITestOutputHelper output;

        readonly ContentSummary ExpectedContentSummary = new ContentSummary
        {
            directoryCount = 2,
            fileCount = 1,
            length = 24930,
            quota = -1,
            spaceConsumed = 24930,
            spaceQuota = -1,
            typeQuota = new TypeQuota
            {
                ARCHIVE = new ARCHIVE
                {
                    consumed = 500,
                    quota = 10000
                },
                DISK = new DISK
                {
                    consumed = 500,
                    quota = 10000
                },
                SSD = new SSD
                {
                    consumed = 500,
                    quota = 10000
                }
            }
        };

        readonly FileChecksum ExpectedFileChecksum = new FileChecksum
        {
            algorithm = "MD5-of-1MD5-of-512CRC32",
            bytes = "eadb10de24aa315748930df6e185c0d",
            length = 28
        };

        readonly FileStatus ExpectedFileStatus = new FileStatus
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

        readonly FileStatuses ExpectedListStatus = new FileStatuses
        {
            FileStatus = new List<FileStatus> {
                new FileStatus {
                    accessTime = 1320171722771,
                    blockSize = 33554432,
                    group = "supergroup",
                    length = 24930,
                    modificationTime = 1320171722771,
                    owner = "webuser",
                    pathSuffix = "a.patch",
                    permission = "644",
                    replication = 1,
                    type = "FILE"
                },
                new FileStatus {
                    accessTime = 0,
                    blockSize = 0,
                    group = "supergroup",
                    length = 0,
                    modificationTime = 1320895981256,
                    owner = "szetszwo",
                    pathSuffix = "bar",
                    permission = "711",
                    replication = 0,
                    type = "DIRECTORY"
                }
            }
        };

        readonly string ExpectedHomeDirectory = "/user/szetszwo";

        public IntegrationTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestWebHDFS()
        {
            var webhdfs = new WebHDFSClient(
                "http://localhost:3000"
            );

            var ActualContentSummary = webhdfs.GetContentSummary("/ContentSummary").Result;
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

            var ActualFileChecksum = webhdfs.GetFileChecksum("/FileChecksum").Result;
            Assert.Equal(ExpectedFileChecksum.algorithm, ActualFileChecksum.algorithm);
            Assert.Equal(ExpectedFileChecksum.bytes, ActualFileChecksum.bytes);
            Assert.Equal(ExpectedFileChecksum.length, ActualFileChecksum.length);

            var ActualFileStatus = webhdfs.GetFileStatus("/FileStatus").Result;
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

            var ActualListStatus = webhdfs.ListStatus("/ListStatus").Result;
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

            var ActualHomeDirectory = webhdfs.GetHomeDirectory().Result;
            Assert.Equal(ExpectedHomeDirectory, ActualHomeDirectory);
        }
         
        [Fact]
        public void TestWebHDFSAuth()
        {
            var webhdfs = new WebHDFSClient("http://localhost:3000/auth") {
                Credentials = new NetworkCredential("admin", "admin")   
            };

            var ActualContentSummary = webhdfs.GetContentSummary("/ContentSummary").Result;
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

            var ActualFileChecksum = webhdfs.GetFileChecksum("/FileChecksum").Result;
            Assert.Equal(ExpectedFileChecksum.algorithm, ActualFileChecksum.algorithm);
            Assert.Equal(ExpectedFileChecksum.bytes, ActualFileChecksum.bytes);
            Assert.Equal(ExpectedFileChecksum.length, ActualFileChecksum.length);

            var ActualFileStatus = webhdfs.GetFileStatus("/FileStatus").Result;
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

            var ActualListStatus = webhdfs.ListStatus("/ListStatus").Result;
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

            var ActualHomeDirectory = webhdfs.GetHomeDirectory().Result;
            Assert.Equal(ExpectedHomeDirectory, ActualHomeDirectory);
        }
    }
}

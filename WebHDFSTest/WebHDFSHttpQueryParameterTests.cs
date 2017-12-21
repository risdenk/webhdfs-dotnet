using Xunit;
using System;
using System.Collections.Specialized;

namespace WebHDFS.Test
{
    public class WebHDFSHttpQueryParameterTests
    {
        NameValueCollection getEmptyQuery() {
            return new NameValueCollection();
        }

        [Fact]
        public void TestOp()
        {
            var query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOp(query, WebHDFSHttpQueryParameter.Op.CREATE);
            Assert.Equal("CREATE", query["op"]);
        }

        [Fact]
        public void TestOverwrite()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetOverwrite(query, null));

            var queryTrue = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOverwrite(queryTrue, true);
            Assert.Equal("true", queryTrue["overwrite"]);

            var queryFalse = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOverwrite(queryFalse, false);
            Assert.Equal("false", queryFalse["overwrite"]);
        }

        [Fact]
        public void TestBlocksize()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.setBlocksize(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBlocksize(query, 128);
            Assert.Equal(128.ToString(), query["blocksize"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBlocksize(query, 512);
            Assert.Equal(512.ToString(), query["blocksize"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setBlocksize(query, 0); });
        }

        [Fact]
        public void TestReplication()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetReplication(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetReplication(query, 1);
            Assert.Equal(1.ToString(), query["replication"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetReplication(query, 3);
            Assert.Equal(3.ToString(), query["replication"]);
        }

        [Fact]
        public void TestPermission()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetPermission(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetPermission(query, "1");
            Assert.Equal("1", query["permission"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetPermission(query, "755");
            Assert.Equal("755", query["permission"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetPermission(query, "800"); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetPermission(query, "11111"); });
        }

        [Fact]
        public void TestBuffersize()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetBuffersize(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetBuffersize(query, 1);
            Assert.Equal("1", query["buffersize"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetBuffersize(query, 100);
            Assert.Equal("100", query["buffersize"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetBuffersize(query, 0); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetBuffersize(query, -1); });
        }

        [Fact]
        public void TestOffset()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetOffset(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOffset(query, 0);
            Assert.Equal("0", query["offset"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOffset(query, 1);
            Assert.Equal("1", query["offset"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOffset(query, 100);
            Assert.Equal("100", query["offset"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetOffset(query, -1); });
        }

        [Fact]
        public void TestLength()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetLength(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetLength(query, 0);
            Assert.Equal("0", query["length"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetLength(query, 1);
            Assert.Equal("1", query["length"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetLength(query, 100);
            Assert.Equal("100", query["length"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetLength(query, -1); });
        }

        [Fact]
        public void TestDestination()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetDestination(query, null));

            query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetDestination(query, ""));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetDestination(query, "/abc");
            Assert.Equal("/abc", query["destination"]);
        }

        [Fact]
        public void TestRecursive()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetRecursive(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetRecursive(query, true);
            Assert.Equal("true", query["recursive"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetRecursive(query, false);
            Assert.Equal("false", query["recursive"]);
        }

        [Fact]
        public void TestNewLength()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetNewLength(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetNewLength(query, 0);
            Assert.Equal("0", query["newlength"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetNewLength(query, 1);
            Assert.Equal("1", query["newlength"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetNewLength(query, 100);
            Assert.Equal("100", query["newlength"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetNewLength(query, -1); });
        }

        [Fact]
        public void TestSources()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetSources(query, null));

            query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetSources(query, ""));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetSources(query, "/abc");
            Assert.Equal("/abc", query["sources"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetSources(query, "/abc,/def");
            Assert.Equal("/abc,/def", query["sources"]);
        }

        [Fact]
        public void TestCreateParent()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetCreateParent(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetCreateParent(query, true);
            Assert.Equal("true", query["createParent"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetCreateParent(query, false);
            Assert.Equal("false", query["createParent"]);
        }

        [Fact]
        public void TestOwner()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetOwner(query, null));

            query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetOwner(query, ""));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetOwner(query, "user1");
            Assert.Equal("user1", query["owner"]);
        }

        [Fact]
        public void TestGroup()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetGroup(query, null));

            query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetGroup(query, ""));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetGroup(query, "group1");
            Assert.Equal("group1", query["group"]);
        }

        [Fact]
        public void TestModificationTime()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetModificationTime(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetModificationTime(query, -1);
            Assert.Equal("-1", query["modificationtime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetModificationTime(query, 0);
            Assert.Equal("0", query["modificationtime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetModificationTime(query, 1);
            Assert.Equal("1", query["modificationtime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetModificationTime(query, 100);
            Assert.Equal("100", query["modificationtime"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetModificationTime(query, -2); });
        }

        [Fact]
        public void TestAccessTime()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetAccessTime(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetAccessTime(query, -1);
            Assert.Equal("-1", query["accesstime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetAccessTime(query, 0);
            Assert.Equal("0", query["accesstime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetAccessTime(query, 1);
            Assert.Equal("1", query["accesstime"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetAccessTime(query, 100);
            Assert.Equal("100", query["accesstime"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetAccessTime(query, -2); });
        }

        [Fact]
        public void TestFSAction()
        {
            var query = getEmptyQuery();
            Assert.Equal(query, WebHDFSHttpQueryParameter.SetFSAction(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetFSAction(query, "---");
            Assert.Equal("---", query["fsaction"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetFSAction(query, "r--");
            Assert.Equal("r--", query["fsaction"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetFSAction(query, "rw-");
            Assert.Equal("rw-", query["fsaction"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetFSAction(query, "rwx");
            Assert.Equal("rwx", query["fsaction"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.SetFSAction(query, "--x");
            Assert.Equal("--x", query["fsaction"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetPermission(query, "a--"); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetPermission(query, "----"); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.SetPermission(query, "rr-"); });
        }
    }
}

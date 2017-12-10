using Xunit;
using System;
using knoxdotnetdsl;
using System.Collections.Specialized;

namespace knox_dotnet_dsl_test
{
    public class WebHDFSHttpQueryParameterTests
    {
        private NameValueCollection getEmptyQuery() {
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
            WebHDFSHttpQueryParameter.SetOffset(query, null);
            Assert.Equal("null", query["offset"]);

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
    }

}

using NUnit.Framework;
using System;
using knoxdotnetdsl;
using System.Collections.Specialized;

namespace knoxdotnetdsltests
{
    [TestFixture()]
    public class WebHDFSHttpQueryParameterTests
    {
        private NameValueCollection getEmptyQuery() {
            return new NameValueCollection();
        }

        [Test()]
        public void TestOp()
        {
            var query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOp(query, WebHDFSHttpQueryParameter.Op.CREATE);
            Assert.AreEqual("CREATE", query["op"]);
        }

        [Test()]
        public void TestOverwrite()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setOverwrite(query, null));

            var queryTrue = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOverwrite(queryTrue, true);
            Assert.AreEqual("true", queryTrue["overwrite"]);

            var queryFalse = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOverwrite(queryFalse, false);
            Assert.AreEqual("false", queryFalse["overwrite"]);
        }

        [Test()]
        public void TestBlocksize()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setBlocksize(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBlocksize(query, 128);
            Assert.AreEqual(128.ToString(), query["blocksize"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBlocksize(query, 512);
            Assert.AreEqual(512.ToString(), query["blocksize"]);
        }

        [Test()]
        public void TestReplication()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setReplication(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setReplication(query, 1);
            Assert.AreEqual(1.ToString(), query["replication"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setReplication(query, 3);
            Assert.AreEqual(3.ToString(), query["replication"]);
        }

        [Test()]
        public void TestPermission()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setPermission(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setPermission(query, "1");
            Assert.AreEqual("1", query["permission"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setPermission(query, "755");
            Assert.AreEqual("755", query["permission"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setPermission(query, "800"); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setPermission(query, "11111"); });
        }

        [Test()]
        public void TestBuffersize()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setBuffersize(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBuffersize(query, 1);
            Assert.AreEqual("1", query["buffersize"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setBuffersize(query, 100);
            Assert.AreEqual("100", query["buffersize"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setBuffersize(query, 0); });
            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setBuffersize(query, -1); });
        }

        [Test()]
        public void TestOffset()
        {
            var query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOffset(query, null);
            Assert.AreEqual("null", query["offset"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOffset(query, 0);
            Assert.AreEqual("0", query["offset"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOffset(query, 1);
            Assert.AreEqual("1", query["offset"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setOffset(query, 100);
            Assert.AreEqual("100", query["offset"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setOffset(query, -1); });
        }

        [Test()]
        public void TestLength()
        {
            var query = getEmptyQuery();
            Assert.AreEqual(query, WebHDFSHttpQueryParameter.setLength(query, null));

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setLength(query, 0);
            Assert.AreEqual("0", query["length"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setLength(query, 1);
            Assert.AreEqual("1", query["length"]);

            query = getEmptyQuery();
            WebHDFSHttpQueryParameter.setLength(query, 100);
            Assert.AreEqual("100", query["length"]);

            Assert.Throws<ArgumentException>(() => { WebHDFSHttpQueryParameter.setLength(query, -1); });
        }
    }

}

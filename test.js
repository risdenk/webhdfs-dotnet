const express = require('express')
const app = express()
const basicAuth = require('express-basic-auth')

var staticUserAuth = basicAuth({
    users: {
        'admin': 'admin'
    },
    challenge: true
});

var FileChecksumData =
{
  "FileChecksum":
  {
    "algorithm": "MD5-of-1MD5-of-512CRC32",
    "bytes"    : "eadb10de24aa315748930df6e185c0d",
    "length"   : 28
  }
};

var ContentSummaryData = 
{
  "ContentSummary":
  {
    "directoryCount": 2,
    "fileCount"     : 1,
    "length"        : 24930,
    "quota"         : -1,
    "spaceConsumed" : 24930,
    "spaceQuota"    : -1,
    "typeQuota":
    {
      "ARCHIVE":
      {
        "consumed": 500,
        "quota": 10000
      },
      "DISK":
      {
        "consumed": 500,
        "quota": 10000
      },
      "SSD":
      {
        "consumed": 500,
        "quota": 10000
      }
    }
  }
};

var FileStatusData =
{
  "FileStatus":
  {
    "accessTime"      : 0,
    "blockSize"       : 0,
    "group"           : "supergroup",
    "length"          : 0,             //in bytes, zero for directories
    "modificationTime": 1320173277227,
    "owner"           : "webuser",
    "pathSuffix"      : "",
    "permission"      : "777",
    "replication"     : 0,
    "type"            : "DIRECTORY"    //enum {FILE, DIRECTORY, SYMLINK}
  }
};

var ListStatusData = 
{
  "FileStatuses":
  {
    "FileStatus":
    [
      {
        "accessTime"      : 1320171722771,
        "blockSize"       : 33554432,
        "group"           : "supergroup",
        "length"          : 24930,
        "modificationTime": 1320171722771,
        "owner"           : "webuser",
        "pathSuffix"      : "a.patch",
        "permission"      : "644",
        "replication"     : 1,
        "type"            : "FILE"
      },
      {
        "accessTime"      : 0,
        "blockSize"       : 0,
        "group"           : "supergroup",
        "length"          : 0,
        "modificationTime": 1320895981256,
        "owner"           : "szetszwo",
        "pathSuffix"      : "bar",
        "permission"      : "711",
        "replication"     : 0,
        "type"            : "DIRECTORY"
      },
    ]
  }
};

var GetHomeDirectoryData = 
{
    "Path": "/user/szetszwo"
};

var RemoteException400Data =
{
  "RemoteException":
  {
    "exception"    : "IllegalArgumentException",
    "javaClassName": "java.lang.IllegalArgumentException",
    "message"      : "Invalid value for webhdfs parameter \"permission\": ..."
  }
};

var RemoteException401Data = 
{
  "RemoteException":
  {
    "exception"    : "SecurityException",
    "javaClassName": "java.lang.SecurityException",
    "message"      : "Failed to obtain user group information: ..."
  }
};

var RemoteException403Data = 
{
  "RemoteException":
  {
    "exception"    : "AccessControlException",
    "javaClassName": "org.apache.hadoop.security.AccessControlException",
    "message"      : "Permission denied: ..."
  }
};

var RemoteException404Data =
{
  "RemoteException":
  {
    "exception"    : "FileNotFoundException",
    "javaClassName": "java.io.FileNotFoundException",
    "message"      : "File does not exist: /foo/a.patch"
  }
};

// No Auth
app.get('/FileChecksum', (req, res) => res.redirect(
  307,
  'http://localhost:3000/FileChecksumData'
));

app.get('/FileChecksumData', (req, res) => res.json(FileChecksumData));
app.get('/ContentSummary', (req, res) => res.json(ContentSummaryData));
app.get('/FileStatus', (req, res) => res.json(FileStatusData));
app.get('/ListStatus', (req, res) => res.json(ListStatusData));
app.get('/', (req, res) => res.json(GetHomeDirectoryData));

// Auth
app.get('/auth/FileChecksum', staticUserAuth, (req, res) => res.redirect(
  307,
  'http://localhost:3000/auth/FileChecksumData'
));

app.get('/auth/FileChecksumData', staticUserAuth, (req, res) => res.json(FileChecksumData));
app.get('/auth/ContentSummary', staticUserAuth, (req, res) => res.json(ContentSummaryData));
app.get('/auth/FileStatus', staticUserAuth, (req, res) => res.json(FileStatusData));
app.get('/auth/ListStatus', staticUserAuth, (req, res) => res.json(ListStatusData));
app.get('/auth', staticUserAuth, (req, res) => res.json(GetHomeDirectoryData));

// Exceptions
app.get('/RemoteException400', function(req, res) {
  res.status(400);
  res.json(RemoteException400Data);
});
app.get('/RemoteException401', function(req, res) {
  res.status(401);
  res.json(RemoteException401Data);
});
app.get('/RemoteException403', function(req, res) {
  res.status(403);
  res.json(RemoteException403Data);
});
app.get('/RemoteException404', function(req, res) {
  res.status(404);
  res.json(RemoteException404Data);
});

app.listen(3000, () => console.log('Example app listening on port 3000!'))


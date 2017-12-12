using System;
namespace WebHDFS
{
    public class RemoteExceptionClass
    {
        public RemoteException RemoteException { get; set; }
    }

    public class RemoteException
    {
        public string exception { get; set; }
        public string javaClassName { get; set; }
        public string message { get; set; }
    }
}

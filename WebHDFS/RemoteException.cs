using System;
namespace WebHDFS
{
    /// <summary>
    /// Remote exception class.
    /// </summary>
    public class RemoteExceptionClass
    {
        /// <summary>
        /// Gets or sets the remote exception.
        /// </summary>
        /// <value>The remote exception.</value>
        public RemoteException RemoteException { get; set; }
    }

    /// <summary>
    /// Remote exception.
    /// </summary>
    public class RemoteException
    {
        /// <summary>
        /// Gets or sets the exception.
        /// </summary>
        /// <value>The exception.</value>
        public string exception { get; set; }

        /// <summary>
        /// Gets or sets the name of the java class.
        /// </summary>
        /// <value>The name of the java class.</value>
        public string javaClassName { get; set; }

        /// <summary>
        /// Gets or sets the message.
        /// </summary>
        /// <value>The message.</value>
        public string message { get; set; }
    }
}

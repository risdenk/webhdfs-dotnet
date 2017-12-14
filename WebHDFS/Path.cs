using System;
namespace WebHDFS
{
    /// <summary>
    /// Path class.
    /// </summary>
    public class PathClass
    {
        /// <summary>
        /// Gets or sets the path.
        /// </summary>
        /// <value>The path.</value>
        public string Path { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.PathClass"/>.
        /// </summary>
        /// <returns>A <see cref="T:System.String"/> that represents the current <see cref="T:WebHDFS.PathClass"/>.</returns>
        public override string ToString()
        {
            return Path;
        }
    }
}

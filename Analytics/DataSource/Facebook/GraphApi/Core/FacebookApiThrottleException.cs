using System;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.Core
{
    [Serializable]
    public class FacebookApiThrottleException : Exception
    {
        public int RetryAfterInSeconds { get; }

        public FacebookApiThrottleException() { }

        public FacebookApiThrottleException(string message)
            : base(message) { }

        public FacebookApiThrottleException(string message, Exception inner)
            : base(message, inner) { }

        public FacebookApiThrottleException(string message, int retryAfterInMinutes)
            : this(message)
        {
            RetryAfterInSeconds = retryAfterInMinutes * 60000;
        }
    }
}

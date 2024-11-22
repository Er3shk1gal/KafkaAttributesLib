using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Exceptions.TopicExceptions
{
    public class DeleteTopicException : TopicException
    {
        public DeleteTopicException()
        {
        }

        public DeleteTopicException(string message)
            : base(message)
        {
        }

        public DeleteTopicException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
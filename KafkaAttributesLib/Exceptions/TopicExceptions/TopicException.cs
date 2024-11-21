namespace KafkaAttributesLib.Exceptions.TopicExceptions;

public class TopicException : MyKafkaException
{
    public TopicException()
    {
    }

    public TopicException(string message)
        : base(message)
    {
    }

    public TopicException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
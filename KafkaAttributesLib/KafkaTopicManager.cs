using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaAttributesLib.Exceptions.TopicExceptions;

namespace KafkaAttributesLib;

public class KafkaTopicManager(IAdminClient adminClient)
{
    private readonly IAdminClient _adminClient = adminClient;

    /// <summary>
    /// Checks if a Kafka topic with the specified name exists.
    /// </summary>
    /// <param name="topicName">The name of the topic to check.</param>
    /// <returns>True if the topic exists, false otherwise.</returns>
    /// <exception cref="CheckTopicException">Thrown if the topic check fails.</exception>
    public bool CheckTopicExists(string topicName)
    {
        try
        {
            var topicExists = _adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            if (topicExists.Topics.Count == 0)
            {
                return false;
            }
            return true;
        }
        catch (Exception e)
        {
            
            Console.WriteLine($"An error occurred: {e.Message}"); 
            throw new CheckTopicException("Failed to check topic");
        }
    }

    /// <summary>
    /// Creates a new Kafka topic with the specified name, number of partitions, and replication factor.
    /// </summary>
    /// <param name="topicName">The name of the topic to create.</param>
    /// <param name="numPartitions">The number of partitions for the topic.</param>
    /// <param name="replicationFactor">The replication factor for the topic.</param>
    /// <returns>True if the topic was successfully created, false otherwise.</returns>
    /// <exception cref="CreateTopicException">Thrown if the topic creation fails.</exception>
    public bool CreateTopic(string topicName, int numPartitions, short replicationFactor)
    {
        try
        {
       
            var result = _adminClient.CreateTopicsAsync(new TopicSpecification[] 
            { 
                new() { 
                    Name = topicName,
                    NumPartitions = numPartitions, 
                    ReplicationFactor =  replicationFactor,   
                    Configs = new Dictionary<string, string>
                    {
                        { "min.insync.replicas", "2" } 
                    }} 
            });
            if (result.IsCompleted)
            {
                return true;
            }
            throw new CreateTopicException("Failed to create topic");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw new CreateTopicException("Failed to create topic");
        }
    }

  
}
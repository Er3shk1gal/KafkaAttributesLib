using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaAttributesLib.Exceptions;
using KafkaAttributesLib.Exceptions.ProducerExceptions;
using KafkaAttributesLib.Utils.MessageHandler;
using KafkaAttributesLib.Utils.NonRpc;
using Microsoft.Extensions.Logging;

namespace KafkaAttributesLib.NonRpc
{
    public class KafkaProducer
    {
        private IProducer<object, object> _producer;
        private readonly ILogger<KafkaProducer> _logger;
        private readonly KafkaTopicManager _kafkaTopicManager;
        private readonly short _replicationFactorStandart = 1;
        public KafkaProducer(IProducer<object,object> producer, ILogger<KafkaProducer> logger, KafkaTopicManager kafkaTopicManager, [Optional] short replicationFactorStandart)
        {
            _producer = producer;
            _logger = logger;
            _kafkaTopicManager = kafkaTopicManager;
            _replicationFactorStandart = replicationFactorStandart;
        }
        public async Task<bool> ProduceAsync(Topic topic, Message<object, object> message)
        {
            try
            {
                bool IsTopicExists = IsTopicAvailable(topic.TopicName, topic.Partition);
                if (IsTopicExists)
                {
                    var deliveryResult = await _producer.ProduceAsync(
                            new TopicPartition(topic.TopicName, new Partition(topic.Partition)), 
                            message);
                    if (deliveryResult.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation("Message delivery status: Persisted {Result}", deliveryResult.Value);
                        return true;
                    }
                    
                    _logger.LogError("Message delivery status: Not persisted {Result}", deliveryResult.Value);
                    throw new MessageProduceException("Message delivery status: Not persisted" + deliveryResult.Value);
                    
                }
                
                bool IsTopicCreated = _kafkaTopicManager.CreateTopic(topic.TopicName, topic.Partition+1, _replicationFactorStandart);
                if (IsTopicCreated)
                {
                    var deliveryResult = await _producer.ProduceAsync(new TopicPartition(topic.TopicName, new Partition(topic.Partition)), message);
                    if (deliveryResult.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation("Message delivery status: Persisted {Result}", deliveryResult.Value);
                       
                        return true;
                    }
                    
                    _logger.LogError("Message delivery status: Not persisted {Result}", deliveryResult.Value);
                    throw new MessageProduceException("Message delivery status: Not persisted");
                    
                }
                _logger.LogError("Topic unavailable");
                throw new MessageProduceException("Topic unavailable");
            }
            catch (Exception e)
            {
                if (e is MyKafkaException)
                {
                    _logger.LogError(e, "Error producing message");
                    throw new ProducerException("Error producing message",e);
                }
                throw;
            }
        }
        private bool IsTopicAvailable(string topicName, int partition)
        {
            try
            {
                bool IsTopicExists = _kafkaTopicManager.CheckTopicContainsPartitions(topicName, partition);
                if (IsTopicExists)
                {
                    return IsTopicExists;
                }
                _logger.LogError("Unable to subscribe to topic");
                throw new ProducerException("Topic unavailable");
            
            }
            catch (Exception e)
            {
                if (e is MyKafkaException)
                {
                    _logger.LogError(e,"Error checking topic");
                    throw new ConsumerException("Error checking topic",e);
                }
                _logger.LogError(e,"Unhandled error");
                throw;
            }
        }
    }
}
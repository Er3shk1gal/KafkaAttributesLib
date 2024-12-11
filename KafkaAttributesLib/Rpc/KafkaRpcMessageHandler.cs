using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaAttributesLib.Exceptions;
using KafkaAttributesLib.Exceptions.ProducerExceptions;
using KafkaAttributesLib.Exceptions.TopicExceptions;
using KafkaAttributesLib.Utils;
using KafkaAttributesLib.Utils.MessageHandler;
using KafkaAttributesLib.Utils.RPC;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaAttributesLib.Rpc
{
    public class KafkaRpcMessageHandler<K,M>
    {
        //TODO: Finish
        private readonly IProducer<K,M> _producer;
        private IConsumer<K,M> _consumer;
        private readonly RpcMessageHandlerConfig _config;
        private readonly ILogger<KafkaRpcMessageHandler<K, M>> _logger;
        private readonly KafkaTopicManager _kafkaTopicManager;
        private readonly IServiceProvider _serviceProvider;

        public KafkaRpcMessageHandler(RpcMessageHandlerConfig config, ILogger<KafkaRpcMessageHandler<K, M>> logger, KafkaTopicManager kafkaTopicManager, IServiceProvider serviceProvider)
        {
            _config = config;
            _logger = logger;
            _kafkaTopicManager = kafkaTopicManager;
            _producer = ConfigureProducer();
            if(ConfigureConsumer())
            {
                _logger.LogDebug("Consumer configured successfully");
            }
            _serviceProvider = serviceProvider;
        }
        public void Consume()
        {
            try
            {
                while (true)
                {
                    if (_consumer == null)
                    {
                        _logger.LogError("Consumer is null");
                        throw new ConsumerException("Consumer is null");
                    }

                    ConsumeResult<K, M> consumeResult = _consumer.Consume();
                    if (consumeResult != null)
                    {
                        var headerBytes = consumeResult.Message.Headers
                            .FirstOrDefault(x => x.Key.Equals("method"));

                        if (headerBytes != null)
                        {
                            var methodString = Encoding.UTF8.GetString(headerBytes.GetValueBytes());
                            ServiceResolver.GetParameters(methodString,consumeResult.Topic);  
                        }_config.topicConfig.Services.Where(x=>x.partition == topicPartition).FirstOrDefault()
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is MyKafkaException)
                {
                    _logger.LogError(ex, "Consumer error");
                }
                else
                {
                    _logger.LogError(ex, "Unhandled error");
                }
            }
        }
        private IProducer<K,M> ConfigureProducer()
        {
            return new ProducerBuilder<K,M>(_config.producerConfig).Build();
        }
        private bool ConfigureConsumer()
        {
            _consumer = new ConsumerBuilder<K,M>(_config.consumerConfig).Build();
            if(CheckTopicConfigs())
            {
                List<TopicPartition> partitions = new List<TopicPartition>();
                foreach(var topic in _config.rpcTopicPairConfig.Services)
                {
                    partitions.Add(new TopicPartition(_config.rpcTopicPairConfig.RequestTopicName, topic.RequestPartition));
                }
                _consumer.Assign(partitions);
                return true;
            }
            throw new ConfigureConsumersException("Failed to configure consumer");
        }
        private bool IsTopicSatisfyesRequirements(string requestTopicName, int requestTopicNumPartitions, string responseTopicName, int responseTopicNumPartitions)
        {
            try
            {
                bool IsTopicSatisfyesRequirements = _kafkaTopicManager.CheckTopicSatisfiesRequirements(requestTopicName, requestTopicNumPartitions) && _kafkaTopicManager.CheckTopicSatisfiesRequirements(responseTopicName, responseTopicNumPartitions);
                if (IsTopicSatisfyesRequirements)
                {
                    return IsTopicSatisfyesRequirements;
                }
                else
                {
                    if(_kafkaTopicManager.DeleteTopic(requestTopicName) && _kafkaTopicManager.DeleteTopic(responseTopicName))
                    {
                        if(_kafkaTopicManager.CreateTopic(requestTopicName,requestTopicNumPartitions,_config.rpcTopicPairConfig.ReplicationFactorStandart) && _kafkaTopicManager.CreateTopic(responseTopicName,responseTopicNumPartitions,_config.rpcTopicPairConfig.ReplicationFactorStandart))
                        {
                            return true;
                        }
                        throw new CreateTopicException("Failed to create topic");
                    }
                    throw new DeleteTopicException("Failed to delete topic");
                }
            
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
        private bool CheckTopicConfigs()
        {
            try
            {
                
                
                
                if(!IsTopicSatisfyesRequirements(_config.rpcTopicPairConfig.RequestTopicName,_config.topicConfig.PartitionCount   ))
                {
                    throw new TopicSatisfyesRequirementsException();
                }
                
                return true;
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
        private bool IsValid(object value)
        {
            var validationResults = new List<ValidationResult>();
            var validationContext = new ValidationContext(value, null, null);
            
            bool isValid = Validator.TryValidateObject(value, validationContext, validationResults, true);

            if (!isValid)
            {
                foreach (var validationResult in validationResults)
                {
                    _logger.LogError(validationResult.ErrorMessage);
                }
            }

            return isValid;
        }
        private void SendResponseMessage()
        {

        }
    }
}
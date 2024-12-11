using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaAttributesLib.Attributes;
using KafkaAttributesLib.Exceptions;
using KafkaAttributesLib.Exceptions.ProducerExceptions;
using KafkaAttributesLib.Exceptions.ReflectionExceptions;
using KafkaAttributesLib.Exceptions.TopicExceptions;
using KafkaAttributesLib.Utils;
using KafkaAttributesLib.Utils.MessageHandler;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaAttributesLib
{
    public class KafkaMessageHandler<K,M>
    {
        //TODO: Add exception handling

        private readonly IProducer<K,M> _producer;
        private IConsumer<K,M> _consumer;
        private readonly MessageHandlerConfig _config;
        private readonly ILogger<KafkaMessageHandler<K, M>> _logger;
        private readonly KafkaTopicManager _kafkaTopicManager;
        private readonly IServiceProvider _serviceProvider;

        public KafkaMessageHandler(MessageHandlerConfig config, ILogger<KafkaMessageHandler<K, M>> logger, KafkaTopicManager kafkaTopicManager, IServiceProvider serviceProvider)
        {
            _config = config;
            _logger = logger;
            _kafkaTopicManager = kafkaTopicManager;
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
                            var parameters = ServiceResolver.GetParameters(methodString,_config.topicConfig.Services.Where(x=>x.partition == consumeResult.TopicPartition.Partition).FirstOrDefault().ServiceName);
                            InvokeMethodByHeader(methodString, consumeResult.Message.Value.ToString(), );
                        }
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
                foreach(var topic in _config.topicConfig.Services)
                {
                    partitions.Add(new TopicPartition(_config.topicConfig.TopicName, topic.partition));
                }
                _consumer.Assign(partitions);
                return true;
            }
            throw new ConfigureConsumersException("Failed to configure consumer");
        }
        private bool IsTopicSatisfyesRequirements(string topicName, int numPartitions)
        {
            try
            {
                bool IsTopicSatisfyesRequirements = _kafkaTopicManager.CheckTopicSatisfiesRequirements(topicName, numPartitions);
                if (IsTopicSatisfyesRequirements)
                {
                    return IsTopicSatisfyesRequirements;
                }
                else
                {
                    if(_kafkaTopicManager.DeleteTopic(topicName))
                    {
                        if(_kafkaTopicManager.CreateTopic(topicName,numPartitions,_config.topicConfig.ReplicationFactorStandart))
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
                if(!IsTopicSatisfyesRequirements(_config.topicConfig.TopicName,_config.topicConfig.PartitionCount   ))
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
        private ServiceLifetime GetServiceLifetime(Type service)
        {
            var serviceCollection = (_serviceProvider as IServiceProvider)?.GetService(typeof(IServiceCollection)) as IServiceCollection;

            if (serviceCollection != null)
            {
                var serviceType = serviceCollection.FirstOrDefault(x=>x.ServiceType==service);
                if(serviceType != null)
                {
                    return serviceType.Lifetime;
                }
            }
            throw new GetServiceLifetimeException("Failed to get service lifetime");
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
    }
}
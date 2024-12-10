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
using KafkaAttributesLib.Utils.MessageHandler;
using KafkaAttributesLib.Utils.RPC;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
                            InvokeMethodByHeader(methodString, consumeResult.Message.Value.ToString(), consumeResult.TopicPartition.Partition);
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
        //TODO: Add parsing for many method parameters
        private void InvokeMethodByHeader(string methodName, string? message, int topicPartition)
        {
            string serviceName = _config.r.Services.Where(x=>x.partition == topicPartition).FirstOrDefault().ServiceName;
            var serviceMethodPair = GetClassAndMethod(serviceName, methodName);
            var method = serviceMethodPair.Method;
            var service = serviceMethodPair.Service;
            using (var scope = _serviceProvider.CreateScope())
            {
            
        
                var serviceInstance = scope.ServiceProvider.GetRequiredService(service.GetInterfaces().FirstOrDefault());
                if (serviceInstance == null)
                {
                    throw new UnconfiguredServiceMethodsExeption("Service not found");
                }
                if(message == null)
                {
                    InvokeMethodWithoutParameters(serviceMethodPair, serviceInstance);
                    return;
                }

                InvokeMethodWithParameters(serviceMethodPair, serviceInstance, message);

                
            }
        }
        private void InvokeMethodWithoutParameters(ServiceMethodPair serviceMethodPair, object serviceInstance)
        {
            var method = serviceMethodPair.Method;

            if (method.GetParameters().Length != 0)
            {
                throw new UnconfiguredServiceMethodsExeption("Wrong method implementation: method should not have parameters.");
            }

            if (method.ReturnType == typeof(void))
            {
                method.Invoke(serviceInstance, null);
            }
            else
            {
                var result = method.Invoke(serviceInstance, null);
                if (!(bool)result)
                {
                    throw new Exception("Wrong method implementation: expected a boolean return type.");
                }
            }
        }

        private void InvokeMethodWithParameters(ServiceMethodPair serviceMethodPair, object serviceInstance, string message)
        {
            var method = serviceMethodPair.Method;

            if (method.GetParameters().Length == 0)
            {
                throw new UnconfiguredServiceMethodsExeption("Wrong method implementation: method should have parameters.");
            }

            var parameterType = method.GetParameters()[0].ParameterType;
            var parameterValue = JsonConvert.DeserializeObject(message, parameterType);

            var result = method.Invoke(serviceInstance, new object[] { parameterValue });
            if (!(bool)result)
            {
                throw new Exception("Wrong method implementation: expected a boolean return type.");
            }
        }
        private ServiceMethodPair GetClassAndMethod(string serviceName,string methodName)
        {
            var serviceClasses = Assembly.GetExecutingAssembly().GetTypes()
            .Where(t => t.GetCustomAttributes(typeof(KafkaServiceNameAttribute), false).Any());
          
            foreach (var serviceClass in serviceClasses)
            {
                var serviceNameAttr = (KafkaServiceNameAttribute)serviceClass
                    .GetCustomAttributes(typeof(KafkaServiceNameAttribute), false)
                    .FirstOrDefault();
                if (serviceNameAttr != null && serviceNameAttr.ServiceName == serviceName)
                {
                    var methods = serviceClass.GetMethods()
                    .Where(m => m.GetCustomAttributes(typeof(KafkaMethodAttribute), false).Any());
                    foreach (var method in methods)
                    {
                        var methodAttr = (KafkaMethodAttribute)method
                            .GetCustomAttributes(typeof(KafkaMethodAttribute), false)
                            .FirstOrDefault();

                        if (methodAttr != null && methodAttr.MethodName == methodName)
                        {
                            return new ServiceMethodPair()
                            {
                                Service = serviceClass,
                                Method = method,
                            };
                        }  
                    }
                }
            }
            throw new UnconfiguredServiceMethodsExeption("Method not found");
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
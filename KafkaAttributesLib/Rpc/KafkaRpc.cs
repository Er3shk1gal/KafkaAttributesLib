using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaAttributesLib.Exceptions;
using KafkaAttributesLib.Exceptions.ProducerExceptions;
using KafkaAttributesLib.Utils.RPC;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaAttributesLib
{
    //TODO: Avro, protobuf, byte[] support
    //TODO: Better error and null handling
    //TODO: KafkaRpcMessageHandler
    public class KafkaRpc
    {
        private IProducer<object, object> _producer;
        private readonly ILogger<KafkaRpc> _logger;
        private readonly RPCConfig _config;
        private readonly KafkaTopicManager _kafkaTopicManager;
        private readonly HashSet<PendingMessagesBus> _pendingMessagesBus;
        private readonly HashSet<RecievedMessagesBus> _recievedMessagesBus;
        private readonly HashSet<IConsumer<object,object>> _consumerPool;
        public KafkaRpc(RPCConfig config, ILogger<KafkaRpc> logger, KafkaTopicManager kafkaTopicManager, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _producer = ConfigureProducer(config);
            _kafkaTopicManager = kafkaTopicManager;
            _config = config;
            _pendingMessagesBus = ConfigurePendingMessages(config.responseTopics);
            _recievedMessagesBus = ConfigureRecievedMessages(config.responseTopics);
            _consumerPool = ConfigureConsumers(config.responseTopics.Count);
        }
        public void BeginRecieving()
        {
            for (int i = 0; i < _consumerPool.Count; i++)
            {
                
                Thread thread = new Thread(x=>{
                     Consume(_consumerPool.ElementAt(i),_config.responseTopics[i]);
                });
                thread.Start();
            }
        }
        public async Task<Q> BroadcastMessageJson<T,Q>(string methodName, string serviceName, RpcTopic requestTopic, RpcTopic responseTopic, T request)
        {
            try
            {
                Guid messageId = Guid.NewGuid();
                Message<object,object> message = new Message<object, object>()
                {
                    Key = messageId,
                    Value = JsonConvert.SerializeObject(request),
                    Headers = new Headers()
                    {
                        new Header("method",Encoding.UTF8.GetBytes(methodName)),
                        new Header("sender",Encoding.UTF8.GetBytes(serviceName))
                    }
                };
                if(await Produce(requestTopic,message,responseTopic))
                {
                    _logger.LogDebug("Message sent :{messageId}",messageId.ToString());
                    while (!IsMessageRecieved(messageId.ToString()))
                    {
                        Thread.Sleep(200);
                    }
                    _logger.LogDebug("Message recieved :{messageId}",messageId.ToString());
                    return GetMessageJson<Q>(messageId.ToString(),responseTopic);
                }
                throw new Exception("Message not recieved");
            }
            catch (Exception)
            {
                throw;
            }
        }
        public T GetMessageJson<T>(object MessageKey, RpcTopic topic)
        {
            //FIXME: find a better way without suppresing not null
            if(IsMessageRecieved(MessageKey))
            {
                var message = _recievedMessagesBus.FirstOrDefault(x=>x.TopicInfo.Equals(topic))!.Messages.FirstOrDefault(x=>x.Key==MessageKey);
                _recievedMessagesBus.FirstOrDefault(x=>x.TopicInfo.Equals(topic))!.Messages.Remove(message!);
                return JsonConvert.DeserializeObject<T>(message!.Value.ToString()!)!;
            }
            throw new ConsumerException("Message not recieved");
        }
        private bool IsMessageRecieved(object MessageKey)
        {
            try
            {
                return _recievedMessagesBus.Any(x=>x.Messages.Any(x=>x.Key==MessageKey));
            }
            catch (Exception e)
            {
                throw new ConsumerException($"Recieved message bus error",e);
            }
        }
        private IProducer<object, object> ConfigureProducer(RPCConfig config)
        {
            return new ProducerBuilder<object, object>(config.producerConfig).Build();
        }

        private HashSet<IConsumer<object,object>> ConfigureConsumers(int amount)
        {
            try
            {
                if(amount<=0)
                {
                    throw new ConfigureConsumersException(" Amount of consumers must be above 0!");
                }
                HashSet<IConsumer<object,object>> consumers = new HashSet<IConsumer<object, object>>();
                for (int i = 0; i < amount; i++)
                {
                    consumers.Add(
                        new ConsumerBuilder<object,object>(
                            _config.consumerConfig
                        ).Build()
                    );
                }
                return consumers;
            }
            catch (Exception ex)
            {
                if (ex is MyKafkaException)
                {
                    _logger.LogError(ex, "Error configuring consumers");
                    throw new ProducerException("Error configuring consumers",ex);
                }
                throw;
            }
          
        }
        private HashSet<PendingMessagesBus> ConfigurePendingMessages(List<RpcTopic> ResponseTopics)
        {
            if(ResponseTopics.Count == 0)
            {
                throw new ConfigureMessageBusException("At least one requests topic must e provided!");
            }
            var PendingMessages = new HashSet<PendingMessagesBus>();
            foreach(var responseTopic in ResponseTopics)
            {
                 if(!IsTopicAvailable(responseTopic.TopicName, responseTopic.Partition))
                {
                    _kafkaTopicManager.CreateTopic(responseTopic.TopicName, ResponseTopics.Where(x=>x.TopicName==responseTopic.TopicName).Max(x=>x.Partition), _config.replicationFactorStandart);
                }
                PendingMessages.Add(new PendingMessagesBus(){ TopicInfo=responseTopic, MessageKeys = new HashSet<MethodKeyPair>()});
            }
            return PendingMessages;
        }
        private HashSet<RecievedMessagesBus> ConfigureRecievedMessages(List<RpcTopic> ResponseTopics)
        {
            if(ResponseTopics.Count == 0)
            {
                throw new ConfigureMessageBusException("At least one response topic must e provided!");
            }
            HashSet<RecievedMessagesBus> Responses = new HashSet<RecievedMessagesBus>();
            foreach(var responseTopic in ResponseTopics)
            {
                if(!IsTopicAvailable(responseTopic.TopicName, responseTopic.Partition))
                {
                    _kafkaTopicManager.CreateTopic(responseTopic.TopicName, ResponseTopics.Where(x=>x.TopicName==responseTopic.TopicName).Max(x=>x.Partition)+1, _config.replicationFactorStandart);
                }
                Responses.Add(new RecievedMessagesBus() { TopicInfo = responseTopic, Messages = new HashSet<Message<object, object>>()});
            }
            return Responses;
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
                throw new ConsumerTopicUnavailableException("Topic unavailable");
            
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
        private bool IsTopicPendingMessageBusExist(RpcTopic responseTopic)
        {
            return _pendingMessagesBus.Any(x => x.TopicInfo.Equals(responseTopic));
        }
        private void Consume(IConsumer<object, object> localConsumer, RpcTopic topic)
        {
            var partitions = new List<TopicPartition> { new TopicPartition(topic.TopicName, topic.Partition) };
            localConsumer.Assign(partitions);

            while (true)
            {
                ConsumeResult<object, object> result = localConsumer.Consume();
                if (result == null)
                {
                    continue;
                }
                try
                {
                    var pendingMessageBus = _pendingMessagesBus.FirstOrDefault(x => x.TopicInfo.Equals(topic));
                    if (pendingMessageBus == null)
                    {
                        _logger.LogError("Pending message bus not found for topic {topicName}", topic.TopicName);
                        throw new ConsumerException($"Pending message bus not found for topic {topic.TopicName}");
                    }

                    var pendingMessage = pendingMessageBus.MessageKeys.FirstOrDefault(x => x.MessageKey == result.Message.Key);
                    if (pendingMessage == null)
                    {
                        _logger.LogError("Pending message not found for key {messageKey}", result.Message.Key);
                        throw new ConsumerException($"Pending message not found for key {result.Message.Key}");
                    }

                    if (result.Message.Headers.Any(x => x.Key.Equals("errors")))
                    {
                        var errors = Encoding.UTF8.GetString(result.Message.Headers.FirstOrDefault(x => x.Key.Equals("errors"))!.GetValueBytes());
                        _logger.LogError(errors);
                        throw new ConsumerException(errors);
                    }

                    var method = Encoding.UTF8.GetString(result.Message.Headers.FirstOrDefault(x => x.Key.Equals("method"))!.GetValueBytes());
                    if (pendingMessage.MessageMethod == method)
                    {
                        localConsumer.Commit(result);
                        _recievedMessagesBus.FirstOrDefault(x => x.TopicInfo.Equals(topic))!.Messages.Add(result.Message);
                        pendingMessageBus.MessageKeys.Remove(pendingMessage);
                    }
                    else
                    {
                        _logger.LogError("Wrong message method");
                        throw new ConsumerException("Wrong message method");
                    }
                }
                catch (Exception e)
                {
                    if (e is MyKafkaException)
                    {
                        _logger.LogError(e, "Consumer error");
                        throw new ConsumerException("Consumer error ", e);
                    }
                    _logger.LogError(e, "Unhandled error");
                    localConsumer.Commit(result);
                }
            }
        }
        private async Task<bool> Produce(RpcTopic requestTopic, Message<object, object> message, RpcTopic responseTopic)
        {
            try
            {
                //FIXME:Check if partition exists
                bool IsTopicExists = IsTopicAvailable(requestTopic.TopicName, responseTopic.Partition);
                if (IsTopicExists && IsTopicPendingMessageBusExist( responseTopic))
                {
                    var deliveryResult = await _producer.ProduceAsync(
                            new TopicPartition(requestTopic.TopicName, new Partition(requestTopic.Partition)), 
                            message);
                    if (deliveryResult.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation("Message delivery status: Persisted {Result}", deliveryResult.Value);
                      
                            _pendingMessagesBus.FirstOrDefault(x=>x.TopicInfo.Equals(responseTopic))!.MessageKeys.Add(new MethodKeyPair(){
                            MessageKey = message.Key,
                            MessageMethod = Encoding.UTF8.GetString(message.Headers.FirstOrDefault(x => x.Key.Equals("method"))!.GetValueBytes())
                        });
                        return true;
                        
                        
                    }
                    
                    _logger.LogError("Message delivery status: Not persisted {Result}", deliveryResult.Value);
                    throw new MessageProduceException("Message delivery status: Not persisted" + deliveryResult.Value);
                    
                }
                
                bool IsTopicCreated = _kafkaTopicManager.CreateTopic(requestTopic.TopicName, requestTopic.Partition+1, _config.replicationFactorStandart);
                if (IsTopicCreated && IsTopicPendingMessageBusExist( responseTopic))
                {
                    var deliveryResult = await _producer.ProduceAsync(new TopicPartition(requestTopic.TopicName, new Partition(requestTopic.Partition)), message);
                    if (deliveryResult.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation("Message delivery status: Persisted {Result}", deliveryResult.Value);
                        _pendingMessagesBus.FirstOrDefault(x=>x.TopicInfo.Equals(responseTopic))!.MessageKeys.Add(new MethodKeyPair(){
                            MessageKey = message.Key,
                            MessageMethod = Encoding.UTF8.GetString(message.Headers.FirstOrDefault(x => x.Key.Equals("method"))!.GetValueBytes())
                        });
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
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using KafkaAttributesLib.Attributes;
using KafkaAttributesLib.Exceptions.ProducerExceptions;
using KafkaAttributesLib.Exceptions.ReflectionExceptions;
using KafkaAttributesLib.Utils.MessageHandler;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace KafkaAttributesLib.Utils
{
    public static class ServiceResolver
    {
        //TODO: Add parsing for many method parameters
        //TODO: Exception handling
        public static bool InvokeMethodByHeader(IServiceProvider serviceProvider,string methodName,string serviceName,[Optional] List<object>? parameters)
        {
            ServiceMethodPair serviceMethodPair = GetClassAndMethodTypes(serviceName, methodName);
            MethodInfo method = serviceMethodPair.Method;
            Type service = serviceMethodPair.Service;
            ServiceLifetime lifetime = GetServiceLifetime(service,serviceProvider);
            if(parameters!=null)
            {
                if(lifetime==ServiceLifetime.Scoped)
                {
                    InvokeMethodWithParameters( method, GetScopedService(serviceProvider,service), parameters);
                }
                else if(lifetime==ServiceLifetime.Singleton)
                {
                    InvokeMethodWithParameters( method, GetSingletonService(serviceProvider,service), parameters);
                }
                else if(lifetime==ServiceLifetime.Transient)
                {
                    InvokeMethodWithParameters( method, GetTransientService(serviceProvider,service), parameters);
                }
            }
            if(lifetime==ServiceLifetime.Scoped)
            {
                InvokeMethodWithoutParameters( method, GetScopedService(serviceProvider,service));
            }
            else if(lifetime==ServiceLifetime.Singleton)
            {
                InvokeMethodWithoutParameters( method, GetSingletonService(serviceProvider,service));
            }
            else if(lifetime==ServiceLifetime.Transient)
            {
                InvokeMethodWithoutParameters( method, GetTransientService(serviceProvider,service));
            }
            return true;
        }
        private static bool InvokeMethodWithoutParameters(MethodInfo method,object serviceInstance)
        {
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

        private static void InvokeMethodWithParameters(MethodInfo method, object serviceInstance, List<object> parameters)
        {

            if (method.GetParameters().Length == 0)
            {
                throw new UnconfiguredServiceMethodsExeption("Wrong method implementation: method should have parameters.");
            }

            var result = method.Invoke(serviceInstance, parameters.ToArray());
            if (!(bool)result)
            {
                throw new Exception("Wrong method implementation: expected a boolean return type.");
            }
        }
        public static ParameterInfo[] GetParameters(string methodName,string serviceName)
        {
            ServiceMethodPair serviceMethodPair = GetClassAndMethodTypes(serviceName, methodName);
            return serviceMethodPair.Method.GetParameters();
        }
        private static ServiceLifetime GetServiceLifetime(Type service, IServiceProvider serviceProvider)
        {
            var serviceCollection = (serviceProvider as IServiceProvider)?.GetService(typeof(IServiceCollection)) as IServiceCollection;

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
        private static object GetScopedService(IServiceProvider serviceProvider, Type serviceType)
        {
            using (var scope = serviceProvider.CreateScope())
            {
                var serviceInstance = scope.ServiceProvider.GetRequiredService(serviceType.GetInterfaces().FirstOrDefault());
                if (serviceInstance != null)
                {
                    return serviceInstance;
                }
            }
            throw new GetScopedServiceException("Failed to get scoped service");

        }
        private static object GetSingletonService(IServiceProvider serviceProvider, Type serviceType)
        {
            var serviceInstance = serviceProvider.GetRequiredService(serviceType.GetInterfaces().FirstOrDefault());
            if (serviceInstance != null)
            {
                return serviceInstance;
            }
            throw new GetSingletonServiceException("Failed to get singleton service");
        }
        private static object GetTransientService(IServiceProvider serviceProvider, Type serviceType)
        {
            var serviceInstance = serviceProvider.GetRequiredService(serviceType.GetInterfaces().FirstOrDefault());
            if (serviceInstance != null)
            {
                return serviceInstance;
            }
            throw new GetTransientServiceException("Failed to get transient service");
        }
        private static ServiceMethodPair GetClassAndMethodTypes(string serviceName,string methodName)
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
    }
}
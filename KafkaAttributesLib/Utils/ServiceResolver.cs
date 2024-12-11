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
        public static object InvokeMethodByHeader(IServiceProvider serviceProvider,string methodName,string serviceName,[Optional] List<object>? parameters)
        {
            ServiceMethodPair serviceMethodPair = GetClassAndMethodTypes(serviceName, methodName);
            MethodInfo method = serviceMethodPair.Method;
            Type service = serviceMethodPair.Service;
            ServiceLifetime lifetime = GetServiceLifetime(service,serviceProvider);
            if(parameters!=null)
            {
                if(lifetime==ServiceLifetime.Scoped)
                {
                    return InvokeMethodWithParameters( method, GetScopedService(serviceProvider,service), parameters);
                }
                else if(lifetime==ServiceLifetime.Singleton)
                {
                    return InvokeMethodWithParameters( method, GetSingletonService(serviceProvider,service), parameters);
                }
                else if(lifetime==ServiceLifetime.Transient)
                {
                    return InvokeMethodWithParameters( method, GetTransientService(serviceProvider,service), parameters);
                }
            }
            if(lifetime==ServiceLifetime.Scoped)
            {
                return InvokeMethodWithoutParameters( method, GetScopedService(serviceProvider,service));
            }
            else if(lifetime==ServiceLifetime.Singleton)
            {
                return InvokeMethodWithoutParameters( method, GetSingletonService(serviceProvider,service));
            }
            else if(lifetime==ServiceLifetime.Transient)
            {
                return InvokeMethodWithoutParameters( method, GetTransientService(serviceProvider,service));
            }
            throw new InvokeMethodException("Failed to invoke method");
        }
        private static object InvokeMethodWithoutParameters(MethodInfo method,object serviceInstance)
        {
            if (method.GetParameters().Length != 0)
            {
                throw new InvokeMethodException("Wrong method implementation: method should not have parameters.");
            }

            if (method.ReturnType == typeof(void))
            {
                method.Invoke(serviceInstance, null);
                return true;
            }
            else
            {
                var result = method.Invoke(serviceInstance, null);
                if (result != null)
                {
                    return result;
                }
            }
            throw new InvokeMethodException("Method invocation failed");
        }

        private static object InvokeMethodWithParameters(MethodInfo method, object serviceInstance, List<object> parameters)
        {

            if (method.GetParameters().Length == 0)
            {
                throw new InvokeMethodException("Wrong method implementation: method should have parameters.");
            }

            if (method.ReturnType == typeof(void))
            {
                method.Invoke(serviceInstance, parameters.ToArray());
                return true;
            }
            else
            {
                var result = method.Invoke(serviceInstance, parameters.ToArray());
                if (result != null)
                {
                    return result;
                }
            }
            throw new InvokeMethodException("Method invocation failed");
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
        public static ServiceMethodPair GetClassAndMethodTypes(string serviceName,string methodName)
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
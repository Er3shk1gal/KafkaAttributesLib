using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaAttributesLib.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    public class KafkaMethodAttribute : Attribute
    {
        public string MethodName { get; }
        public KafkaMethodAttribute(string methodName)
        {
            MethodName = methodName;
        }
    }

}
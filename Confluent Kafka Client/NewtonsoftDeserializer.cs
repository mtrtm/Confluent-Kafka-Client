using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace Confluent_Kafka_Client
{
	/// <summary>
	/// todo - implement this using newtonsoft then go back to consumer/producer and make them generic: Guid, TestClassForKafka
	/// </summary>
	/// <typeparam name="T"></typeparam>
	class NewtonsoftDeserializer<T> : IDeserializer<T>
	{
		private readonly Encoding _encoding;

		public NewtonsoftDeserializer(Encoding encoding)
		{
			_encoding = encoding;
		}

		public T Deserialize(string topic, byte[] data)
		{
			//not sure what the topic is for, but confluent's method doesn't use it either!?!?
			//todo - if (data == null) return (T)null;
			var stringToDeserialize = _encoding.GetString(data);
			return JsonConvert.DeserializeObject<T>(stringToDeserialize);
		}

		public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
		{
			return config;
		}
	}
}

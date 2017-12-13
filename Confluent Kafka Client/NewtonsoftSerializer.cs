using System.Collections.Generic;
using System.Text;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace Confluent_Kafka_Client
{
	class NewtonsoftSerializer<T> : ISerializer<T>
	{
		private readonly Encoding _encoding;

		public NewtonsoftSerializer(Encoding encoding)
		{
			_encoding = encoding;
		}

		public byte[] Serialize(string topic, T data)
		{
			var serializedBytes = JsonConvert.SerializeObject(data);
			return _encoding.GetBytes(serializedBytes);
		}

		public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
		{
			return config;
		}
	}
}

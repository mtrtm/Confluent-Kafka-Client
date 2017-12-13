using Confluent.Kafka;
using Ploeh.AutoFixture;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Confluent_Kafka_Client
{
	class TypedExample
	{
		private static Producer<Guid, TestClassForKafka> _producer;
		private static Consumer<Guid, TestClassForKafka> _consumer;

		static void Main(string[] args)
		{
			var topicToProduceAndConsumeFrom = "TestTopic001";
			var configSettings = new Dictionary<string, object>
			{
				{"group.id", "ConfluentClient001" },
				{ "bootstrap.servers", "localhost:9092" },
				{ "default.topic.config", new Dictionary<string, object>()
					{
						{ "auto.offset.reset", "smallest" }
					}
				}
			};

			//use the generic typed consumer/producer since the non-generic is deprecated
			_producer = new Producer<Guid, TestClassForKafka>(configSettings, keySerializer: new NewtonsoftSerializer<Guid>(Encoding.UTF8), valueSerializer: new NewtonsoftSerializer<TestClassForKafka>(Encoding.UTF8));
			ProduceTenMessages(topicToProduceAndConsumeFrom);

			_consumer = new Confluent.Kafka.Consumer<Guid, TestClassForKafka>(configSettings, keyDeserializer: new NewtonsoftDeserializer<Guid>(Encoding.UTF8), valueDeserializer: new NewtonsoftDeserializer<TestClassForKafka>(Encoding.UTF8));
			ConsumeMessages(topicToProduceAndConsumeFrom);

			Console.WriteLine("Complete. Hit any key to exit.");
			Console.ReadLine();
		}

		private static void ProduceTenMessages(string topicToProduceAndConsumeFrom)
		{
			Console.WriteLine("================Producing 10 messages...==================");

			using (_producer)
			{
				Console.WriteLine($"{_producer.Name} producing on {topicToProduceAndConsumeFrom}. q to exit.");

				Fixture fixture = new Fixture();

				var deliveryReports = new List<Task<Message<Guid, TestClassForKafka>>>();

				for (int i = 0; i < 10; i++)
				{
					var objToProduce = fixture.Create<TestClassForKafka>();
					Task<Message<Guid, TestClassForKafka>> deliveryReport = _producer.ProduceAsync(topicToProduceAndConsumeFrom, objToProduce.Id, objToProduce);

					deliveryReports.Add(deliveryReport);
					deliveryReport.ContinueWith(task => { Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}"); });
				}

				//if I do <string, string> typed producer and consumer, 10 seconds works and publishes to localhost but 1 doesn't and doesn't give me an error
				//if I do <Guid, TestClassForKafka> typed producer and consumer, no matter how long I wait, publish does not work and task doesn't complete
				_producer.Flush(TimeSpan.FromSeconds(10));

				foreach (var reportAfterFlush in deliveryReports)
				{
					if (reportAfterFlush.IsCompleted == false)
					{
						Console.WriteLine("task not completed after flush");
					}
					else if (reportAfterFlush.Result.Error.HasError)
					{
						Console.WriteLine($"encountered error: {reportAfterFlush.Result.Error}");
					}
				}
			}

			Console.WriteLine("================Done Producing==================");
		}

		private static void ConsumeMessages(string topicToProduceAndConsumeFrom)
		{
			Console.WriteLine("================Consuming messages...==================");

			_consumer.OnMessage += (_, msg)
				=> Console.WriteLine($"\nCONSUMED! Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} Key:{msg.Key} \nValue: {msg.Value}\n");

			_consumer.OnPartitionEOF += (_, end)
				=> Console.WriteLine($"Reached end of topic {end.Topic} partition {end.Partition}, next message will be at offset {end.Offset}");

			_consumer.OnError += (_, error)
				=> Console.WriteLine($"Error: {error}");

			_consumer.OnConsumeError += (_, msg)
				=> Console.WriteLine($"Error consuming from topic/partition/offset {msg.Topic}/{msg.Partition}/{msg.Offset}: {msg.Error}");

			_consumer.OnOffsetsCommitted += (_, commit) =>
			{
				Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

				if (commit.Error)
				{
					Console.WriteLine($"Failed to commit offsets: {commit.Error}");
				}
				Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
			};

			_consumer.OnPartitionsAssigned += (_, partitions) =>
			{
				Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {_consumer.MemberId}");
				_consumer.Assign(partitions);
			};

			_consumer.OnPartitionsRevoked += (_, partitions) =>
			{
				Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
				_consumer.Unassign();
			};

			_consumer.OnStatistics += (_, json)
				=> Console.WriteLine($"Statistics: {json}");

			_consumer.Subscribe(topicToProduceAndConsumeFrom);



			bool cancelled = false;
			Console.CancelKeyPress += (_, e) =>
			{
				e.Cancel = true; // prevent the process from terminating.
				cancelled = true;
			};

			Console.WriteLine("press Ctrl+C to exit");
			while (!cancelled)
			{
				_consumer.Poll(millisecondsTimeout: 5000);
				Console.WriteLine("\rdone polling");
			}

			Console.WriteLine("================Done consuming messages...==================");
		}
	}
}

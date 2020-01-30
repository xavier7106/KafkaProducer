# KafkaProducer

 
		 producer = newKafkaProducer(Kafka_Producer.newKafkaProducerConfig()
     
				 .Host("127.0.0.1")
         
				 .Port(9092)
         
				 .Buffer_memory(33554432)
         
				 .Retries(1)
         
				 .Linger_ms(1)
         
				 .Retries(16384));
         
		 System.out.println("Producer created");

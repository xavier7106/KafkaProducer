import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Kafka_Producer {

	 Properties props = new Properties();
	 private String Host;
	 private int Port;
	 private int Retries;
	 private int Batch_size;
	 private int Linger_ms;
	 private int Buffer_memory;
	 
	 
	 private static Producer<String, String> producer;
	 
	 private Kafka_Producer(Builder builder)
		{
			setHost(builder.Host);
			setPort(builder.Port);
			setRetries(builder.Retries);
			setBatch_size(builder.Batch_size);
			setLinger_ms(builder.Linger_ms);
			setBuffer_memory(builder.Buffer_memory);
		}
	
	 public static Builder newKafkaProducerConfig(){
			return new Builder();
		}
	 public static Producer newKafkaProducer( Builder builder){
		 	Properties props = new Properties();
			props.put("bootstrap.servers", builder.Host + ":" + builder.Port);
			props.put("acks", "all"); // //Set acknowledgements for producer requests. 
			props.put("retries", builder.Retries);
			props.put("batch.size", builder.Batch_size); //buffer size
			props.put("linger.ms", builder.Linger_ms);
			props.put("buffer.memory", builder.Buffer_memory); //total amount of memory available to the producer for buffering
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<>(props);
			return producer;
		}
	 
	 public void setProducer(Properties  Props) {
		 producer = new KafkaProducer<>(Props);
	 }
	 public Producer<String, String> getProducer() {
		 return producer ;
	 }
	 public void setHost(String host) { this.Host = host;}
	 public void setPort(int port) {this.Port = port;}
	 public void setRetries(int retries) {this.Retries=retries;}
	 public void setBatch_size(int batch_size) {this.Batch_size = batch_size;}
	 public void setLinger_ms(int linger_ms) {this.Linger_ms = linger_ms;}
	 public void setBuffer_memory(int buffer_memory) {this.Buffer_memory = buffer_memory;}
	 
	 public String getHost() { return Host;}
	 public int getPort() {return Port ;}
	 public int getRetries() {return Retries;}
	 public int getBatch_size() {return Batch_size;}
	 public int getLinger_ms() {return Linger_ms;}
	 public int getBuffer_memory() {return Buffer_memory;}
	 
	 public static final class Builder{
		 private String Host;
		 private int Port;
		 private int Retries;
		 private int Batch_size;
		 private int Linger_ms;
		 private int Buffer_memory;
			
		    public Builder Host(String host)
			{
				this.Host=  host;
				return this;
			}
			public Builder Port(int port)
			{
				this.Port=  port;
				return this;
			}
			public Builder Retries(int retries)
			{
				this.Retries =  retries;
				return this;
			}
			public Builder Batch_size(int batch_size)
			{
				this.Batch_size = batch_size;
				return this;
			}
			public Builder Linger_ms(int linger_ms)
			{
				this.Linger_ms = linger_ms;
				return this;
			}
			public Builder Buffer_memory(int buffer_memory)
			{
				this.Buffer_memory= buffer_memory;
				return this;
			}
			 
	 }
	 
	 public static void main(String[] args) throws Exception {
		 producer = newKafkaProducer(Kafka_Producer.newKafkaProducerConfig()
				 .Host("127.0.0.1")
				 .Port(9092)
				 .Buffer_memory(33554432)
				 .Retries(1)
				 .Linger_ms(1)
				 .Retries(16384));
		 System.out.println("Producer created");

	 }
	 
}

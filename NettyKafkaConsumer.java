package http.Snoop.netty;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 public class NettyKafkaConsumer {
    private ConsumerConnector consumerConnector = null;
    private final String topic = "test-topic";
    static String message = null;
    private static final String FILENAME = "E:\\Documents\\Kafka.txt";

    public void initialize() {
          Properties props = new Properties();
          props.put("zookeeper.connect", "localhost:2181");
          props.put("group.id", "testgroup");
          props.put("zookeeper.session.timeout.ms", "400");
          props.put("zookeeper.sync.time.ms", "300");
          props.put("auto.commit.interval.ms", "1000");
          props.put("auto.offset.reset", "smallest");
          ConsumerConfig conConfig = new ConsumerConfig(props);
          consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() throws UnsupportedEncodingException{
          Map<String, Integer> topicCount = new HashMap<String, Integer>();       
          topicCount.put(topic, new Integer(1));
          Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);         
          List<KafkaStream<byte[], byte[]>> kStreamList =
                                               consumerStreams.get(topic);
          for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
                 ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
                 while (consumerIte.hasNext())
                        System.out.println("Message consumed from topic[" + topic + "] : "       +
                                        new String(consumerIte.next().message()));
                 message += new String(consumerIte.next().message());
          }
          if (consumerConnector != null)   consumerConnector.shutdown();          
    }
	public static void main(String[] args) throws InterruptedException, IOException{
          NettyKafkaConsumer kafkaConsumer = new NettyKafkaConsumer();
          kafkaConsumer.initialize();
          kafkaConsumer.consume();
          FileOutputStream outputStream = new FileOutputStream(FILENAME);
          byte[] m = message.getBytes();
          outputStream.write(m);
          outputStream.close();
	}
}
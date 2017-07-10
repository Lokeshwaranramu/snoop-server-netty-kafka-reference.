package http.Snoop.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.zookeeper.ZooKeeper;
import http.Snoop.netty.HttpSnoopServerHandler;
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class NettyKafkaProducer
{
    public static void main(String[] args) throws Exception
    {
    	 ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, null);
    	    List<String> brokerList = new ArrayList<String>();

    	    List<String> ids = zk.getChildren("/brokers/ids", false);
    	    for (String id : ids) {
    	        String brokerInfoString = new String(zk.getData("/brokers/ids/" + id, false, null));
    	        Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfoString);
    	        if (broker != null) {
    	            brokerList.add(broker.getConnectionString());
    	        }
    	    }
    	Properties props=new Properties();
    	props.put("metadata.broker.list", String.join(",", brokerList)); 
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        System.out.println("Broker Initialized");
        ProducerConfig config=new ProducerConfig(props);
        Producer<String, String> kafkaProducer=new Producer<String, String>(config);
        new HttpSnoopServerHandler();
        	KeyedMessage<String, String> data=new KeyedMessage<String, String>("test-topic", HttpSnoopServerHandler.uri);
        	kafkaProducer.send(data);
        System.out.println("Data sending completed!");
       kafkaProducer.close();
    }
}
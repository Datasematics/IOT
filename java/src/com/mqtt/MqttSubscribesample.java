import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

public class MqttSubscribesample implements MqttCallback{

	public final static Logger logger = Logger.getLogger(MqttSubscribesample.class);
	
    public static void main(String[] args){
    
    	new MqttSubscribesample().subscribe();
    }
    
    public  void subscribe(){
    	
        String topic = "nsl";
        String content = "Message from MqttPublishSample";
        int qos = 2;
        String broker = "tcp://iot.eclipse.org:1883";
        String clientId = "JavaSubcribe";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            sampleClient.setTimeToWait(1234);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            
            connOpts.setUserName("code");
            connOpts.setPassword("code@123".toCharArray());
            connOpts.setCleanSession(true);
            connOpts.setWill(sampleClient.getTopic(topic),
            		"I'm gone".getBytes(), 2, true);
            System.out.println("Connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            sampleClient.subscribe(topic,qos);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            sampleClient.publish(topic, message);
            System.out.println("Connected");
           sampleClient.setCallback(this);

        } catch(MqttException me){
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("except " + me);
            me.printStackTrace();
        }
    }
    
    @Override
    public void connectionLost(Throwable cause) { //Called when the client lost the connection to the broker

    	logger.info("Connection lost!"+cause.getMessage());
    	logger.info("reason " + ((MqttException) cause).getReasonCode());
    	logger.info("msg " + cause.getMessage());
    	logger.info("loc " + cause.getLocalizedMessage());
    	logger.info("cause " + cause.getCause());
    	logger.info("except " + cause);
    
    
    
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {// called when ever a new message is arrived
    /* kafkaProducerMqtt k1= new kafkaProducerMqtt();
    k1.insert_to_topic(message.toString());
*/
    	logger.info("----------Date inserted---------------------------------------");
    	System.out.println(message.toString());
      
    }

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		System.out.println("****");
	}

    
}

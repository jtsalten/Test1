package IOT;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.intersys.gateway.BusinessOperation;

public class PublishBO implements BusinessOperation {
	
	public static final String SETTINGS = "MqttTopic,LogFile";
	private PrintWriter logFile = new PrintWriter(System.out,true);
	private String mqttTopic;
	private MqttClient client;
	
	@Override
	public boolean onInitBO(String[] args) throws Exception {
		for (int i = 0; i < args.length-1; i++) {
			if (args[i] != null && args[i].equals("-LogFile")) {
				logFile = new PrintWriter(new FileOutputStream(args[++i], true), true);
			}
			else if (args[i] != null && args[i].equals("-MqttTopic")) {
				mqttTopic = args[++i];
			}
		}
		
		logFile.print("Starting up with arguments: ");
		for (String arg : args) {
			logFile.print(arg+" ");
		}
		logFile.println();
		
		client = new MqttClient("tcp://localhost:1883",MqttClient.generateClientId());
		client.connect();
		
		logFile.println(client.toString());
		
		return true;
	}

	@Override
	public boolean onMessage(String msgTxt) throws Exception {
		logFile.println("********************* onMessage del BO ********************");
		logFile.println("Parametro MqttTopic: "+mqttTopic+" Argumento: "+msgTxt);
	
		MqttMessage message = new MqttMessage();
		message.setPayload(msgTxt.getBytes());
		client.publish(mqttTopic,message);
		return true;
	}

	@Override
	public boolean onTearDownBO() throws Exception {
		// TODO Auto-generated method stub
		client.disconnect();
		return true;
	}

}



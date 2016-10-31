package IOT;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.intersys.gateway.BusinessService;
import com.intersys.gateway.Production;
import com.intersys.gateway.Production.Severity;

public class ListenBS implements BusinessService, MqttCallback {
	public static final String SETTINGS = "MQTTTopic";
	private String mqttTopic;
	private MqttClient client;
	private Production production;
	
	public boolean onInitBS(Production prod) throws Exception {
		try {
			this.production = prod;
			this.production.logMessage("java, OnInitBS", com.intersys.gateway.Production.Severity.TRACE);
			
			this.mqttTopic = prod.getSetting("MQTTTopic");
			
			//Create the MqttClient connection to the broker
			client = new MqttClient("tcp://localhost:1883",MqttClient.generateClientId());
			client.connect();
			this.production.logMessage("java, connected", com.intersys.gateway.Production.Severity.TRACE);
			
			//Subscribe to topic	
			String[]myTopics = {this.mqttTopic};
			
			client.subscribe(myTopics);
			this.production.logMessage("java, subscribed to: "+this.mqttTopic, com.intersys.gateway.Production.Severity.TRACE);
			client.setCallback(this);
		}
		catch (Exception e) {
			try {
				production.logMessage(e.toString(), Severity.ERROR);
				production.setStatus(Production.Status.ERROR);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return true;
	}
	
	@Override
	public boolean onTearDownBS() throws Exception {
		this.production.logMessage("java:onTearDownBS", com.intersys.gateway.Production.Severity.TRACE);
		client.disconnect();
		return true;
	}
	
	//From MqttCallback
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		try {
			this.production.logMessage("java, connectionLost", com.intersys.gateway.Production.Severity.TRACE);
		} catch (Exception e)
		{
			//Nothing to do, really
		}
	}
	
	//From MqttCallback interface
	public void deliveryComplete(IMqttDeliveryToken token) {
		//TODO Auto-generated method stub
		try {
			this.production.logMessage("java, Delivery Complete", com.intersys.gateway.Production.Severity.TRACE);
			this.production.logMessage("java, message Delivered, token:"+token.toString(), com.intersys.gateway.Production.Severity.TRACE);
		} catch (Exception e)
		{
			//Nothing to do, really
			//Nothing to do, really
		}
	}
	
	///From MqttCallback interface
		public void messageArrived(String topic, MqttMessage message) throws Exception {
			this.production.logMessage("java, messageArrived", com.intersys.gateway.Production.Severity.TRACE);
			this.production.logMessage("java, topic:  "+topic+"; Message:"+message.toString(),com.intersys.gateway.Production.Severity.TRACE);
			String xmlMessage="<message><type>"+topic+"</type><content>"+message.toString()+"</content></message>";
			
			this.production.logMessage("java, xmlMessage:"+xmlMessage,com.intersys.gateway.Production.Severity.TRACE);
			this.production.sendRequest(xmlMessage);
		}
	
}

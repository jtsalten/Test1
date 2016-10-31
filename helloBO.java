package Summit;

import java.io.FileOutputStream;
import java.io.PrintWriter;

import com.intersys.gateway.BusinessOperation;

public class helloBO implements BusinessOperation {
	public static final String SETTINGS = "LogFile";
	private PrintWriter logFile = new PrintWriter(System.out, true);
	
	@Override
	public boolean onInitBO(String[] args) throws Exception {
		for (int i = 0; i < args.length-1; i++) {
			if (args[i] != null && args[i].equals("-LogFile")) {
				logFile = new PrintWriter(new FileOutputStream(args[++i], true), true);
			}
		}

		logFile.print("Starting up with arguments: ");
		for (String arg : args) {
			logFile.print(arg+" ");
		}
		logFile.println();
		return true;
	}

	@Override
	public boolean onMessage(String message) throws Exception {
		logFile.print("Received Message: "+message);
		return true;
	}

	@Override
	public boolean onTearDownBO() throws Exception {
		return true;
	}

}

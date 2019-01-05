package org.cripac.isee.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Level;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

public class ConfUtil {
	protected transient Logger logger = new ConsoleLogger(Level.INFO);
	Properties sysProps = new Properties();
	public static String mysqlIp="";
	public static String neo4jIp="";
	public static String hdfsMaster="";

	public void getConf() {
		InputStream is = getClass().getResourceAsStream("/conf/ip.properties");
		try {
			sysProps.load(is);
		} catch (IOException e) {
			logger.error("Error on loading system properties file from JAR file", e);
			System.exit(0); 
		}
	
	  for (Entry<Object, Object> entry : sysProps.entrySet()) {
          switch ((String) entry.getKey()) {
              case "zk.connect":
//                  zkConn = (String) entry.getValue();
                  break;
          }
          sysProps.remove(entry);
      }
	}
}

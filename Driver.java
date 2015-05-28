import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class Driver {
    private static final int ARGLENGTH = 5;
        
    public static void main(String[] args) throws Exception {
        String DatabaseName = null;
        String Table1Name = null;
        String Table2Name = null;
        String HiveMetaStoreLocation = null;
        if (args.length != ARGLENGTH){
            usage();
            return;
        }
        if (args[0].equals("debug=true")) {
            Properties log4jProperties = new Properties();
            log4jProperties.setProperty("log4j.rootLogger", "DEBUG, theConsoleAppender");
            log4jProperties.setProperty("log4j.appender.theConsoleAppender","org.apache.log4j.ConsoleAppender");
            log4jProperties.setProperty("log4j.appender.theConsoleAppender.layout","org.apache.log4j.PatternLayout");
            log4jProperties.setProperty("log4j.appender.theConsoleAppender.layout.ConversionPattern","%d [%t] %-5p %c %x - %m%n");
            PropertyConfigurator.configure(log4jProperties);
        }
        HiveMetaStoreLocation = args[1];
        DatabaseName = args[2];
        Table1Name = args[3];
        Table2Name = args[4];
        
        outputRowCount(HiveMetaStoreLocation,DatabaseName,Table1Name,Table2Name);
        org.apache.hadoop.fs.FileSystem.closeAll();
    }
    
   private static void usage() {
        System.out.println("Usage: java -classpath $CLASSPATH Driver debug=[true|false] HiveMetastoreAddress database table1 table2");
        
    }

public static void outputRowCount(String HiveMetaStoreLocation, String DatabaseName, String Table1Name, String Table2Name) throws Exception {
       int splitID = 0;
       System.setProperty("HADOOP_USER_NAME", "hive");

       // connect to HCatalog
       HCatalog hc = new HCatalog(HiveMetaStoreLocation, "9083",
               "hive", 300, true, 5, false);
       hc.Connect(DatabaseName, Table1Name);
       List<InputSplit> ipSplits = hc.GetRawInputSplits();
       
       hc.Connect(DatabaseName, Table2Name);
       List<InputSplit> ipSplits_B = hc.GetRawInputSplits();
       
       
       for (InputSplit i : ipSplits) {
           
           new TableReader("Thread-A-" + splitID++, HiveMetaStoreLocation, DatabaseName, Table2Name, i).start();
       }
       splitID = 0;
       for (InputSplit i : ipSplits_B) {
           
           new TableReader("Thread-B-" + splitID++, HiveMetaStoreLocation, DatabaseName, Table1Name,i).start();
       }
//       splitID = 0;
//       for (InputSplit i : ipSplits_B) {
//           
//           new TableReader("Thread-C-" + splitID++, HiveMetaStoreLocation, DatabaseName, Table1Name,i).start();
//       }
   }

}

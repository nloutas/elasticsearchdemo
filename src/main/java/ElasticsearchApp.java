import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;



/**
 * Elasticsearch application entry point.
 */
public class ElasticsearchApp {
  private static ElasticClient ec;

  /**
   * @param args String array
   */
  public static void main(String[] args) {
    try {
      ec = new ElasticClient("traffic", "100001");
    } catch (UnknownHostException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    HashMap<String, String> entry = new HashMap<String, String>();
    entry.put("Teid", "1");
    entry.put("UsageData", "100Kb");
    entry.put("Cost", "0.1");
    entry.put("Currency", "Euro");
    entry.put("TrafficType", "RX");
    entry.put("Imsi", "2200000001");
    entry.put("Organisation", "Enterprise 11");
    entry.put("Sender", "Vodafone DE");
    entry.put("Recipient", "EPlus");
    entry.put("Timestamp", (new Date()).toString());

    ec.indexEntry(UUID.randomUUID().toString(), entry);

    ec.close();
  }



}

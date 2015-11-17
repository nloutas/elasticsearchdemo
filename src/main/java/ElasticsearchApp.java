
import org.joda.time.DateTime;
import java.net.UnknownHostException;
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
    entry.put("teid", "1");
    entry.put("endpoint_id", "932");
    entry.put("endpoint_name", "My Endpoint");
    entry.put("sim_id", "123");
    entry.put("iccid", "0527242926180300002");
    entry.put("imsi_id", "321");
    entry.put("imsi", "901430000000105");
    entry.put("cost", "0.1");
    entry.put("currency_id", "1");
    entry.put("currency", "EUR");
    entry.put("organisation_id", "123");
    entry.put("organisation_name", "Enterprise 11");
    entry.put("Sender", "XXANY");
    entry.put("Recipient", "EMNDE");
    entry.put("event_start_timestamp", new DateTime().minusMillis(10000).toDate().toString());
    entry.put("event_stop_timestamp", new DateTime().toDate().toString());
    entry.put("tariff_id", "302");
    entry.put("tariff_name", "Professional");
    entry.put("ratezone_id", "123");
    entry.put("ratezone_name", "EU+");
    entry.put("operator_id", "2");
    entry.put("operator_name", "ANY");
    entry.put("country_id", "2");
    entry.put("country_name", "ANY");
    entry.put("traffic_type_id", "1");
    entry.put("traffic_type", "Data");
    entry.put("volume", "0.04284");
    entry.put("volume_tx", "0.02142");
    entry.put("volume_rx", "0.02142");

    ec.indexEntry(UUID.randomUUID().toString(), entry);

    ec.close();
  }



}

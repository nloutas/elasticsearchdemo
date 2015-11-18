package bsh.commands;

import bsh.CallStack;
import bsh.Interpreter;

import com.emnify.es.ElasticClient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;

/**
 * @author nikos
 *
 */
public class esCmd {

  /**
   * status OK
   */
  public static final int OK_STATUS = 0;
  /**
   * status Error
   */
  public static final int NOK_STATUS = 1;

  private static ElasticClient ec = new ElasticClient("traffic", "100001");

  private esCmd() {
  }


  /**
   * Send {count} pings of length {length} from {srcAddr} to {destAddr} with teid {teid}
   *
   * @param env Interpreter
   * @param callstack CallStack
   * @param command to execute
   * @param id of es entry
   * @return int status
   * @throws Exception upwards
   */
  public static int invoke(Interpreter env, CallStack callstack, String command, String id)
      throws Exception {
    env.println("Running command '" + command + "' with parameter '" + id + "'");

    boolean success = false;
    if (id.length() == 0) {
      id = UUID.randomUUID().toString();
    }

    switch (command) {
      case "index":
        success = ec.indexEntry(id, getEntry());
        break;
      case "read":
        Map<String, ?> entry = ec.readEntry(id);
        if (entry != null) {
          success = true;
          env.println("Found entry: " + jsonBuilder().map(entry).string());
        }
        break;
      case "delete":
        success = ec.deleteEntry(id);
        break;
      case "count":
        env.println("count result: " + ec.count());
        success = true;
        break;
      case "search":
        SearchHits hits = ec.search(id, 0, 0, true);
        if (hits != null) {
          success = true;
          env.println("search total hits: " + hits.getTotalHits());
          hits.forEach(hit -> {
            env.println("Found entry with id " + hit.getId() + ", score " + hit.getScore()
                + ", source " + hit.getSourceAsString());
          });
        }

        break;
      default:
        env.error("Uknown command " + command + "!");
        return NOK_STATUS;
    }


    if (!success) {
      env.error("Command " + command + " failed!");
      return NOK_STATUS;
    } else {
      return OK_STATUS;
    }

  }



  private static HashMap<String, String> getEntry() {

    Random r = new Random();
    double volume = r.nextDouble();

    HashMap<String, String> entry = new HashMap<String, String>();

    entry.put("teid", "1");
    entry.put("endpoint_id", "" + r.nextInt(10000));
    entry.put("endpoint_name", "My Endpoint");
    entry.put("sim_id", "" + r.nextInt(10000));
    entry.put("iccid", "0527242926180300002");
    entry.put("imsi_id", "321");
    entry.put("imsi", "" + r.nextInt(99143105));
    entry.put("cost", "" + (volume * 0.1));
    entry.put("currency_id", "1");
    entry.put("currency", "EUR");
    entry.put("organisation_id", "11");
    entry.put("organisation_name", "Enterprise 11");
    entry.put("Sender", "XXANY");
    entry.put("Recipient", "EMNDE");
    entry.put("event_start_timestamp", new DateTime().minusMillis(10000).toDate().toString());
    entry.put("event_stop_timestamp", new DateTime().toDate().toString());
    entry.put("tariff_id", "" + r.nextInt(99));
    entry.put("tariff_name", "Professional");
    entry.put("ratezone_id", "" + r.nextInt(99));
    entry.put("ratezone_name", "EU+");
    entry.put("operator_id", "2");
    entry.put("operator_name", "ANY");
    entry.put("country_id", "2");
    entry.put("country_name", "ANY");
    entry.put("traffic_type_id", "1");
    entry.put("traffic_type", "Data");
    entry.put("volume", "" + volume);
    entry.put("volume_tx", "" + (volume / 2));
    entry.put("volume_rx", "" + (volume / 2));
    return entry;
  }


}

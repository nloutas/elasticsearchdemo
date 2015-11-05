

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * elasticsearch client
 *
 */
public class ElasticClient {

  private Settings settings = Settings.settingsBuilder().loadFromSource("elasticsearch.yml")
      .build();
  private TransportClient client = TransportClient.builder().settings(settings).build();

  /**
   * constructor
   *
   * @throws UnknownHostException elasticHost is invalid
   */
  public ElasticClient() throws UnknownHostException {

    String elasticHost = settings.get("elasticHost", "localhost");
    Integer elasticPort = settings.getAsInt("elasticPort", 9300);
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticHost),
        elasticPort));

  }

  // on shutdown client.close();


}

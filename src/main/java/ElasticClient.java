

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * elasticsearch client
 *
 */
public class ElasticClient {

  private Settings settings;
  private TransportClient client;


  /**
   * constructor
   *
   * @throws UnknownHostException elasticHost is invalid
   */
  public ElasticClient() throws UnknownHostException {

    try {
      InputStream is = getClass().getClassLoader().getResource("elasticsearch.yml").openStream();
      settings = Settings.settingsBuilder().loadFromStream("elasticsearch.yml", is).build();
      client = TransportClient.builder().settings(settings).build();
    } catch (IOException e) {

      System.err.println("Cannot load configuration from elasticsearch.yml");
    }
    String elasticHost = settings.get("network.host", "localhost");
    Integer elasticPort = settings.getAsInt("http.port", 9200);
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticHost),
        elasticPort));

  }

  // on shutdown client.close();


}

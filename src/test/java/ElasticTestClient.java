


import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.io.InputStream;


/**
 * elasticsearch client
 *
 */
public class ElasticTestClient implements AutoCloseable {
  private Settings settings;
  private Node node;
  private Client esClient;

  /**
   *
   */
  public ElasticTestClient() {

    try {
      InputStream is = getClass().getClassLoader().getResource("elasticsearch.yml").openStream();
      settings = Settings.settingsBuilder().loadFromStream("elasticsearch.yml", is).build();
      node = nodeBuilder().settings(settings).local(true).node();
    } catch (IOException e) {
      System.err.println("Cannot load configuration from elasticsearch.yml");
    }
  }

  /**
   * initialise the client
   *
   * @throws Exception upwards
   */
  public void start() throws Exception {

    if (esClient != null) {
      throw new IllegalStateException("ElasticTestClient already started");
    }

    try {
      esClient = node.client();
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  /**
   * @return Client
   */
  public Client getClient() {
    return esClient;
  }


  public void close() {
    if (esClient != null) {
      esClient.close();
    }
  }

}




import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;


/**
 * elasticsearch client
 *
 */
public class ElasticTestClient implements AutoCloseable {
  private Settings settings = Settings.settingsBuilder()
      .put("path.home", "/tmp/elasticsearch")
      .loadFromSource("elasticsearch.yml").build();
  private Node node = nodeBuilder().settings(settings).local(true).node();
  private Client esClient;

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

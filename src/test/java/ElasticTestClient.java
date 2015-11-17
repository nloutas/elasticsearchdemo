
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * elasticsearch test client
 *
 */
public class ElasticTestClient implements AutoCloseable {
  private final Logger log = LoggerFactory.getLogger(this.getClass().getName());
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

      // cleanup old test data
      File dataPath = new File(settings.get("path.home") + "/data/" + settings.get("cluster.name"));
      FileUtils.deleteDirectory(dataPath);

      node = nodeBuilder().settings(settings).local(true).node();
    } catch (IOException e) {
      log.error("Cannot load configuration from elasticsearch.yml");
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
      log.error("Cannot start Client");
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

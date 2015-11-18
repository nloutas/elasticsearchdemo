import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

/**
 * Http Client for Elasticsearch
 *
 */
public class RestClient {
  private String url;
  private Client client;

  /**
   * @param url of elasticsearch
   */
  public RestClient(String host, int port) {
    this.url = "http://" + host + ":" + port;
    client =
        ClientBuilder.newClient().register(JacksonFeature.class)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
  }

  /**
   * @param path to the target
   * @param body JsonString
   * @return Response result of the post request
   */
  public Response post(String index, String type, String id, String body) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request().post(entity(body, APPLICATION_JSON_TYPE));
  }

  /**
   * @param path to the target
   * @param body JsonString
   * @return Response result of the patch request
   */
  public Response patch(String index, String type, String id, String body) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request()
        .method("PATCH", entity(body, APPLICATION_JSON_TYPE));
  }

  /**
   * @param path to the target
   * @return Response result of the get request
   */
  public Response get(String index, String type, String id) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request(APPLICATION_JSON_TYPE).get();
  }

  /**
   * @param path to the target
   * @return Response result of the get request
   */
  public Response put(String index, String type, String id) {
    return this.put(index, type, id, null);
  }

  /**
   * @param path to the target
   * @param body JsonString
   * @return Response result of the get request
   */
  public Response put(String index, String type, String id, String body) {
    String path = getPath(index, type, id);
    if (body != null) {
      return client.target(url).path(path).request(APPLICATION_JSON_TYPE)
          .put(entity(body, APPLICATION_JSON_TYPE));
    } else {
      return client.target(url).path(path).request(APPLICATION_JSON_TYPE)
          .put(entity("", TEXT_PLAIN));
    }
  }

  /**
   * @param path to the target
   * @return Response result of the delete request
   */
  public Response delete(String index, String type, String id) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request().delete();
  }

  public String getPath(String index, String type, String id) {
    return index + "/" + type + "/" + id;
  }
}

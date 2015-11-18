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
   * @param host of elasticsearch
   * @param port http port of elasticsearch (usually 9200)
   */
  public RestClient(String host, int port) {
    this.url = "http://" + host + ":" + port;
    client =
        ClientBuilder.newClient().register(JacksonFeature.class)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry (can be null for indexing)
   * @param body json body
   * @return Response result of the post request
   */
  public Response post(String index, String type, String id, String body) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request().post(entity(body, APPLICATION_JSON_TYPE));
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @param body json body
   * @return Response result of the patch request
   */
  public Response patch(String index, String type, String id, String body) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request()
        .method("PATCH", entity(body, APPLICATION_JSON_TYPE));
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @return Response result of the get request
   */
  public Response get(String index, String type, String id) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request(APPLICATION_JSON_TYPE).get();
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @return Response result of the get request
   */
  public Response put(String index, String type, String id) {
    return this.put(index, type, id, null);
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @param body json body
   * @return Response result of the put request
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
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @param body json body
   * @return Response result of the put request
   */
  public Response search(String index, String type, String body) {
    String path = getPath(index, type, "_search");
    if (body != null) {
      return client.target(url).path(path).request(APPLICATION_JSON_TYPE)
          .method("POST",entity(body, APPLICATION_JSON_TYPE));
    } else {
      return client.target(url).path(path).request(APPLICATION_JSON_TYPE)
          .method("POST",entity("", TEXT_PLAIN));
    }
  }

  /**
   * @param index Index
   * @param type JsonString
   * @param id of the entry
   * @return Response result of the delete request
   */
  public Response delete(String index, String type, String id) {
    String path = getPath(index, type, id);
    return client.target(url).path(path).request().delete();
  }

  /**
   * create path for http request (index/type/id)
   * @param index Index
   * @param type Type
   * @param id entry id, can be null for creation
   * @return String path
   */
  public String getPath(String index, String type, String id) {
    if (id == null || id.isEmpty()) {
      return index + "/" + type + "/";
    }
    return index + "/" + type + "/" + id;
  }
}

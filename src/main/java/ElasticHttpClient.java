import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response;


public class ElasticHttpClient {

  private static Settings settings;
  private static RestClient client;

  public static void main(String[] args) throws InterruptedException, IOException,
      ExecutionException {

    loadConfig();

    client = new RestClient(settings.get("network.host"), settings.getAsInt("http.port", 6666));

    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().field("user", "Bier")
            .field("postDate", new Date()).field("message", "yo").endObject();

    Response res = client.put("twitter", "tweet", "1", builder.string());

    System.out.println("INDEX: " + res.readEntity(String.class));


    res = client.get("twitter", "tweet", "1");

    System.out.println("GET: " + res.readEntity(String.class));

    XContentBuilder queryBuilder =
        XContentFactory.jsonBuilder().startObject().field("version",true).startObject("query").startObject("match").field("user", "bier").endObject().endObject().endObject();
    Thread.sleep(5000L);
    res = client.search("twitter", "tweet", queryBuilder.string());
    
    System.out.println("Search request body: " + queryBuilder.string());
    
    System.out.println("SEARCH: " + res.readEntity(String.class));
    
    builder = XContentFactory.jsonBuilder().startObject().field("user", "male").endObject();

    res = client.put("twitter", "tweet", "1", builder.string());

    System.out.println("UPDATE: " + res.readEntity(String.class));
    Thread.sleep(5000L);
    res = client.search("twitter", "tweet", queryBuilder.string());
    System.out.println("SEARCH: " + res.readEntity(String.class));
    

    res = client.get("twitter", "tweet", "1");

    System.out.println("GET: " + res.readEntity(String.class));


    res = client.delete("twitter", "tweet", "1");

    System.out.println("DELETE: " + res.readEntity(String.class));



    res = client.delete("twitter", "tweet", "1");

    System.out.println("DELETE: " + res.readEntity(String.class));



    res = client.get("twitter", "tweet", "1");

    System.out.println("GET: " + res.readEntity(String.class));
  }

  public static void loadConfig() throws IOException {
    InputStream is =
        ElasticHttpClient.class.getClassLoader().getResource("elasticsearch.yml").openStream();
    settings = Settings.settingsBuilder().loadFromStream("elasticsearch.yml", is).build();
  }
}

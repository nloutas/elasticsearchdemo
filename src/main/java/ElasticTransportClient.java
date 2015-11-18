import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.ExecutionException;


public class ElasticTransportClient {

  private static Settings settings;
  private static TransportClient client;

  public static void main(String[] args) throws InterruptedException, IOException,
      ExecutionException {

    loadConfig();

    client = TransportClient.builder().build();

    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(settings
        .get("network.host")), settings.getAsInt("network.transport.tcp.port", 6666)));

    System.out.println(client.transportAddresses());



    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().field("user", "Bier")
            .field("postDate", new Date()).field("message", "yo").endObject();

    IndexRequest request = new IndexRequest("twitter", "tweet", "1");
    request.source(builder);

    IndexResponse index = client.index(request).get();

    System.out.println("INDEX: " + index.toString());



    GetRequest getReq = new GetRequest("twitter", "tweet", "1");

    GetResponse get = client.get(getReq).get();

    System.out.println("GET: " + get.getSource());



    UpdateRequest updateReq = new UpdateRequest("twitter", "tweet", "1");
    updateReq.doc(XContentFactory.jsonBuilder().startObject().field("user", "male").endObject());

    UpdateResponse update = client.update(updateReq).get();

    System.out.println("UPDATE: " + update);



    get = client.get(getReq).get();

    System.out.println("GET: " + get.getSource());



    DeleteRequest delReq = new DeleteRequest("twitter", "tweet", "1");

    DeleteResponse delete = client.delete(delReq).get();

    System.out.println("DELETE: " + delete.isFound());



    delete = client.delete(delReq).get();

    System.out.println("DELETE: " + delete.isFound());



    get = client.get(getReq).get();

    System.out.println("GET: " + get.getSource());



    client.close();
  }

  public static void loadConfig() throws IOException {
    InputStream is =
        ElasticTransportClient.class.getClassLoader().getResource("elasticsearch.yml").openStream();
    settings = Settings.settingsBuilder().loadFromStream("elasticsearch.yml", is).build();
  }
}

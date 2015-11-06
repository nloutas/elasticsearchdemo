

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import java.util.Map;

/**
 * elasticsearch client
 *
 */
public class ElasticIndexTest {
  ElasticTestClient etc = new ElasticTestClient();
  private Client esClient;

  @SuppressWarnings("javadoc")
  @Rule
  public ErrorCollector collector = new ErrorCollector();

  /**
   * start esClient
   *
   * @throws Exception when the ES client cannot properly start
   */
  @Before
  public void setUp() throws Exception {
    etc.start();
    esClient = etc.getClient();

  }

  /**
   * stop esClient
   */
  @After
  public void tearDown() {
    etc.close();
  }

  /**
   * @throws Exception upwards
   */
  @Test
  public void esTest() throws Exception {
    String idxName = "index-name";
    String idxType = "index-type";
    String id = "1";


    // generate JSON
    XContentBuilder builder = jsonBuilder().startObject()
        .field("user", "emnifyES")
        .field("message", "trying out Elasticsearch")
        .endObject();

    String json = builder.string();

    // Index
    IndexResponse idxResponse = esClient.prepareIndex(idxName, idxType, id).setTTL(86400000L)
        .setSource(json).execute().actionGet();

    collector.checkThat(idxResponse.getIndex(), equalTo(idxName));
    collector.checkThat(idxResponse.getType(), equalTo(idxType));
    collector.checkThat(idxResponse.getVersion(), equalTo(1L));
    collector.checkThat(idxResponse.isCreated(), is(true));
    collector.checkThat(idxResponse.getId(), equalTo(id));

    // GET
    GetResponse getResponse = esClient.prepareGet(idxName, idxType, id).execute().actionGet();
    collector.checkThat(getResponse.getId(), equalTo(id));
    Map<String, Object> fMap = getResponse.getSource();
    collector.checkThat(fMap.get("user").toString(), equalTo("emnifyES"));
    collector.checkThat(fMap.get("message").toString(), equalTo("trying out Elasticsearch"));

    // Update with dynamic script: requires "script.disable_dynamic: false"
    Script updateSript = new Script("ctx._source.message = \"updated message\"",
        ScriptService.ScriptType.INLINE, null, null);
    UpdateResponse updateResponse =
        esClient.prepareUpdate(idxName, idxType, id).setScript(updateSript).get();

    collector.checkThat(updateResponse.getVersion(), equalTo(2L));
    collector.checkThat(updateResponse.isCreated(), is(false));

    // GET updated message
    getResponse = esClient.prepareGet(idxName, idxType, id).execute().actionGet();
    collector.checkThat(getResponse.getSource().get("message").toString(),
        equalTo("updated message"));

    //Search
    SearchResponse searchResponse = esClient.prepareSearch(idxName).setTypes(idxType)
        .setPostFilter(QueryBuilders.existsQuery("message"))
        .setFrom(0).setSize(10).setExplain(true)
        .execute().actionGet();

    collector.checkThat(searchResponse.status(), equalTo(RestStatus.OK));
    collector.checkThat(searchResponse.getSuccessfulShards(), equalTo(5));
    collector.checkThat(searchResponse.getFailedShards(), equalTo(0));
    collector.checkThat(searchResponse.isContextEmpty(), is(true));
    collector.checkThat(searchResponse.getHits().getTotalHits(), equalTo(1L));


    // Count
    CountResponse count = esClient.prepareCount(idxName).execute().actionGet();
    collector.checkThat(count.status(), equalTo(RestStatus.OK));
    collector.checkThat(count.getCount(), equalTo(1L));

    // DELETE
    DeleteResponse delResponse = esClient.prepareDelete(idxName, idxType, id).execute().actionGet();
    collector.checkThat(delResponse.isFound(), is(true));
    collector.checkThat(delResponse.getVersion(), equalTo(3L));

    // GET deleted
    getResponse = esClient.prepareGet(idxName, idxType, id).execute().actionGet();
    collector.checkThat(getResponse.isExists(), is(false));


  }


}

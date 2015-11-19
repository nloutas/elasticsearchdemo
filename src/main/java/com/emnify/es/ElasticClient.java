

package com.emnify.es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * elasticsearch client
 *
 */
public class ElasticClient {

  private final Logger log = LoggerFactory.getLogger(this.getClass().getName());
  private Settings settings;
  private TransportClient esClient;

  private String idxName = "traffic";
  private String idxType = "data";


  /**
   * @param idxName String
   * @param idxType String
   */
  public ElasticClient(String idxName, String idxType) {
    this();
    this.idxName = idxName;
    this.idxType = idxType;
  }

  /**
   * constructor
   *
   */
  public ElasticClient() {

    loadConfig();
    esClient = TransportClient.builder().settings(settings).build();
    String host = settings.get("network.host", "localhost");
    Integer port = settings.getAsInt("network.transport.tcp.port", 9300);
    try {
      esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    } catch (UnknownHostException e) {
      log.error("Cannot resolve host " + host);
      System.exit(-1);
    }

    esClient.connectedNodes().forEach(action -> {
      log.info("Connected Node HostAddress: " + action.getHostAddress());
    });
  }

  private void loadConfig() {
    try {
      InputStream is = getClass().getClassLoader().getResource("elasticsearch.yml").openStream();
      settings = Settings.settingsBuilder().loadFromStream("elasticsearch.yml", is).build();
    } catch (IOException e) {
      log.error("Cannot load configuration from elasticsearch.yml");
      System.exit(-1);
    }
  }

  /**
   * @param settingName String
   * @return String setting value
   */
  public String getSetting(String settingName) {
    return settings.get(settingName, "Unknown setting");
  }

  /**
   * close the es client
   */
  public void close() {
    if (esClient != null) {
      esClient.close();
    }
  }

  /**
   * @param id String
   * @param entry HashMap of String keys
   * @return boolean created or failed
   */
  public boolean indexEntry(String id, HashMap<String, ?> entry) {

    try {
      // Index
      IndexResponse idxResponse = esClient.prepareIndex(idxName, idxType, id).setTTL(86400000L)
          .setSource(jsonBuilder().map(entry).string()).execute().actionGet();
      return idxResponse.isCreated();
    } catch (IOException e) {
      log.error("Failed to index entry: '" + entry + "'. Exception: " + e.getMessage());
    } catch (Exception e) {
      log.error("Exception: " + e.getMessage());
    }
    return false;
  }

  /**
   * @param id String
   * @return Map
   */
  public Map<String, ?> readEntry(String id) {
    GetResponse getResponse = esClient.prepareGet(idxName, idxType, id).execute().actionGet();
    if (getResponse.getId().equalsIgnoreCase(id))
      return getResponse.getSource();

    return null;
  }


  /**
   * @return long number of entries in the index
   */
  public long count() {
    CountResponse cRes = esClient.prepareCount(idxName).execute().actionGet();
    if (cRes.status().equals(RestStatus.OK)) {
      return cRes.getCount();
    }
    return 0;
  }

  /**
   * @param id String
   * @return boolean if deleted or failed
   */
  public boolean deleteEntry(String id) {
    DeleteResponse delResponse = esClient.prepareDelete(idxName, idxType, id).execute().actionGet();
    return delResponse.isFound();
  }

  /**
   * @param id String
   * @param entry HashMap of String keys
   * @return boolean if updated or failed
   */
  public long updateEntry(String id, HashMap<String, ?> entry) {
    try {
      UpdateResponse updateResponse =
          esClient.prepareUpdate(idxName, idxType, id).setDoc(entry).get();

      return updateResponse.getVersion();
    } catch (Exception e) {
      log.error("Exception: " + e.getMessage());
    }
    return 0L;
  }


  /**
   * @param searchTerm String
   * @param from int
   * @param size int
   * @param explain boolean
   * @return SearchHits results
   */
  public SearchHits search(String searchTerm, int from, int size, boolean explain) {
    SearchRequestBuilder search = esClient.prepareSearch(idxName).setTypes(idxType);

    if (!searchTerm.isEmpty()) {
      String[] searchSplit = searchTerm.split(":");
      if (searchSplit.length==2) {
        search.setPostFilter(QueryBuilders.matchQuery(searchSplit[0], searchSplit[1]));
      } else {
        return null;
      }
    }
    if (from > 0) {
      search.setFrom(from);
    }
    if (size > 0) {
      search.setSize(size);
    }
    if (explain) {
      search.setExplain(true);
    }

    SearchResponse searchResponse = search.execute().actionGet();
    if (searchResponse.status().equals(RestStatus.OK)) {
      return searchResponse.getHits();
    }
    return null;
  }


  /**
   * @return the idxName
   */
  public String getIdxName() {
    return idxName;
  }

  /**
   * @param idxName the idxName to set
   */
  public void setIdxName(String idxName) {
    this.idxName = idxName;
  }

  /**
   * @return the idxType
   */
  public String getIdxType() {
    return idxType;
  }

  /**
   * @param idxType the idxType to set
   */
  public void setIdxType(String idxType) {
    this.idxType = idxType;
  }



}

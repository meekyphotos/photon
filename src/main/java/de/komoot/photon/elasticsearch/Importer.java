package de.komoot.photon.elasticsearch;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * elasticsearch importer
 *
 * @author felix
 */
@Slf4j
public class Importer implements de.komoot.photon.Importer {

  private final String indexName = "photon";
  private final String indexType = "place";
  private final Client esClient;
  private final String[] languages;
  private int documentCount;
  private BulkRequestBuilder bulkRequest;

  public Importer(final Client esClient, final String languages) {
    this.esClient = esClient;
    this.bulkRequest = esClient.prepareBulk();
    this.languages = languages.split(",");
  }

  @Override
  public void add(final PhotonDoc doc) {
    try {
      this.bulkRequest.add(this.esClient.prepareIndex(indexName, indexType).
          setSource(Utils.convert(doc, languages)).setId(doc.getUid()));
    } catch (final IOException e) {
      log.error("could not bulk add document {}", doc.getUid(), e);
      return;
    }
    this.documentCount += 1;
    if (this.documentCount > 0 && this.documentCount % 10000 == 0) {
      this.saveDocuments();
    }
  }

  private void saveDocuments() {
    if (this.documentCount < 1) {
      return;
    }

    final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      log.error("error while bulk import:{}", bulkResponse.buildFailureMessage());
    }
    this.bulkRequest = this.esClient.prepareBulk();
  }

  @Override
  public void finish() {
    this.saveDocuments();
    this.documentCount = 0;
  }

  public long count() {
    return this.esClient
        .search(Requests
                    .searchRequest(indexName)
                    .types(indexType)
                    .source(SearchSourceBuilder.searchSource().size(0)))
        .actionGet()
        .getHits()
        .getTotalHits().value;
  }
}

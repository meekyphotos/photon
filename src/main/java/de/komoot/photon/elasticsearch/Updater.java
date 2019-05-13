package de.komoot.photon.elasticsearch;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Utils;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * Updater for elasticsearch
 *
 * @author felix
 */
@Slf4j
public class Updater implements de.komoot.photon.Updater {

  private final Client esClient;
  private final String[] languages;
  private BulkRequestBuilder bulkRequest;

  public Updater(final Client esClient, final String languages) {
    this.esClient = esClient;
    this.bulkRequest = esClient.prepareBulk();
    this.languages = languages.split(",");
  }

  @Override public void finish() {
    this.updateDocuments();
  }

  @Override
  public void updateOrCreate(final PhotonDoc updatedDoc) {
    final boolean exists = this.esClient.get(this.esClient.prepareGet("photon", "place", String.valueOf(updatedDoc.getPlaceId())).request()).actionGet().isExists();
    if (exists) {
      this.update(updatedDoc);
    } else {
      this.create(updatedDoc);
    }
  }

  @Override public void create(final PhotonDoc doc) {
    try {
      this.bulkRequest.add(this.esClient.prepareIndex("photon", "place").setSource(Utils.convert(doc, this.languages)).setId(String.valueOf(doc.getPlaceId())));
    } catch (final IOException e) {
      log.error(String.format("creation of new doc [%s] failed", doc), e);
    }
  }

  @Override public void update(final PhotonDoc doc) {
    try {
      this.bulkRequest.add(this.esClient.prepareUpdate("photon", "place", String.valueOf(doc.getPlaceId())).setDoc(Utils.convert(doc, this.languages)));
    } catch (final IOException e) {
      log.error(String.format("update of new doc [%s] failed", doc), e);
    }
  }

  @Override public void delete(final Long id) {
    this.bulkRequest.add(this.esClient.prepareDelete("photon", "place", String.valueOf(id)));
  }

  private void updateDocuments() {
    if (this.bulkRequest.numberOfActions() == 0) {
      log.warn("Update empty");
      return;
    }
    final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      log.error("error while bulk update: " + bulkResponse.buildFailureMessage());
    }
    this.bulkRequest = this.esClient.prepareBulk();
  }
}

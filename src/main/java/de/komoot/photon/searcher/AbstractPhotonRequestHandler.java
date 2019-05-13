package de.komoot.photon.searcher;

import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.TagFilterQueryBuilder;
import de.komoot.photon.utils.ConvertToJson;

import org.elasticsearch.action.search.SearchResponse;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by Sachin Dole on 2/20/2015.
 */
public abstract class AbstractPhotonRequestHandler<R extends PhotonRequest> implements PhotonRequestHandler<R> {

  private final ElasticsearchSearcher elasticsearchSearcher;

  public AbstractPhotonRequestHandler(final ElasticsearchSearcher elasticsearchSearcher) {
    this.elasticsearchSearcher = elasticsearchSearcher;
  }

  @Override
  public List<JSONObject> handle(final R photonRequest) {
    final TagFilterQueryBuilder queryBuilder = buildQuery(photonRequest);
    // for the case of deduplication we need a bit more results, #300
    final int limit = photonRequest.getLimit();
    final int extLimit = limit > 1 ? (int) Math.round(photonRequest.getLimit() * 1.5) : 1;
    SearchResponse results = elasticsearchSearcher.search(queryBuilder.buildQuery(), extLimit);
    if (results.getHits().getTotalHits().value == 0L) {
      results = elasticsearchSearcher.search(queryBuilder.withLenientMatch().buildQuery(), extLimit);
    }
    List<JSONObject> resultJsonObjects = new ConvertToJson(photonRequest.getLanguage()).convert(results);
    final StreetDupesRemover streetDupesRemover = new StreetDupesRemover(photonRequest.getLanguage());
    resultJsonObjects = streetDupesRemover.execute(resultJsonObjects);
    if (resultJsonObjects.size() > limit) {
      resultJsonObjects = resultJsonObjects.subList(0, limit);
    }
    return resultJsonObjects;
  }

  /**
   * Given a {@link PhotonRequest photon request}, build a {@link TagFilterQueryBuilder photon specific query builder} that can be used in the {@link
   * AbstractPhotonRequestHandler#handle handle} method to execute the search.
   */
  abstract TagFilterQueryBuilder buildQuery(R photonRequest);
}

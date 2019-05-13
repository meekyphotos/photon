package de.komoot.photon.searcher;

import de.komoot.photon.query.ReverseQueryBuilder;
import de.komoot.photon.query.ReverseRequest;
import de.komoot.photon.query.TagFilterQueryBuilder;

/**
 * @author svantulden
 */
public class SimpleReverseRequestHandler extends AbstractReverseRequestHandler<ReverseRequest> {

  public SimpleReverseRequestHandler(final ElasticsearchReverseSearcher elasticsearchSearcher) {
    super(elasticsearchSearcher);
  }

  @Override
  public TagFilterQueryBuilder buildQuery(final ReverseRequest photonRequest) {
    return ReverseQueryBuilder.builder(photonRequest.getLocation(), photonRequest.getRadius(), photonRequest.getQueryStringFilter());
  }
}

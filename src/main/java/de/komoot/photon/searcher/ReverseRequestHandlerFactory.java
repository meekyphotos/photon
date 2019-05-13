package de.komoot.photon.searcher;

import de.komoot.photon.query.ReverseRequest;

/**
 * @author svantulden
 */
public class ReverseRequestHandlerFactory {

  private final ElasticsearchReverseSearcher elasticsearchSearcher;

  public ReverseRequestHandlerFactory(final ElasticsearchReverseSearcher elasticsearchSearcher) {
    this.elasticsearchSearcher = elasticsearchSearcher;
  }

  public <R extends ReverseRequest> ReverseRequestHandler<R> createHandler(final R request) {
    return (ReverseRequestHandler<R>) new SimpleReverseRequestHandler(elasticsearchSearcher);
  }
}

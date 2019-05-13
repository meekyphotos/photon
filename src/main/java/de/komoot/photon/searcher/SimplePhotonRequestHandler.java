package de.komoot.photon.searcher;

import de.komoot.photon.query.PhotonQueryBuilder;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.TagFilterQueryBuilder;

/**
 * Created by Sachin Dole on 2/12/2015.
 */
public class SimplePhotonRequestHandler extends AbstractPhotonRequestHandler<PhotonRequest> {

  public SimplePhotonRequestHandler(final ElasticsearchSearcher elasticsearchSearcher) {
    super(elasticsearchSearcher);
  }

  @Override
  public TagFilterQueryBuilder buildQuery(final PhotonRequest photonRequest) {
    return PhotonQueryBuilder.builder(photonRequest.getQuery(), photonRequest.getLanguage()).
        withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias());
  }
}

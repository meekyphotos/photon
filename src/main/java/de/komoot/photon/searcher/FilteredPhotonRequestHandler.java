package de.komoot.photon.searcher;

import de.komoot.photon.query.FilteredPhotonRequest;
import de.komoot.photon.query.PhotonQueryBuilder;
import de.komoot.photon.query.TagFilterQueryBuilder;

import java.util.Map;
import java.util.Set;

/**
 * Created by Sachin Dole on 2/20/2015.
 */
public class FilteredPhotonRequestHandler extends AbstractPhotonRequestHandler<FilteredPhotonRequest> {

  public FilteredPhotonRequestHandler(final ElasticsearchSearcher elasticsearchSearcher) {
    super(elasticsearchSearcher);
  }

  @Override
  public TagFilterQueryBuilder buildQuery(final FilteredPhotonRequest photonRequest) {
    final Map<String, Set<String>> includeTags = photonRequest.tags();
    final Set<String> includeKeys = photonRequest.keys();
    final Set<String> includeValues = photonRequest.values();
    final Map<String, Set<String>> excludeTags = photonRequest.notTags();
    final Set<String> excludeKeys = photonRequest.notKeys();
    final Set<String> excludeValues = photonRequest.notValues();
    final Map<String, Set<String>> excludeTagValues = photonRequest.tagNotValues();

    return PhotonQueryBuilder.
        builder(photonRequest.getQuery(), photonRequest.getLanguage()).
        withTags(includeTags).
        withKeys(includeKeys).
        withValues(includeValues).
        withoutTags(excludeTags).
        withoutKeys(excludeKeys).
        withoutValues(excludeValues).
        withTagsNotValues(excludeTagValues).
        withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias());
  }

}

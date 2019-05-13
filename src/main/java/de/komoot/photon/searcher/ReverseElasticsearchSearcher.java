package de.komoot.photon.searcher;

import com.vividsolutions.jts.geom.Point;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author svantulden
 */
public class ReverseElasticsearchSearcher implements ElasticsearchReverseSearcher {

  private final Client client;

  public ReverseElasticsearchSearcher(final Client client) {
    this.client = client;
  }

  @Override
  public SearchResponse search(final QueryBuilder queryBuilder, final Integer limit, final Point location,
                               final Boolean locationDistanceSort) {
    final TimeValue timeout = TimeValue.timeValueSeconds(7L);

    final SearchRequestBuilder builder = client
        .prepareSearch("photon")
        .setSearchType(SearchType.DEFAULT)
        .setQuery(queryBuilder)
        .setSize(limit)
        .setTimeout(timeout);

    if (locationDistanceSort) {
      builder.addSort(SortBuilders.geoDistanceSort("coordinate", new GeoPoint(location.getY(), location.getX()))
                          .order(SortOrder.ASC));
    }

    return builder.execute().actionGet();
  }
}

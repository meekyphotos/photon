package de.komoot.photon.searcher;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by Sachin Dole on 2/12/2015.
 */
public class BaseElasticsearchSearcher implements ElasticsearchSearcher {

  private final Client client;

  public BaseElasticsearchSearcher(final Client client) {
    this.client = client;
  }

  @Override
  public SearchResponse search(final QueryBuilder queryBuilder, final Integer limit) {
    final TimeValue timeout = TimeValue.timeValueSeconds(7);
    return client.prepareSearch("photon").
        setSearchType(SearchType.DEFAULT).
        setQuery(queryBuilder).
        setSize(limit).
        setTimeout(timeout).
        execute().
        actionGet();

  }

}

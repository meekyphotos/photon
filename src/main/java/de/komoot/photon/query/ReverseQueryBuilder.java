package de.komoot.photon.query;

import com.google.common.collect.ImmutableSet;

import com.vividsolutions.jts.geom.Point;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;
import java.util.Set;

/**
 * @author svantulden
 */
public class ReverseQueryBuilder implements TagFilterQueryBuilder {

  private Integer limit;

  private final Double radius;

  private final Point location;

  private final String queryStringFilter;

  private ReverseQueryBuilder(final Point location, final Double radius, final String queryStringFilter) {
    this.location = location;
    this.radius = radius;
    this.queryStringFilter = queryStringFilter;
  }

  public static TagFilterQueryBuilder builder(final Point location, final Double radius, final String queryStringFilter) {
    return new ReverseQueryBuilder(location, radius, queryStringFilter);
  }

  @Override
  public TagFilterQueryBuilder withLimit(final Integer limit) {
    this.limit = limit == null || limit < 0 ? 0 : limit;
    this.limit = this.limit > 5000 ? 5000 : this.limit;

    return this;
  }

  @Override
  public TagFilterQueryBuilder withLocationBias(final Point point, final double scale) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withTags(final Map<String, Set<String>> tags) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withKeys(final Set<String> keys) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withValues(final Set<String> values) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withTagsNotValues(final Map<String, Set<String>> tags) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withoutTags(final Map<String, Set<String>> tagsToExclude) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withoutKeys(final Set<String> keysToExclude) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withoutValues(final Set<String> valuesToExclude) {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withKeys(final String... keys) {
    return this.withKeys(ImmutableSet.<String>builder().add(keys).build());
  }

  @Override
  public TagFilterQueryBuilder withValues(final String... values) {
    return this.withValues(ImmutableSet.<String>builder().add(values).build());
  }

  @Override
  public TagFilterQueryBuilder withoutKeys(final String... keysToExclude) {
    return this.withoutKeys(ImmutableSet.<String>builder().add(keysToExclude).build());
  }

  @Override
  public TagFilterQueryBuilder withoutValues(final String... valuesToExclude) {
    return this.withoutValues(ImmutableSet.<String>builder().add(valuesToExclude).build());
  }

  @Override
  public TagFilterQueryBuilder withStrictMatch() {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public TagFilterQueryBuilder withLenientMatch() {
    throw new RuntimeException(new NoSuchMethodException("this method is not implemented (NOOP)"));
  }

  @Override
  public QueryBuilder buildQuery() {
    final QueryBuilder fb = QueryBuilders.geoDistanceQuery("coordinate").point(location.getY(), location.getX())
        .distance(radius, DistanceUnit.KILOMETERS);

    final BoolQueryBuilder finalQuery;

    if (queryStringFilter != null && queryStringFilter.trim().length() > 0) {
      finalQuery = QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery(queryStringFilter)).filter(fb);
    } else {
      finalQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(fb);
    }

    return finalQuery;
  }

  @Override
  public Integer getLimit() {
    return limit;
  }

  private Boolean checkTags(final Set<String> keys) {
    return !(keys == null || keys.isEmpty());
  }

  private Boolean checkTags(final Map<String, Set<String>> tags) {
    return !(tags == null || tags.isEmpty());
  }

  private enum State {
    PLAIN, FILTERED, QUERY_ALREADY_BUILT, FINISHED,
  }
}

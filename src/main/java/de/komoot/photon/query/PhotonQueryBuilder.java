package de.komoot.photon.query;


import com.google.common.collect.ImmutableSet;

import com.vividsolutions.jts.geom.Point;

import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;


/**
 * There are four {@link PhotonQueryBuilder.State states} that this query builder goes through before a query can be executed on elastic search. Of
 * these, three are of importance.
 * <ul>
 * <li>{@link PhotonQueryBuilder.State#PLAIN PLAIN} The query builder is being used to build a query without any tag filters.</li>
 * <li>{@link PhotonQueryBuilder.State#FILTERED FILTERED} The query builder is being used to build a query that has tag filters and can no longer
 * be used to build a PLAIN filter.</li>
 * <li>{@link PhotonQueryBuilder.State#FINISHED FINISHED} The query builder has been built and the query has been placed inside a
 * {@link QueryBuilder filtered query}. Further calls to any methods will have no effect on this query builder.</li>
 * </ul>
 * <p/>
 * Created by Sachin Dole on 2/12/2015.
 *
 * @see TagFilterQueryBuilder
 */
public class PhotonQueryBuilder implements TagFilterQueryBuilder {

  protected ArrayList<FilterFunctionBuilder> m_alFilterFunction4QueryBuilder = new ArrayList<>(1);
  protected QueryBuilder m_query4QueryBuilder;
  private FunctionScoreQueryBuilder m_finalQueryWithoutTagFilterBuilder;
  private Integer limit = 50;
  private final BoolQueryBuilder m_queryBuilderForTopLevelFilter;
  private State state;
  private BoolQueryBuilder orQueryBuilderForIncludeTagFiltering;
  private BoolQueryBuilder andQueryBuilderForExcludeTagFiltering;
  private final MatchQueryBuilder defaultMatchQueryBuilder;
  private final MatchQueryBuilder languageMatchQueryBuilder;
  private QueryBuilder m_finalQueryBuilder;


  private PhotonQueryBuilder(final String query, final String language) {
    defaultMatchQueryBuilder =
        QueryBuilders.matchQuery("collector.default", query).fuzziness(Fuzziness.ZERO).prefixLength(2).analyzer("search_ngram").minimumShouldMatch("100%");

    languageMatchQueryBuilder = QueryBuilders.matchQuery(String.format("collector.%s.ngrams", language), query).fuzziness(Fuzziness.ZERO).prefixLength(2)
        .analyzer("search_ngram").minimumShouldMatch("100%");

    // @formatter:off
    m_query4QueryBuilder = QueryBuilders.boolQuery()
        .must(QueryBuilders.boolQuery().should(defaultMatchQueryBuilder).should(languageMatchQueryBuilder)
                  .minimumShouldMatch("1"))
        .should(QueryBuilders.matchQuery(String.format("name.%s.raw", language), query).boost(200)
                    .analyzer("search_raw"))
        .should(QueryBuilders.matchQuery(String.format("collector.%s.raw", language), query).boost(100)
                    .analyzer("search_raw"));
    // @formatter:on

    // this is former general-score, now inline
    final String strCode = "double score = 1 + doc['importance'].value * 100; score";
    final ScriptScoreFunctionBuilder functionBuilder4QueryBuilder =
        ScoreFunctionBuilders.scriptFunction(new Script(ScriptType.INLINE, "painless", strCode, new HashMap<String, Object>()));

    m_alFilterFunction4QueryBuilder.add(new FilterFunctionBuilder(functionBuilder4QueryBuilder));

    m_finalQueryWithoutTagFilterBuilder = new FunctionScoreQueryBuilder(m_query4QueryBuilder, m_alFilterFunction4QueryBuilder.toArray(new FilterFunctionBuilder[0]))
        .boostMode(CombineFunction.MULTIPLY).scoreMode(FunctionScoreQuery.ScoreMode.MULTIPLY);

    // @formatter:off
    m_queryBuilderForTopLevelFilter = QueryBuilders.boolQuery()
        .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("housenumber")))
        .should(QueryBuilders.matchQuery("housenumber", query).analyzer("standard"))
        .should(QueryBuilders.existsQuery(String.format("name.%s.raw", language)));
    // @formatter:on

    state = State.PLAIN;
  }


  /**
   * Create an instance of this builder which can then be embellished as needed.
   *
   * @param query the value for photon query parameter "q"
   * @return An initialized {@link TagFilterQueryBuilder photon query builder}.
   */
  public static TagFilterQueryBuilder builder(final String query, final String language) {
    return new PhotonQueryBuilder(query, language);
  }


  @Override
  public TagFilterQueryBuilder withLimit(final Integer limit) {
    this.limit = limit == null || limit < 1 ? 15 : limit;
    this.limit = this.limit > 50 ? 50 : this.limit;
    return this;
  }

  @Override
  public TagFilterQueryBuilder withLocationBias(final Point point, double scale) {
    if (point == null) {
      return this;
    }
    final Map<String, Object> params = newHashMap();
    params.put("lon", point.getX());
    params.put("lat", point.getY());

    scale = Math.abs(scale);
    final String strCode = "double dist = doc['coordinate'].planeDistance(params.lat, params.lon); " +
                           "double score = 0.1 + " + scale + " / (1.0 + dist * 0.001 / 10.0); " +
                           "score";
    final ScriptScoreFunctionBuilder builder = ScoreFunctionBuilders.scriptFunction(new Script(ScriptType.INLINE, "painless", strCode, params));
    m_alFilterFunction4QueryBuilder.add(new FilterFunctionBuilder(builder));
    m_finalQueryWithoutTagFilterBuilder =
        new FunctionScoreQueryBuilder(m_query4QueryBuilder, m_alFilterFunction4QueryBuilder.toArray(new FilterFunctionBuilder[0]))
            .boostMode(CombineFunction.MULTIPLY);
    return this;
  }

  @Override
  public TagFilterQueryBuilder withTags(final Map<String, Set<String>> tags) {
    if (!checkTags(tags)) {
      return this;
    }

    ensureFiltered();

    final List<BoolQueryBuilder> termQueries = new ArrayList<BoolQueryBuilder>(tags.size());
    for (final String tagKey : tags.keySet()) {
      final Set<String> valuesToInclude = tags.get(tagKey);
      final TermQueryBuilder keyQuery = QueryBuilders.termQuery("osm_key", tagKey);
      final TermsQueryBuilder valueQuery = QueryBuilders.termsQuery("osm_value", valuesToInclude.toArray(new String[valuesToInclude.size()]));
      final BoolQueryBuilder includeAndQuery = QueryBuilders.boolQuery().must(keyQuery).must(valueQuery);
      termQueries.add(includeAndQuery);
    }
    this.appendIncludeTermQueries(termQueries);
    return this;
  }


  @Override
  public TagFilterQueryBuilder withKeys(final Set<String> keys) {
    if (!checkTags(keys)) {
      return this;
    }

    ensureFiltered();

    final List<TermsQueryBuilder> termQueries = new ArrayList<TermsQueryBuilder>(keys.size());
    termQueries.add(QueryBuilders.termsQuery("osm_key", keys.toArray()));
    this.appendIncludeTermQueries(termQueries);
    return this;
  }


  @Override
  public TagFilterQueryBuilder withValues(final Set<String> values) {
    if (!checkTags(values)) {
      return this;
    }

    ensureFiltered();

    final List<TermsQueryBuilder> termQueries = new ArrayList<TermsQueryBuilder>(values.size());
    termQueries.add(QueryBuilders.termsQuery("osm_value", values.toArray(new String[values.size()])));
    this.appendIncludeTermQueries(termQueries);
    return this;
  }


  @Override
  public TagFilterQueryBuilder withTagsNotValues(final Map<String, Set<String>> tags) {
    if (!checkTags(tags)) {
      return this;
    }

    ensureFiltered();

    final List<BoolQueryBuilder> termQueries = new ArrayList<BoolQueryBuilder>(tags.size());
    for (final String tagKey : tags.keySet()) {
      final Set<String> valuesToInclude = tags.get(tagKey);
      final TermQueryBuilder keyQuery = QueryBuilders.termQuery("osm_key", tagKey);
      final TermsQueryBuilder valueQuery = QueryBuilders.termsQuery("osm_value", valuesToInclude.toArray(new String[valuesToInclude.size()]));

      final BoolQueryBuilder includeAndQuery = QueryBuilders.boolQuery().must(keyQuery).mustNot(valueQuery);

      termQueries.add(includeAndQuery);
    }
    this.appendIncludeTermQueries(termQueries);
    return this;
  }


  @Override
  public TagFilterQueryBuilder withoutTags(final Map<String, Set<String>> tagsToExclude) {
    if (!checkTags(tagsToExclude)) {
      return this;
    }

    ensureFiltered();

    final List<QueryBuilder> termQueries = new ArrayList<>(tagsToExclude.size());
    for (final String tagKey : tagsToExclude.keySet()) {
      final Set<String> valuesToExclude = tagsToExclude.get(tagKey);
      final TermQueryBuilder keyQuery = QueryBuilders.termQuery("osm_key", tagKey);
      final TermsQueryBuilder valueQuery = QueryBuilders.termsQuery("osm_value", valuesToExclude.toArray(new String[valuesToExclude.size()]));

      final BoolQueryBuilder withoutTagsQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.boolQuery().must(keyQuery).must(valueQuery));

      termQueries.add(withoutTagsQuery);
    }

    this.appendExcludeTermQueries(termQueries);

    return this;
  }


  @Override
  public TagFilterQueryBuilder withoutKeys(final Set<String> keysToExclude) {
    if (!checkTags(keysToExclude)) {
      return this;
    }

    ensureFiltered();

    final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("osm_key", keysToExclude.toArray()));

    final LinkedList<QueryBuilder> lList = new LinkedList<>();
    lList.add(boolQuery);
    this.appendExcludeTermQueries(lList);

    return this;
  }


  @Override
  public TagFilterQueryBuilder withoutValues(final Set<String> valuesToExclude) {
    if (!checkTags(valuesToExclude)) {
      return this;
    }

    ensureFiltered();

    final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("osm_value", valuesToExclude.toArray()));

    final LinkedList<QueryBuilder> lList = new LinkedList<>();
    lList.add(boolQuery);
    this.appendExcludeTermQueries(lList);

    return this;
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
    defaultMatchQueryBuilder.minimumShouldMatch("100%");
    languageMatchQueryBuilder.minimumShouldMatch("100%");
    return this;
  }


  @Override
  public TagFilterQueryBuilder withLenientMatch() {
    defaultMatchQueryBuilder.fuzziness(Fuzziness.ONE).minimumShouldMatch("-1");
    languageMatchQueryBuilder.fuzziness(Fuzziness.ONE).minimumShouldMatch("-1");
    return this;
  }


  /**
   * When this method is called, all filters are placed inside their OR and AND containers and the top level filter
   * builder is built. Subsequent invocations of this method have no additional effect. Note that after this method is called, calling other methods on this class also
   * have no effect.
   *
   * @see TagFilterQueryBuilder#buildQuery()
   */
  @Override
  public QueryBuilder buildQuery() {
    if (state.equals(State.FINISHED)) {
      return m_finalQueryBuilder;
    }

    if (state.equals(State.FILTERED)) {

      if (orQueryBuilderForIncludeTagFiltering != null) {
        m_queryBuilderForTopLevelFilter.must(orQueryBuilderForIncludeTagFiltering);
      }
      if (andQueryBuilderForExcludeTagFiltering != null) {
        m_queryBuilderForTopLevelFilter.must(andQueryBuilderForExcludeTagFiltering);
      }

    }

    state = State.FINISHED;

    m_finalQueryBuilder = QueryBuilders.boolQuery().must(m_finalQueryWithoutTagFilterBuilder).filter(m_queryBuilderForTopLevelFilter);

    return m_finalQueryBuilder;
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


  private void appendIncludeTermQueries(final List<? extends QueryBuilder> termQueries) {

    if (orQueryBuilderForIncludeTagFiltering == null) {
      orQueryBuilderForIncludeTagFiltering = QueryBuilders.boolQuery();
    }

    for (final QueryBuilder eachTagFilter : termQueries) {
      orQueryBuilderForIncludeTagFiltering.should(eachTagFilter);
    }
  }


  private void appendExcludeTermQueries(final List<QueryBuilder> termQueries) {

    if (andQueryBuilderForExcludeTagFiltering == null) {
      andQueryBuilderForExcludeTagFiltering = QueryBuilders.boolQuery();
    }

    for (final QueryBuilder eachTagFilter : termQueries) {
      andQueryBuilderForExcludeTagFiltering.must(eachTagFilter);
    }
  }


  private void ensureFiltered() {
    state = State.FILTERED;
  }


  private enum State {
    PLAIN, FILTERED, QUERY_ALREADY_BUILT, FINISHED,
  }
}

package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Point;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import spark.QueryParamsMap;
import spark.Request;

/**
 * A factory that creates a {@link PhotonRequest} from a {@link Request web request}
 * Created by Sachin Dole on 2/12/2015.
 */
public class PhotonRequestFactory {

  private static final LocationParamConverter optionalLocationParamConverter = new LocationParamConverter(false);
  protected static HashSet<String> m_hsRequestQueryParams = new HashSet<>(Arrays.asList("lang", "q", "lon", "lat",
                                                                                        "limit", "osm_tag", "location_bias_scale"));
  private final LanguageChecker languageChecker;

  public PhotonRequestFactory(final Set<String> supportedLanguages) {
    this.languageChecker = new LanguageChecker(supportedLanguages);
  }

  public <R extends PhotonRequest> R create(final Request webRequest) throws BadRequestException {

    for (final String queryParam : webRequest.queryParams()) {
      if (!m_hsRequestQueryParams.contains(queryParam)) {
        throw new BadRequestException(400, "unknown query parameter '" + queryParam + "'.  Allowed parameters are: " + m_hsRequestQueryParams);
      }
    }

    String language = webRequest.queryParams("lang");
    language = language == null ? "en" : language;
    languageChecker.apply(language);
    final String query = webRequest.queryParams("q");
    if (query == null) {
      throw new BadRequestException(400, "missing search term 'q': /?q=berlin");
    }
    Integer limit;
    try {
      limit = Integer.valueOf(webRequest.queryParams("limit"));
    } catch (final NumberFormatException e) {
      limit = 15;
    }
    final Point locationForBias = optionalLocationParamConverter.apply(webRequest);

    // don't use too high default value, see #306
    double scale = 1.6;
    final String scaleStr = webRequest.queryParams("location_bias_scale");
    if (scaleStr != null && !scaleStr.isEmpty()) {
      try {
        scale = Double.parseDouble(scaleStr);
      } catch (final Exception nfe) {
        throw new BadRequestException(400, "invalid parameter 'location_bias_scale' must be a number");
      }
    }

    final QueryParamsMap tagFiltersQueryMap = webRequest.queryMap("osm_tag");
    if (!new CheckIfFilteredRequest().execute(tagFiltersQueryMap)) {
      return (R) new PhotonRequest(query, limit, locationForBias, scale, language);
    }
    final FilteredPhotonRequest photonRequest = new FilteredPhotonRequest(query, limit, locationForBias, scale, language);
    final String[] tagFilters = tagFiltersQueryMap.values();
    setUpTagFilters(photonRequest, tagFilters);

    return (R) photonRequest;
  }

  private void setUpTagFilters(final FilteredPhotonRequest request, final String[] tagFilters) {
    for (final String tagFilter : tagFilters) {
      if (tagFilter.contains(":")) {
        //might be tag and value OR just value.
        if (tagFilter.startsWith("!")) {
          //exclude
          final String keyValueCandidate = tagFilter.substring(1);
          if (keyValueCandidate.startsWith(":")) {
            //just value
            request.notValues(keyValueCandidate.substring(1));
          } else {
            //key and value
            final String[] keyAndValue = keyValueCandidate.split(":");
            final String excludeKey = keyAndValue[0];
            final String value = keyAndValue[1].startsWith("!") ? keyAndValue[1].substring(1) : keyAndValue[1];
            Set<String> valuesToExclude = request.notTags().get(excludeKey);
            if (valuesToExclude == null) {
              valuesToExclude = new HashSet<String>();
            }
            valuesToExclude.add(value);
            request.notTags(excludeKey, valuesToExclude);
          }
        } else {
          //include key, not sure about value
          if (tagFilter.startsWith(":")) {
            //just value

            final String valueCandidate = tagFilter.substring(1);
            if (valueCandidate.startsWith("!")) {
              //exclude value
              request.notValues(valueCandidate.substring(1));
            } else {
              //include value
              request.values(valueCandidate);
            }
          } else {
            //key and value
            final String[] keyAndValue = tagFilter.split(":");

            final String key = keyAndValue[0];
            final String value = keyAndValue[1];
            if (value.startsWith("!")) {
              //exclude value
              Set<String> tagKeysValuesNotIncluded = request.tagNotValues().get(key);
              if (tagKeysValuesNotIncluded == null) {
                tagKeysValuesNotIncluded = new HashSet<String>();
              }
              tagKeysValuesNotIncluded.add(value.substring(1));
              request.tagNotValues(key, tagKeysValuesNotIncluded);
            } else {
              //include value
              Set<String> valuesToInclude = request.tags().get(key);
              if (valuesToInclude == null) {
                valuesToInclude = new HashSet<String>();
              }
              valuesToInclude.add(value);
              request.tags(key, valuesToInclude);
            }
          }
        }
      } else {
        //only tag
        if (tagFilter.startsWith("!")) {
          request.notKeys(tagFilter.substring(1));
        } else {
          request.keys(tagFilter);
        }
      }
    }
  }
}

package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Point;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import spark.Request;

/**
 * @author svantulden
 */
public class ReverseRequestFactory {

  private static final LocationParamConverter mandatoryLocationParamConverter = new LocationParamConverter(true);
  protected static HashSet<String> m_hsRequestQueryParams = new HashSet<>(Arrays.asList("lang", "lon", "lat", "radius", "query_string_filter", "distance_sort", "limit"));
  private final LanguageChecker languageChecker;

  public ReverseRequestFactory(final Set<String> supportedLanguages) {
    this.languageChecker = new LanguageChecker(supportedLanguages);
  }

  public <R extends ReverseRequest> R create(final Request webRequest) throws BadRequestException {
    for (final String queryParam : webRequest.queryParams()) {
      if (!m_hsRequestQueryParams.contains(queryParam)) {
        throw new BadRequestException(400, "unknown query parameter '" + queryParam + "'.  Allowed parameters are: " + m_hsRequestQueryParams);
      }
    }

    String language = webRequest.queryParams("lang");
    language = language == null ? "en" : language;
    languageChecker.apply(language);

    final Point location = mandatoryLocationParamConverter.apply(webRequest);

    Double radius = 1d;
    final String radiusParam = webRequest.queryParams("radius");
    if (radiusParam != null) {
      try {
        radius = Double.valueOf(radiusParam);
      } catch (final Exception nfe) {
        throw new BadRequestException(400, "invalid search term 'radius', expected a number.");
      }
      if (radius <= 0) {
        throw new BadRequestException(400, "invalid search term 'radius', expected a strictly positive number.");
      } else {
        // limit search radius to 5000km
        radius = Math.min(radius, 5000d);
      }
    }

    final Boolean locationDistanceSort;
    try {
      locationDistanceSort = Boolean.valueOf(webRequest.queryParamOrDefault("distance_sort", "true"));
    } catch (final Exception nfe) {
      throw new BadRequestException(400, "invalid parameter 'distance_sort', can only be true or false");
    }

    Integer limit = 1;
    final String limitParam = webRequest.queryParams("limit");
    if (limitParam != null) {
      try {
        limit = Integer.valueOf(limitParam);
      } catch (final Exception nfe) {
        throw new BadRequestException(400, "invalid search term 'limit', expected an integer.");
      }
      if (limit <= 0) {
        throw new BadRequestException(400, "invalid search term 'limit', expected a strictly positive integer.");
      } else {
        // limit number of results to 50
        limit = Math.min(limit, 50);
      }
    }

    final String queryStringFilter = webRequest.queryParams("query_string_filter");
    final ReverseRequest reverseRequest = new ReverseRequest(location, language, radius, queryStringFilter, limit, locationDistanceSort);
    return (R) reverseRequest;
  }
}

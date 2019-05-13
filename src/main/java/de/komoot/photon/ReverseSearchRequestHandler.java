package de.komoot.photon;

import de.komoot.photon.query.BadRequestException;
import de.komoot.photon.query.ReverseRequest;
import de.komoot.photon.query.ReverseRequestFactory;
import de.komoot.photon.searcher.ReverseElasticsearchSearcher;
import de.komoot.photon.searcher.ReverseRequestHandler;
import de.komoot.photon.searcher.ReverseRequestHandlerFactory;
import de.komoot.photon.utils.ConvertToGeoJson;

import org.elasticsearch.client.Client;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import spark.Request;
import spark.Response;
import spark.RouteImpl;

import static spark.Spark.halt;

/**
 * @author svantulden
 */
public class ReverseSearchRequestHandler<R extends ReverseRequest> extends RouteImpl {

  private final ReverseRequestFactory reverseRequestFactory;
  private final ReverseRequestHandlerFactory requestHandlerFactory;
  private final ConvertToGeoJson geoJsonConverter;

  ReverseSearchRequestHandler(final String path, final Client esNodeClient, final String languages) {
    super(path);
    final Set<String> supportedLanguages = new HashSet<String>(Arrays.asList(languages.split(",")));
    this.reverseRequestFactory = new ReverseRequestFactory(supportedLanguages);
    this.geoJsonConverter = new ConvertToGeoJson();
    this.requestHandlerFactory = new ReverseRequestHandlerFactory(new ReverseElasticsearchSearcher(esNodeClient));
  }

  @Override
  public String handle(final Request request, final Response response) {
    R photonRequest = null;
    try {
      photonRequest = reverseRequestFactory.create(request);
    } catch (final BadRequestException e) {
      final JSONObject json = new JSONObject();
      json.put("message", e.getMessage());
      halt(e.getHttpStatus(), json.toString());
    }
    final ReverseRequestHandler<R> handler = requestHandlerFactory.createHandler(photonRequest);
    final List<JSONObject> results = handler.handle(photonRequest);
    final JSONObject geoJsonResults = geoJsonConverter.convert(results);
    if (request.queryParams("debug") != null) {
      return geoJsonResults.toString(4);
    }

    return geoJsonResults.toString();
  }
}

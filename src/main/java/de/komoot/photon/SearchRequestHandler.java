package de.komoot.photon;

import de.komoot.photon.query.BadRequestException;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.query.PhotonRequestFactory;
import de.komoot.photon.searcher.BaseElasticsearchSearcher;
import de.komoot.photon.searcher.PhotonRequestHandler;
import de.komoot.photon.searcher.PhotonRequestHandlerFactory;
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
 * Created by Sachin Dole on 2/12/2015.
 */
public class SearchRequestHandler<R extends PhotonRequest> extends RouteImpl {

  private final PhotonRequestFactory photonRequestFactory;
  private final PhotonRequestHandlerFactory requestHandlerFactory;
  private final ConvertToGeoJson geoJsonConverter;

  SearchRequestHandler(final String path, final Client esNodeClient, final String languages) {
    super(path);
    final Set<String> supportedLanguages = new HashSet<String>(Arrays.asList(languages.split(",")));
    this.photonRequestFactory = new PhotonRequestFactory(supportedLanguages);
    this.geoJsonConverter = new ConvertToGeoJson();
    this.requestHandlerFactory = new PhotonRequestHandlerFactory(new BaseElasticsearchSearcher(esNodeClient));
  }

  @Override
  public String handle(final Request request, final Response response) {
    R photonRequest = null;
    try {
      photonRequest = photonRequestFactory.create(request);
    } catch (final BadRequestException e) {
      final JSONObject json = new JSONObject();
      json.put("message", e.getMessage());
      halt(e.getHttpStatus(), json.toString());
    }
    final PhotonRequestHandler<R> handler = requestHandlerFactory.createHandler(photonRequest);
    final List<JSONObject> results = handler.handle(photonRequest);
    final JSONObject geoJsonResults = geoJsonConverter.convert(results);
    if (request.queryParams("debug") != null) {
      return geoJsonResults.toString(4);
    }

    return geoJsonResults.toString();
  }
}

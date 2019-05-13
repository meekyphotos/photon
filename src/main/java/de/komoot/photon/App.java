package de.komoot.photon;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import de.komoot.photon.elasticsearch.Server;
import de.komoot.photon.nominatim.NominatimConnector;
import de.komoot.photon.nominatim.NominatimUpdater;
import de.komoot.photon.utils.CorsFilter;

import org.elasticsearch.client.Client;

import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import spark.Request;
import spark.Response;

import static spark.Spark.before;
import static spark.Spark.get;
import static spark.Spark.ipAddress;
import static spark.Spark.port;


@Slf4j
public class App {

  public static void main(final String[] rawArgs) throws Exception {
    // parse command line arguments
    final CommandLineArgs args = new CommandLineArgs();
    final JCommander jCommander = new JCommander(args);
    try {
      jCommander.parse(rawArgs);
      if (args.isCorsAnyOrigin() && args.getCorsOrigin() != null) { // these are mutually exclusive
        throw new ParameterException("Use only one cors configuration type");
      }
    } catch (final ParameterException e) {
      log.warn("could not start photon: " + e.getMessage());
      jCommander.usage();
      return;
    }

    // show help
    if (args.isUsage()) {
      jCommander.usage();
      return;
    }

    if (args.getJsonDump() != null) {
      startJsonDump(args);
      return;
    }

    boolean shutdownES = false;
    final Server esServer = new Server(args).start();
    try {
      final Client esClient = esServer.getClient();

      if (args.isRecreateIndex()) {
        shutdownES = true;
        startRecreatingIndex(esServer);
        return;
      }

      if (args.isNominatimImport()) {
        shutdownES = true;
        startNominatimImport(args, esServer, esClient);
        return;
      }

      // no special action specified -> normal mode: start search API
      startApi(args, esClient);
    } finally {
      if (shutdownES) {
        esServer.shutdown();
      }
    }
  }


  /**
   * dump elastic search index and create a new and empty one
   */
  private static void startRecreatingIndex(final Server esServer) {
    try {
      esServer.recreateIndex();
    } catch (final IOException e) {
      throw new RuntimeException("cannot setup index, elastic search config files not readable", e);
    }

    log.info("deleted photon index and created an empty new one.");
  }


  /**
   * take nominatim data and dump it to json
   */
  private static void startJsonDump(final CommandLineArgs args) {
    try {
      final String filename = args.getJsonDump();
      final JsonDumper jsonDumper = new JsonDumper(filename, args.getLanguages());
      final NominatimConnector nominatimConnector = new NominatimConnector(args.getHost(), args.getPort(), args.getDatabase(), args.getUser(), args.getPassword());
      nominatimConnector.setImporter(jsonDumper);
      nominatimConnector.readEntireDatabase(args.getCountryCodes().split(","));
      log.info("json dump was created: " + filename);
    } catch (final FileNotFoundException e) {
      log.error("cannot create dump", e);
    }
  }


  /**
   * take nominatim data to fill elastic search index
   */
  private static void startNominatimImport(final CommandLineArgs args, final Server esServer, final Client esNodeClient) {
    try {
      esServer.recreateIndex(); // dump previous data
    } catch (final IOException e) {
      throw new RuntimeException("cannot setup index, elastic search config files not readable", e);
    }

    log.info("starting import from nominatim to photon with languages: " + args.getLanguages());
    final de.komoot.photon.elasticsearch.Importer importer = new de.komoot.photon.elasticsearch.Importer(esNodeClient, args.getLanguages());
    final NominatimConnector nominatimConnector = new NominatimConnector(args.getHost(), args.getPort(), args.getDatabase(), args.getUser(), args.getPassword());
    nominatimConnector.setImporter(importer);
    nominatimConnector.readEntireDatabase(args.getCountryCodes().split(","));

    log.info("imported data from nominatim to photon with languages: " + args.getLanguages());
  }


  /**
   * start api to accept search requests via http
   */
  private static void startApi(final CommandLineArgs args, final Client esNodeClient) {
    port(args.getListenPort());
    ipAddress(args.getListenIp());

    final String allowedOrigin = args.isCorsAnyOrigin() ? "*" : args.getCorsOrigin();
    if (allowedOrigin != null) {
      CorsFilter.enableCORS(allowedOrigin, "get", "*");
    } else {
      before((request, response) -> {
        response.type("application/json"); // in the other case set by enableCors
      });
    }

    // setup search API
    get("api", new SearchRequestHandler("api", esNodeClient, args.getLanguages()));
    get("api/", new SearchRequestHandler("api/", esNodeClient, args.getLanguages()));
    get("reverse", new ReverseSearchRequestHandler("reverse", esNodeClient, args.getLanguages()));
    get("reverse/", new ReverseSearchRequestHandler("reverse/", esNodeClient, args.getLanguages()));

    // setup update API
    final NominatimUpdater nominatimUpdater = new NominatimUpdater(args.getHost(), args.getPort(), args.getDatabase(), args.getUser(), args.getPassword());
    final Updater updater = new de.komoot.photon.elasticsearch.Updater(esNodeClient, args.getLanguages());
    nominatimUpdater.setUpdater(updater);

    get("/nominatim-update", (Request request, Response response) -> {
      new Thread(() -> nominatimUpdater.update()).start();
      return "nominatim update started (more information in console output) ...";
    });
  }
}

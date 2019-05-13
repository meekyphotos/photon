package de.komoot.photon.elasticsearch;

import de.komoot.photon.CommandLineArgs;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * Helper class to start/stop elasticsearch node and get elasticsearch clients
 *
 * @author felix
 */
@Slf4j
public class Server {

  private final String[] languages;
  private Node esNode;
  private Client esClient;
  private final String clusterName;
  private File esDirectory;
  private final String transportAddresses;

  public Server(final CommandLineArgs args) {
    this(args.getCluster(), args.getDataDirectory(), args.getLanguages(), args.getTransportAddresses());
  }

  public Server(final String clusterName, final String mainDirectory, final String languages, final String transportAddresses) {
    try {
      if (SystemUtils.IS_OS_WINDOWS) {
        setupDirectories(new URL("file:///" + mainDirectory));
      } else {
        setupDirectories(new URL("file://" + mainDirectory));
      }
    } catch (final Exception e) {
      throw new RuntimeException("Can't create directories: ", e);
    }
    this.clusterName = clusterName;
    this.languages = languages.split(",");
    this.transportAddresses = transportAddresses;
  }

  public Server start() {
    final Settings.Builder sBuilder = Settings.builder();
    sBuilder.put("path.home", this.esDirectory.toString());
    sBuilder.put("network.host", "127.0.0.1"); // http://stackoverflow.com/a/15509589/1245622
    sBuilder.put("cluster.name", clusterName);

    if (transportAddresses != null && !transportAddresses.isEmpty()) {
      final TransportClient trClient = new PreBuiltTransportClient(sBuilder.build());
      final List<String> addresses = Arrays.asList(transportAddresses.split(","));
      for (final String tAddr : addresses) {
        final int index = tAddr.indexOf(":");
        if (index >= 0) {
          final int port = Integer.parseInt(tAddr.substring(index + 1));
          final String addrStr = tAddr.substring(0, index);
          trClient.addTransportAddress(new TransportAddress(new InetSocketAddress(addrStr, port)));
        } else {
          trClient.addTransportAddress(new TransportAddress(new InetSocketAddress(tAddr, 9300)));
        }
      }

      esClient = trClient;

      log.info("started elastic search client connected to " + addresses);

    } else {

      try {

        sBuilder.put("transport.type", "netty4").put("http.type", "netty4").put("http.enabled", "true");
        final Settings settings = sBuilder.build();
        final Collection<Class<? extends Plugin>> lList = new LinkedList<>();
        lList.add(Netty4Plugin.class);
        esNode = new MyNode(settings, lList);
        esNode.start();

        log.info("started elastic search node");

        esClient = esNode.client();

      } catch (final NodeValidationException e) {
        throw new RuntimeException("Error while starting elasticsearch server", e);
      }

    }
    return this;
  }

  /**
   * stops the elasticsearch node
   */
  public void shutdown() {
    try {
      if (esNode != null) {
        esNode.close();
      }

      esClient.close();
    } catch (final IOException e) {
      throw new RuntimeException("Error during elasticsearch server shutdown", e);
    }
  }

  /**
   * returns an elasticsearch client
   */
  public Client getClient() {
    return esClient;
  }

  private void setupDirectories(final URL directoryName) throws IOException, URISyntaxException {
    final File mainDirectory = new File(directoryName.toURI());
    final File photonDirectory = new File(mainDirectory, "photon_data");
    this.esDirectory = new File(photonDirectory, "elasticsearch");
    final File pluginDirectory = new File(esDirectory, "plugins");
    final File scriptsDirectory = new File(esDirectory, "config/scripts");
    final File painlessDirectory = new File(esDirectory, "modules/lang-painless");

    for (final File directory : new File[] {photonDirectory, esDirectory, pluginDirectory, scriptsDirectory,
                                            painlessDirectory}) {
      directory.mkdirs();
    }

    // copy script directory to elastic search directory
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();

    Files.copy(loader.getResourceAsStream("modules/lang-painless/antlr4-runtime.jar"),
               new File(painlessDirectory, "antlr4-runtime.jar").toPath(),
               StandardCopyOption.REPLACE_EXISTING);
    Files.copy(loader.getResourceAsStream("modules/lang-painless/asm-debug-all.jar"),
               new File(painlessDirectory, "asm-debug-all.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(loader.getResourceAsStream("modules/lang-painless/lang-painless.jar"),
               new File(painlessDirectory, "lang-painless.jar").toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(loader.getResourceAsStream("modules/lang-painless/plugin-descriptor.properties"),
               new File(painlessDirectory, "plugin-descriptor.properties").toPath(),
               StandardCopyOption.REPLACE_EXISTING);
    Files.copy(loader.getResourceAsStream("modules/lang-painless/plugin-security.policy"),
               new File(painlessDirectory, "plugin-security.policy").toPath(), StandardCopyOption.REPLACE_EXISTING);

  }

  public void recreateIndex() throws IOException {
    deleteIndex();

    final Client client = this.getClient();
    final InputStream mappings = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("mappings.json");
    final InputStream index_settings = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("index_settings.json");

    final String mappingsString = IOUtils.toString(mappings);
    JSONObject mappingsJSON = new JSONObject(mappingsString);
    final String indexSettingsString = IOUtils.toString(index_settings);
    final JSONObject indexJson = new JSONObject(indexSettingsString);

    // add all langs to the mapping
    mappingsJSON = addLangsToMapping(mappingsJSON);
    client.admin().indices().prepareCreate("photon").setSettings(createMapFromJson(indexJson)).execute()
        .actionGet();
    client.admin().indices().preparePutMapping("photon").setType("place").setSource(mappingsJSON.toString())
        .execute().actionGet();
    log.info("mapping created: " + mappingsJSON.toString());
  }

  private static Map<String, Object> createMapFromJson(final JSONObject indexJson) {
    final Map<String, Object> sourceMap = new HashMap<>();
    for (final Object s : indexJson.keySet()) {
      sourceMap.put((String) s, indexJson.get((String) s));
    }
    return sourceMap;
  }

  public void deleteIndex() {
    try {
      this.getClient().admin().indices().prepareDelete("photon").execute().actionGet();
    } catch (final IndexNotFoundException e) {
      // ignore
    }
  }

  private JSONObject addLangsToMapping(final JSONObject mappingsObject) {
    // define collector json strings
    final String copyToCollectorString = "{\"type\":\"text\",\"index\":false,\"copy_to\":[\"collector.{lang}\"]}";
    final String nameToCollectorString = "{\"type\":\"text\",\"index\":false,\"fields\":{\"ngrams\":{\"type\":\"text\",\"analyzer\":\"index_ngram\"},\"raw\":{\"type\":\"text\",\"analyzer\":\"index_raw\"}},\"copy_to\":[\"collector.{lang}\"]}";
    final String collectorString = "{\"type\":\"text\",\"index\":false,\"fields\":{\"ngrams\":{\"type\":\"text\",\"analyzer\":\"index_ngram\"},\"raw\":{\"type\":\"text\",\"analyzer\":\"index_raw\"}},\"copy_to\":[\"collector.{lang}\"]}}},\"street\":{\"type\":\"object\",\"properties\":{\"default\":{\"text\":false,\"type\":\"text\",\"copy_to\":[\"collector.default\"]}";

    final JSONObject placeObject = mappingsObject.optJSONObject("place");
    JSONObject propertiesObject = placeObject == null ? null : placeObject.optJSONObject("properties");

    if (propertiesObject != null) {
      for (final String lang : languages) {
        // create lang-specific json objects
        final JSONObject copyToCollectorObject = new JSONObject(copyToCollectorString.replace("{lang}", lang));
        final JSONObject nameToCollectorObject = new JSONObject(nameToCollectorString.replace("{lang}", lang));
        final JSONObject collectorObject = new JSONObject(collectorString.replace("{lang}", lang));

        // add language specific tags to the collector
        propertiesObject = addToCollector("city", propertiesObject, copyToCollectorObject, lang);
        propertiesObject = addToCollector("context", propertiesObject, copyToCollectorObject, lang);
        propertiesObject = addToCollector("country", propertiesObject, copyToCollectorObject, lang);
        propertiesObject = addToCollector("state", propertiesObject, copyToCollectorObject, lang);
        propertiesObject = addToCollector("street", propertiesObject, copyToCollectorObject, lang);
        propertiesObject = addToCollector("name", propertiesObject, nameToCollectorObject, lang);

        // add language specific collector to default for name
        final JSONObject name = propertiesObject.optJSONObject("name");
        final JSONObject nameProperties = name == null ? null : name.optJSONObject("properties");
        if (nameProperties != null) {
          final JSONObject defaultObject = nameProperties.optJSONObject("default");
          final JSONArray copyToArray = defaultObject.optJSONArray("copy_to");
          copyToArray.put("name." + lang);

          defaultObject.put("copy_to", copyToArray);
          nameProperties.put("default", defaultObject);
          name.put("properties", nameProperties);
          propertiesObject.put("name", name);
        }

        // add language specific collector
        propertiesObject = addToCollector("collector", propertiesObject, collectorObject, lang);
      }
      placeObject.put("properties", propertiesObject);
      return mappingsObject.put("place", placeObject);
    }

    log.error("cannot add languages to mapping.json, please double-check the mappings.json or the language values supplied");
    return null;
  }

  private JSONObject addToCollector(final String key, final JSONObject properties, final JSONObject collectorObject, final String lang) {
    final JSONObject keyObject = properties.optJSONObject(key);
    final JSONObject keyProperties = keyObject == null ? null : keyObject.optJSONObject("properties");
    if (keyProperties != null) {
      keyProperties.put(lang, collectorObject);
      keyObject.put("properties", keyProperties);
      return properties.put(key, keyObject);
    }
    return properties;
  }

  protected static class MyNode extends Node {

    public MyNode(final Settings preparedSettings, final Collection<Class<? extends Plugin>> classpathPlugins) {
      super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null, null, () -> "default"), classpathPlugins, false);
    }
  }
}

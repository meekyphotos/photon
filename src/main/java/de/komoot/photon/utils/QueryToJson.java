package de.komoot.photon.utils;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by Sachin Dole on 2/28/2015.
 */
@Slf4j
public class QueryToJson implements OneWayConverter<QueryBuilder, String> {

  @Override
  public String convert(final QueryBuilder anItem) {
    try {
      return Strings.toString(anItem.toXContent(JsonXContent.contentBuilder(), new ToXContent.MapParams(null)));
    } catch (final IOException e) {
      log.error("Unable to transform querybuilder to a json string due to an exception", e);
      throw new RuntimeException("Unable to transform querybuilder to a json string due to an exception", e);
    }
  }
}

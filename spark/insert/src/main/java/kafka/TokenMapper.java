// Copyright (c) 2025 Skyflow, Inc.

package kafka;

import java.util.*;
import skyflow.types.V1InsertResponse;
import skyflow.types.V1RecordResponseObject;

public class TokenMapper {
  public static List<Map<String, Object>> extractKafkaEventsFromInsertResponse(
      V1InsertResponse response) {
    List<Map<String, Object>> events = new ArrayList<>();
    if (response.getRecords().isPresent()) {
      for (V1RecordResponseObject record : response.getRecords().get()) {
        if (record.getSkyflowId().isPresent()) {
          Map<String, Object> event = new HashMap<>();
          event.put("skyflowID", record.getSkyflowId().get());
          record
              .getTokens()
              .ifPresent(
                  tokensMap -> {
                    tokensMap.forEach(
                        (key, rawValue) -> {
                          List<?> rawList = (List<?>) rawValue;
                          if (!rawList.isEmpty() && rawList.get(0) instanceof Map) {
                            Object tokenObj = ((Map<?, ?>) rawList.get(0)).get("token");
                            if (tokenObj != null) event.put(key, tokenObj.toString());
                          }
                        });
                  });
          events.add(event);
        }
      }
    }
    return events;
  }
}

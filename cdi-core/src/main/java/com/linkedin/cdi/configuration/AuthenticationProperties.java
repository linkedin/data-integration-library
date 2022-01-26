// Copyright 2021 LinkedIn Corporation. All rights reserved.
// Licensed under the BSD-2 Clause license.
// See LICENSE in the project root for license information.

package com.linkedin.cdi.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.linkedin.cdi.util.SecretManager;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.cdi.configuration.PropertyCollection.*;
import static com.linkedin.cdi.configuration.StaticConstants.*;


public class AuthenticationProperties extends JsonObjectProperties {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationProperties.class);
  private final static List<String> methods = Lists.newArrayList("basic", "bearer", "oauth", "custom");
  private final static String BASIC_TOKEN_PREFIX = "Basic";
  private final static String BEARER_TOKEN_PREFIX = "Bearer";
  private final static String OAUTH_TOKEN_PREFIX = "OAuth";
  private final static String TOKEN_PREFIX_SEPARATOR = " ";
  private final static String HEADER_DEFAULT = "Authorization";

  /**
   * Constructor with implicit default value
   * @param config property name
   */
  AuthenticationProperties(String config) {
    super(config);
  }

  @Override
  public boolean isValid(State state) {
    if (super.isValid(state) && !super.isBlank(state)) {
      // avoid using the get() method of JsonObjectProperties as that will recursively call isValid()
      JsonObject auth = GSON.fromJson(state.getProp(getConfig()), JsonObject.class);
      if(!auth.has(KEY_WORD_METHOD) || !auth.has(KEY_WORD_ENCRYPTION)) {
        return false;
      }

      if (!auth.get(KEY_WORD_METHOD).isJsonPrimitive() || !auth.get(KEY_WORD_ENCRYPTION).isJsonPrimitive()) {
        return false;
      }

      String method = auth.get(KEY_WORD_METHOD).getAsString().toLowerCase();
      if (methods.stream().noneMatch(x -> x.equals(method))) {
        return false;
      }
    }
    return super.isValid(state);
  }

  /**
   * Get the authentication header attribute as a map
   *
   * Support:
   *   Basic Http Authentication
   *   Bearer token with Authorization header only, not including access_token in URI or Entity Body
   *
   * see Bearer token reference: https://tools.ietf.org/html/rfc6750
   *
   * @param state source state
   * @return header tag with proper encryption of tokens
   */
  public Map<String, String> getAsMap(State state) {
    JsonObject authentication = get(state);
    if (authentication.entrySet().size() == 0) {
      return new HashMap<>();
    }

    String authMethod = authentication.get(KEY_WORD_METHOD).getAsString();
    if (!authMethod.toLowerCase().matches("basic|bearer|oauth|custom")) {
      LOG.warn("Unsupported authentication type: " + authMethod);
      return new HashMap<>();
    }

    String token = "";
    if (authentication.has(KEY_WORD_TOKEN)) {
      token = SecretManager.getInstance(state).decrypt(authentication.get(KEY_WORD_TOKEN).getAsString());
    } else {
      String u = SecretManager.getInstance(state).decrypt(SOURCE_CONN_USERNAME.get(state));
      String p = SecretManager.getInstance(state).decrypt(SOURCE_CONN_PASSWORD.get(state));
      token = u + ":" + p;
    }

    if (authentication.get("encryption").getAsString().equalsIgnoreCase("base64")) {
      token = new String(Base64.encodeBase64((token).getBytes(StandardCharsets.UTF_8)),
          StandardCharsets.UTF_8);
    }

    String headerValue = authMethod.equalsIgnoreCase(BASIC_TOKEN_PREFIX)
        ? BASIC_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token
        : authMethod.equalsIgnoreCase(BEARER_TOKEN_PREFIX)
            ? BEARER_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token
            : authMethod.equalsIgnoreCase(OAUTH_TOKEN_PREFIX)
                ? OAUTH_TOKEN_PREFIX + TOKEN_PREFIX_SEPARATOR + token
                : token;

    String headerTag = authentication.has(KEY_WORD_HEADER)
        ? authentication.get(KEY_WORD_HEADER).getAsString() : HEADER_DEFAULT;

    return new ImmutableMap.Builder<String, String>().put(headerTag, headerValue).build();
  }
}

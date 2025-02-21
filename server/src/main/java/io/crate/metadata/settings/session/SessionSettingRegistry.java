/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.settings.session;

import static io.crate.Constants.DEFAULT_DATE_STYLE;
import static io.crate.metadata.SearchPath.createSearchPathFrom;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.common.collections.MapBuilder;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.settings.SessionSettings;
import io.crate.protocols.postgres.PostgresWireProtocol;
import io.crate.types.DataTypes;

@Singleton
public class SessionSettingRegistry {

    private static final String SEARCH_PATH_KEY = "search_path";
    public static final String HASH_JOIN_KEY = "enable_hashjoin";
    static final String MAX_INDEX_KEYS = "max_index_keys";
    private static final String SERVER_VERSION_NUM = "server_version_num";
    private static final String SERVER_VERSION = "server_version";
    static final String ERROR_ON_UNKNOWN_OBJECT_KEY = "error_on_unknown_object_key";
    static final String DATE_STYLE_KEY = "datestyle";
    static final SessionSetting<String> APPLICATION_NAME = new SessionSetting<String>(
        "application_name",
        inputs -> {},
        inputs -> DataTypes.STRING.implicitCast(inputs[0]),
        CoordinatorSessionSettings::setApplicationName,
        SessionSettings::applicationName,
        () -> null,
        "Optional application name. Can be set by a client to identify the application which created the connection",
        DataTypes.STRING
    );
    static final SessionSetting<String> DATE_STYLE = new SessionSetting<String>(
        "datestyle",
        inputs -> validateDateStyleFrom(objectsToStringArray(inputs)),
        inputs -> DEFAULT_DATE_STYLE,
        CoordinatorSessionSettings::setDateStyle,
        SessionSettings::dateStyle,
        () -> String.valueOf(DEFAULT_DATE_STYLE),
        "Display format for date and time values.",
        DataTypes.STRING
    );

    private final Map<String, SessionSetting<?>> settings;

    @Inject
    public SessionSettingRegistry(Set<SessionSettingProvider> sessionSettingProviders) {
        var builder = MapBuilder.<String, SessionSetting<?>>treeMapBuilder()
            .put(SEARCH_PATH_KEY,
                 new SessionSetting<>(
                     SEARCH_PATH_KEY,
                     objects -> {}, // everything allowed, empty list (resulting by ``SET .. TO DEFAULT`` results in defaults
                     objects -> createSearchPathFrom(objectsToStringArray(objects)),
                     CoordinatorSessionSettings::setSearchPath,
                     s -> String.join(", ", s.searchPath().showPath()),
                     () -> String.join(", ", SearchPath.pathWithPGCatalogAndDoc().showPath()),
                     "Sets the schema search order.",
                     DataTypes.STRING))
            .put(HASH_JOIN_KEY,
                 new SessionSetting<>(
                     HASH_JOIN_KEY,
                     objects -> {
                         if (objects.length != 1) {
                             throw new IllegalArgumentException(HASH_JOIN_KEY + " should have only one argument.");
                         }
                     },
                     objects -> DataTypes.BOOLEAN.implicitCast(objects[0]),
                     CoordinatorSessionSettings::setHashJoinEnabled,
                     s -> Boolean.toString(s.hashJoinsEnabled()),
                     () -> String.valueOf(true),
                     "Considers using the Hash Join instead of the Nested Loop Join implementation.",
                     DataTypes.BOOLEAN))
            .put(MAX_INDEX_KEYS,
                 new SessionSetting<>(
                     MAX_INDEX_KEYS,
                     objects -> {},
                     Function.identity(),
                     (s, v) -> {
                         throw new UnsupportedOperationException("\"" + MAX_INDEX_KEYS + "\" cannot be changed.");
                     },
                     s -> String.valueOf(32),
                     () -> String.valueOf(32),
                     "Shows the maximum number of index keys.",
                     DataTypes.INTEGER))
            .put(SERVER_VERSION_NUM,
                 new SessionSetting<>(
                     SERVER_VERSION_NUM,
                     objects -> {},
                     Function.identity(),
                     (s, v) -> {
                         throw new UnsupportedOperationException("\"" + SERVER_VERSION_NUM + "\" cannot be changed.");
                     },
                     s -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                     () -> String.valueOf(PostgresWireProtocol.SERVER_VERSION_NUM),
                     "Reports the emulated PostgreSQL version number",
                     DataTypes.INTEGER
                 )
            )
            .put(SERVER_VERSION,
                 new SessionSetting<>(
                     SERVER_VERSION,
                     objects -> {},
                     Function.identity(),
                     (s, v) -> {
                         throw new UnsupportedOperationException("\"" + SERVER_VERSION + "\" cannot be changed.");
                     },
                     s -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                     () -> String.valueOf(PostgresWireProtocol.PG_SERVER_VERSION),
                     "Reports the emulated PostgreSQL version number",
                     DataTypes.STRING
                 )
            )
            .put(ERROR_ON_UNKNOWN_OBJECT_KEY,
                 new SessionSetting<>(
                     ERROR_ON_UNKNOWN_OBJECT_KEY,
                     objects -> {
                         if (objects.length != 1) {
                             throw new IllegalArgumentException(ERROR_ON_UNKNOWN_OBJECT_KEY + " should have only one argument.");
                         }
                     },
                     objects -> DataTypes.BOOLEAN.implicitCast(objects[0]),
                     CoordinatorSessionSettings::setErrorOnUnknownObjectKey,
                     s -> Boolean.toString(s.errorOnUnknownObjectKey()),
                     () -> String.valueOf(true),
                     "Raises or suppresses ObjectKeyUnknownException when querying nonexistent keys to dynamic objects.",
                     DataTypes.BOOLEAN)
            )
            .put(APPLICATION_NAME.name(), APPLICATION_NAME)
            .put(DATE_STYLE.name(), DATE_STYLE);

        for (var providers : sessionSettingProviders) {
            for (var setting : providers.sessionSettings()) {
                builder.put(setting.name(), setting);
            }
        }
        this.settings = builder.immutableMap();
    }

    public Map<String, SessionSetting<?>> settings() {
        return settings;
    }

    private static String[] objectsToStringArray(Object[] objects) {
        ArrayList<String> argumentList = new ArrayList<>();
        for (int i = 0; i < objects.length; i++) {
            String str = DataTypes.STRING.implicitCast(objects[i]);
            for (String element : str.split(",")) {
                argumentList.add(element.trim());
            }
        }
        return argumentList.toArray(String[]::new);
    }

    private static void validateDateStyleFrom(String... strings) {
        String dateStyle;
        for (String s : strings) {
            dateStyle = s.toUpperCase(Locale.ENGLISH);
            switch (dateStyle) {
                // date format style
                case "ISO":
                    break;
                case "SQL":
                case "POSTGRES":
                case "GERMAN":
                    throw new IllegalArgumentException("Invalid value for parameter \"datestyle\": \"" + dateStyle + "\". Valid values include: [\"ISO\"].");
                // date order style
                case "MDY":
                case "NONEURO":
                case "NONEUROPEAN":
                case "US":
                case "DMY":
                case "EURO":
                case "EUROPEAN":
                case "YMD":
                    break;
                default:
                    throw new IllegalArgumentException("Invalid value for parameter \"datestyle\": \"" + dateStyle + "\". Valid values include: [\"ISO\"].");
            }
        }
    }
}

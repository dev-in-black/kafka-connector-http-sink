package com.devinblack.kafka.connect.http;

import java.io.InputStream;
import java.util.Properties;

/**
 * Version utility class for the HTTP Sink Connector.
 */
public class Version {
    private static final String VERSION;

    static {
        String version = "unknown";
        try (InputStream stream = Version.class.getResourceAsStream("/connector-version.properties")) {
            if (stream != null) {
                Properties props = new Properties();
                props.load(stream);
                version = props.getProperty("version", version).trim();
            }
        } catch (Exception e) {
            // Ignore and use default version
        }
        VERSION = version;
    }

    public static String getVersion() {
        return VERSION;
    }
}

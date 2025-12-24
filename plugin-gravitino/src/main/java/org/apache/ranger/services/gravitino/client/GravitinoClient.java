package org.apache.ranger.services.gravitino.client;

import java.util.List;

public interface GravitinoClient {
    List<String> listMetalakes(String prefix) throws Exception;
    List<String> listCatalogs(String metalake, String prefix) throws Exception;
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.kudu;

import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.client.BaseClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * RangerService for Apache Kudu.
 */
public class RangerServiceKudu extends RangerBaseService {
	@Override
    public HashMap<String, Object> validateConfig(){
      HashMap<String, Object> responseData = new HashMap<String, Object>();
      String message = "Currently unimplemented. This can be safely ignored.";
      BaseClient.generateResponseDataMap(false, message, message, null, null, responseData);
      return responseData;
    }


    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
      // TODO: implement resource lookup for Kudu policies.
      return new ArrayList<>();
    }

}

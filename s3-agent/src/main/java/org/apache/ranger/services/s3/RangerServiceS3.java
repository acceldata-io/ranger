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
package org.apache.ranger.services.s3;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.s3.client.S3ResourceMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceS3 extends RangerBaseService {

    private static final Logger LOG                     = LoggerFactory.getLogger(RangerServiceS3.class);
    public RangerServiceS3() {
        super();
    }

    @Override
    public void init(RangerServiceDef serviceDef, RangerService service) {
        super.init(serviceDef, service);
    }

    @Override
    public Map<String,Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();
        String 	serviceName  	    = getServiceName();
        LOG.info("==> RangerServiceS3.validateConfig Service: (" + serviceName + " )");

        if ( configs != null) {
            try  {
                ret = S3ResourceMgr.connectionTest(serviceName, configs);
            } catch (S3Exception e) {
                LOG.error("<== RangerServiceS3.validateConfig Error: " + e.getMessage(),e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceS3.validateConfig Response : (" + ret + " )");

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();
        String 	serviceName  	   = getServiceName();
        String	serviceType		   = getServiceType();
        Map<String,String> configs = getConfigs();

        LOG.info("==> RangerServiceS3.lookupResource Context: (" + context + ")");

        if (context != null) {
            try {
                long timeLookup = new Long(this.config.getProperties().getProperty("ranger.resource.lookup.timeout.value.in.ms"));
                ret  = S3ResourceMgr.getS3Resources(serviceName, configs, context, timeLookup);
            } catch (S3Exception e) {
                LOG.error( "<==RangerServiceS3.lookupResource Error : " + e);
                throw e;
            }
        }

        LOG.info("<== RangerServiceS3.lookupResource Response Received");

        return ret;
    }

}

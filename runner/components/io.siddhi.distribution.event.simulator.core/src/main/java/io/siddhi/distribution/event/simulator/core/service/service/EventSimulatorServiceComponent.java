/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.distribution.event.simulator.core.service.service;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.analytics.permissions.PermissionManager;

/**
 * Event simulator service component.
 */
@Component(
        name = "Event-Simulator-Service Component",
        immediate = true
)
public class EventSimulatorServiceComponent {
    @Reference(
            name = "permission-manager",
            service = PermissionManager.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetPermissionManager"
    )
    protected void setPermissionManager(PermissionManager permissionManager) {
        EventSimulatorDataHolder.setPermissionProvider(permissionManager.getProvider());
    }

    protected void unsetPermissionManager(PermissionManager permissionManager) {
        EventSimulatorDataHolder.setPermissionProvider(null);
    }
}

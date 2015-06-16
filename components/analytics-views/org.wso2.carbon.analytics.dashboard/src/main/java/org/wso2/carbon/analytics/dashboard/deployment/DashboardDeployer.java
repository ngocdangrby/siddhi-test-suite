/**
 * Copyright (c) 2005 - 2013, WSO2 Inc. (http://www.wso2.com) All Rights Reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.analytics.dashboard.deployment;

import org.apache.axis2.deployment.DeploymentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.analytics.dashboard.DashboardConstants;
import org.wso2.carbon.analytics.dashboard.DashboardDeploymentException;
import org.wso2.carbon.analytics.dashboard.util.DeploymentUtil;
import org.wso2.carbon.application.deployer.config.Artifact;
import org.wso2.carbon.application.deployer.config.CappFile;
import org.wso2.carbon.registry.core.exceptions.RegistryException;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Deploys dashboard definition files.
 */
public class DashboardDeployer extends AbstractDashboardDeployer {
    private static final Log log = LogFactory.getLog(DashboardDeployer.class);

    @Override
    protected String getArtifactType() {
        return DashboardConstants.DASHBOARD_ARTIFACT_TYPE;
    }

    /**
     * Reads the dashboard defintion and creates a registry resource.
     * @param artifacts
     * @throws DashboardDeploymentException
     */
    protected void deploy(List<Artifact> artifacts) throws DashboardDeploymentException {
        for (Artifact artifact : artifacts) {
            if (DashboardConstants.DASHBOARD_ARTIFACT_TYPE.equals(artifact.getType())) {
                List<CappFile> files = artifact.getFiles();
                if (files == null || files.isEmpty()) {
                    continue;
                }
                for (CappFile cappFile : files) {
                    String fileName = cappFile.getName();
                    String path = artifact.getExtractedPath() + File.separator + fileName;
                    File file = new File(path);
                    try {
                        if (fileName.endsWith(DashboardConstants.DASHBOARD_EXTENSION)) {
                            String dashboardDefn = DeploymentUtil.readFile(file);
                            String resourceName = fileName.substring(0,
                                    fileName.lastIndexOf(DashboardConstants.DASHBOARD_EXTENSION));
                            DeploymentUtil.createRegistryResource(DashboardConstants.DASHBOARDS_RESOURCE_PATH
                                            + resourceName,
                                    dashboardDefn);
                            if (log.isDebugEnabled()) {
                                log.debug("Dashboard definition [" + resourceName + "] has been created.");
                            }
                        }
                    } catch (IOException e) {
                        String errorMsg = "Error while reading from the file : " + file.getAbsolutePath();
                        log.error(errorMsg, e);
                        throw new DashboardDeploymentException(errorMsg, e);
                    } catch (RegistryException e) {
                        String errorMsg = "Error while creating registry resource for dashboard";
                        log.error(errorMsg, e);
                        throw new DashboardDeploymentException(errorMsg, e);
                    }
                }
            }
        }
    }

    /**
     * Deletes the registry resource when undeploying
     * @param artifacts
     */
    @Override
    protected void undeploy(List<Artifact> artifacts) {
        for (Artifact artifact : artifacts) {
            if (DashboardConstants.DASHBOARD_ARTIFACT_TYPE.equals(artifact.getType())) {
                List<CappFile> files = artifact.getFiles();
                String fileName = artifact.getFiles().get(0).getName();
                String artifactPath = artifact.getExtractedPath() + File.separator + fileName;
                File file = new File(artifactPath);
                try {
                    if (fileName.endsWith(DashboardConstants.DASHBOARD_EXTENSION)) {
                        String resourcePath = DashboardConstants.DASHBOARDS_RESOURCE_PATH
                                + fileName.substring(0, fileName.lastIndexOf(DashboardConstants.DASHBOARD_EXTENSION));
                        try {
                            DeploymentUtil.removeRegistryResource(resourcePath);
                        } catch (RegistryException e) {
                            String errorMsg = "Error deleting registry resource " + resourcePath;
                            log.error(errorMsg, e);
                            throw new DashboardDeploymentException(errorMsg, e);
                        }
                    }
                } catch (DeploymentException e) {
                    log.error("Error occurred while trying to undeploy : " + artifact.getName());
                }
            }
        }
    }

}
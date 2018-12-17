/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.siddhi.editor.core.util.designview.codegenerator.generators;

import org.wso2.carbon.siddhi.editor.core.util.designview.beans.configs.siddhielements.TableConfig;
import org.wso2.carbon.siddhi.editor.core.util.designview.constants.SiddhiCodeBuilderConstants;
import org.wso2.carbon.siddhi.editor.core.util.designview.exceptions.CodeGenerationException;
import org.wso2.carbon.siddhi.editor.core.util.designview.utilities.CodeGeneratorUtils;

/**
 * Generates the code for a Siddhi table element
 */
public class TableCodeGenerator {

    /**
     * Generates the Siddhi code representation of a TableConfig object
     *
     * @param table The TableConfig object
     * @return The Siddhi code representation of the given TableConfig object
     * @throws CodeGenerationException Error when generating the code
     */
    public String generateTable(TableConfig table, boolean isToolTip) throws CodeGenerationException {
        CodeGeneratorUtils.NullValidator.validateConfigObject(table);
        StringBuilder tableStringBuilder = new StringBuilder();
        if (!isToolTip) {
            tableStringBuilder.append(SubElementCodeGenerator.generateComment(table.getPreviousCommentSegment()));
        }

        tableStringBuilder.append(SubElementCodeGenerator.generateStore(table.getStore()))
                .append(SubElementCodeGenerator.generateAnnotations(table.getAnnotationList()))
                .append(SiddhiCodeBuilderConstants.NEW_LINE)
                .append(SiddhiCodeBuilderConstants.DEFINE_TABLE)
                .append(SiddhiCodeBuilderConstants.SPACE)
                .append(table.getName())
                .append(SiddhiCodeBuilderConstants.SPACE)
                .append(SiddhiCodeBuilderConstants.OPEN_BRACKET)
                .append(SubElementCodeGenerator.generateAttributes(table.getAttributeList()))
                .append(SiddhiCodeBuilderConstants.CLOSE_BRACKET)
                .append(SiddhiCodeBuilderConstants.SEMI_COLON);

        return  tableStringBuilder.toString();
    }

}

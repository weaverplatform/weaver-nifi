/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weaverplatform.nifi;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;


public class GetWeaverIdTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetWeaverId.class);
    }

    @Test
    public void testProcessor() {

        String demo = "blabla";
        InputStream in = new ByteArrayInputStream(demo.getBytes());

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();

        FlowFile f = session.create();
        f = session.importFrom(in, f);
        f = session.putAttribute(f, "maximo_id", "80");

        testRunner.setProperty(GetWeaverId.ATTRIBUTE, "maximo_id");
        testRunner.setProperty(GetWeaverId.WEAVER, "http://localhost:9487");

        testRunner.enqueue(f);

        testRunner.run();

        List<MockFlowFile> listFlowFiles = testRunner.getFlowFilesForRelationship("original");
        MockFlowFile result = listFlowFiles.get(0);

        result.assertAttributeExists("weaver_id");
        String resultValue = result.getAttribute("weaver_id");

        System.out.println(resultValue);

        assertEquals(resultValue, "90");
    }

}

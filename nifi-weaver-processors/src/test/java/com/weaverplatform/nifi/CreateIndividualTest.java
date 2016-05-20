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

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.Weaver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;


public class CreateIndividualTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CreateIndividual.class);
    }

    @Test
    public void testOnTrigger(){


        try {

            //random info and simulate flowfile (with attributes) passed through to this processor in early state
            String file = "line.txt";
            byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
            InputStream in = new ByteArrayInputStream(contents);
            InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
            ProcessSession session = testRunner.getProcessSessionFactory().createSession();
            FlowFile f = session.create();
            f = session.importFrom(cont, f);
            f = session.putAttribute(f, "id", "cio5u54ts00023j6ku6j3j1jg"); //"816ee370-4274-e211-a3a8-b8ac6f902f00");
//
//
            String connectionUrl = "http://weaver.test.ib.weaverplatform.com";
//
            //from nifi-envi the user specifies this dynamic attribute, which to look for on the flowfile later
            // Add properties (required)
            testRunner.setProperty(CreateIndividual.WEAVER, connectionUrl);//"http://localhost:9487");
            testRunner.setProperty(CreateIndividual.INDIVIDUAL_ATTRIBUTE, "id"); // RDF_TYPE_STATIC: ib:Afsluitboom

            // Add the flowfile to the runner
            testRunner.enqueue(f);

            // Run the enqueued content, it also takes an int = number of contents queued
            testRunner.run();

            Weaver weaver = new Weaver();
            weaver.connect(connectionUrl);

            Entity e = weaver.get("cio5u54ts00023j6ku6j3j1jg");
            System.out.println(e.getId());

//            //get original flowfile contents
//            List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("original");
//            MockFlowFile result = results.get(0);
//            String resultValue = new String(testRunner.getContentAsByteArray(result));
//            System.out.println(resultValue);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
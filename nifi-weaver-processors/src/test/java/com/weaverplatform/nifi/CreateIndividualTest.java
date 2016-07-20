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

import com.google.common.io.Resources;
import com.weaverplatform.nifi.individual.CreateIndividual;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class CreateIndividualTest {

  private TestRunner testRunner;

  private Weaver weaver;
  private static String WEAVER_URL;
  private static String WEAVER_DATASET;

  public static final int BATCH_NUM = 1;

  @BeforeClass
  public static void beforeClass() throws IOException {

    // Define property file for NiFi
    Properties props = System.getProperties();
    props.setProperty("nifi.properties.file.path", Resources.getResource("nifi.properties").getPath());

    // Read test properties
    Properties testProperties = new Properties();
    testProperties.load(Resources.getResource("test.properties").openStream());
    WEAVER_URL     = testProperties.get("weaver.url").toString();
    WEAVER_DATASET = testProperties.get("weaver.global.dataset").toString();

    // Set Nifi Weaver properties
    NiFiProperties.getInstance().put(WeaverProperties.URL, WEAVER_URL);
    NiFiProperties.getInstance().put(WeaverProperties.DATASET, WEAVER_DATASET);
  }

  @Before
  public void init() throws URISyntaxException {

    // Wipe weaver database first
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));
    weaver.wipe();

    System.out.println(new File(getClass().getClassLoader().getResource("nifi.properties").getFile()).toString());
    Properties props = System.getProperties();
    props.setProperty("nifi.properties.file.path", new File(getClass().getClassLoader().getResource("nifi.properties").getFile()).toString());
    testRunner = TestRunners.newTestRunner(CreateIndividual.class);
  }

  @Test
  public void testIndividualCreationWithName() {

    String id = UUID.randomUUID().toString();


    InputStream cont = new ByteArrayInputStream("Test".getBytes());

    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(cont, flowFile);
    flowFile = session.putAttribute(flowFile, "id", id);
    flowFile = session.putAttribute(flowFile, "name", "Name is set");

    testRunner.setProperty(CreateIndividual.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(CreateIndividual.NAME_ATTRIBUTE, "name");
    testRunner.setProperty(CreateIndividual.SOURCE_STATIC, "testSource");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    Entity reloaded = weaver.get(id, new ReadPayload.Opts(-1));
    assertEquals("Name is set",  reloaded.getAttributes().get("name"));
    assertEquals("testSource",  reloaded.getAttributes().get("source"));

  }

  @Test
  public void testIndividualCreationWithPostponedName() {

    String id = UUID.randomUUID().toString();


    InputStream cont = new ByteArrayInputStream("Test".getBytes());

    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    FlowFile flowFile1 = session.create();
    flowFile1 = session.importFrom(cont, flowFile1);
    flowFile1 = session.putAttribute(flowFile1, "id", id);
//    flowFile = session.putAttribute(flowFile, "name", "Name is set");
    flowFile1 = session.putAttribute(flowFile1, "source", "partly");

    testRunner.setProperty(CreateIndividual.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(CreateIndividual.SOURCE_ATTRIBUTE, "source");
    testRunner.setProperty(CreateIndividual.IS_ADDIFYING, "true");
    testRunner.enqueue(flowFile1);
    testRunner.run();

    Entity reloaded = weaver.get(id, new ReadPayload.Opts(-1));
    assertEquals("Unnamed",  reloaded.getAttributes().get("name"));
    assertEquals("partly",  reloaded.getAttributes().get("source"));



    cont = new ByteArrayInputStream("Test".getBytes());
    session = testRunner.getProcessSessionFactory().createSession();
    FlowFile flowFile2 = session.create();
    flowFile2 = session.importFrom(cont, flowFile2);
    flowFile2 = session.putAttribute(flowFile2, "id", id);
    flowFile2 = session.putAttribute(flowFile2, "name", "Name is set");
    flowFile2 = session.putAttribute(flowFile2, "source", "complete");

    testRunner.enqueue(flowFile2);
    testRunner.setProperty(CreateIndividual.NAME_ATTRIBUTE, "name");
    testRunner.run();




    reloaded = weaver.get(id, new ReadPayload.Opts(-1));
    assertEquals("Name is set",  reloaded.getAttributes().get("name"));
    assertEquals("complete",  reloaded.getAttributes().get("source"));

  }

  @Test
  public void testIndividualCreationWithNameForPerformance() {

    long then = new Date().getTime();

    int runsLeft = BATCH_NUM;
    while(runsLeft-- > 0) {

      InputStream cont = new ByteArrayInputStream("Test".getBytes());

      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile flowFile = session.create();
      flowFile = session.importFrom(cont, flowFile);
      flowFile = session.putAttribute(flowFile, "id", UUID.randomUUID().toString());
      flowFile = session.putAttribute(flowFile, "name", "Name is set");

      testRunner.setProperty(CreateIndividual.INDIVIDUAL_ATTRIBUTE, "id");
      testRunner.setProperty(CreateIndividual.NAME_ATTRIBUTE, "name");
      testRunner.setProperty(CreateIndividual.SOURCE_STATIC, "testSource");

      // Add the flowfile to the runner
      testRunner.enqueue(flowFile);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();
    }

    long now = new Date().getTime();
    System.out.println(now - then);
  }

  @Test
  public void testOnTrigger() {

    try {

      // Random info and simulate flowfile (with attributes) passed through to this processor in early state
      String file = "line.txt";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = session.create();
      f = session.importFrom(cont, f);
      f = session.putAttribute(f, "id", "cio5u54ts00023j6ku6j3j1jg"); //"816ee370-4274-e211-a3a8-b8ac6f902f00");

      String connectionUrl = "http://weaver.test.ib.weaverplatform.com";

      //from nifi-envi the user specifies this dynamic attribute, which to look for on the flowfile later
      // Add properties (required)
      testRunner.setProperty(CreateIndividual.WEAVER, connectionUrl);//"http://localhost:9487");
      testRunner.setProperty(CreateIndividual.INDIVIDUAL_ATTRIBUTE, "id"); // RDF_TYPE_STATIC: ib:Afsluitboom

      // Add the flowfile to the runner
      testRunner.enqueue(f);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();

      Weaver weaver = new Weaver();
      weaver.connect(new WeaverSocket(new URI(connectionUrl)));

      Entity e = weaver.get("cio5u54ts00023j6ku6j3j1jg");
      System.out.println(e.getId());

//            //get original flowfile contents
//            List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("original");
//            MockFlowFile result = results.get(0);
//            String resultValue = new String(testRunner.getContentAsByteArray(result));
//            System.out.println(resultValue);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
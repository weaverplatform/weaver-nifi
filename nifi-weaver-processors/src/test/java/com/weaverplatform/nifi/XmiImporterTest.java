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
import com.weaverplatform.nifi.individual.XmiImporter;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Weaver;
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
import java.util.Properties;


public class XmiImporterTest {

  private TestRunner testRunner;

  private Weaver weaver;
  private static String WEAVER_URL;
  private static String WEAVER_DATASET;


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
    testRunner = TestRunners.newTestRunner(XmiImporter.class);
  }



  @Test
  public void testOnTrigger() {

    try {

      // Random info and simulate flowfile (with attributes) passed through to this processor in early state
      String file = "xmi.xml";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = session.create();
      f = session.importFrom(cont, f);

      // Add the flowfile to the runner
      testRunner.enqueue(f);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
package com.weaverplatform.nifi.view;

import com.google.common.io.Resources;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.model.Dataset;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CreateViewTest {

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
    testRunner = TestRunners.newTestRunner(CreateView.class);
    
    // Wipe weaver database first
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));
    weaver.wipe();
    
    // Create dataset
    Entity dataset = new Dataset(weaver, WEAVER_DATASET).create();
  }
  

  @Test
  public void testViewCreationWithVariableNameAndId() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    
    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);
    
    // Set attributes
    String viewId = UUID.randomUUID().toString();
    flowFile = session.putAttribute(flowFile, "id", viewId);
    flowFile = session.putAttribute(flowFile, "name", "View name");

    testRunner.setProperty(CreateView.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(CreateView.NAME_ATTRIBUTE, "name");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();
    
    // Assert View is created in Weaver
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));
    
    Entity view = weaver.get(viewId);
    assertEquals(view.getAttributes().get("name"), "View name");
    assertNotNull(view.getRelations().get("objects"));
    assertNotNull(view.getRelations().get("filters"));
  }


  @Test
  public void testViewCreationWithStaticNameAndId() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();

    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    String viewId = UUID.randomUUID().toString();
    testRunner.setProperty(CreateView.INDIVIDUAL_STATIC, viewId);
    testRunner.setProperty(CreateView.NAME_STATIC, "View name");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Assert View is created in Weaver
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));

    Entity view = weaver.get(viewId);
    assertEquals(view.getAttributes().get("name"), "View name");
    assertNotNull(view.getRelations().get("objects"));
    assertNotNull(view.getRelations().get("filters"));
  }
}
package com.weaverplatform.nifi.view;

import com.google.common.io.Resources;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.model.Dataset;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class CreateFilterTest {

  private TestRunner testRunner;

  private Weaver weaver;
  private Entity dataset;
  private Entity view;
  
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
    testRunner = TestRunners.newTestRunner(CreateFilter.class);
    
    // Wipe weaver database first
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));
    weaver.wipe();
    
    // Create dataset
    dataset = new Dataset(weaver, WEAVER_DATASET).create();
    
    // Create view
    Map<String, String> viewAttributes = new HashMap<>();
    viewAttributes.put("name", "View");
    view = weaver.add(viewAttributes, EntityType.VIEW);

    // Give it the minimal collections it needs to be qualified as a valid view
    view.linkEntity("filters", weaver.collection());
  }
  

  @Test
  public void testFilterCreation() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    
    // Set properties
    testRunner.setProperty(CreateFilter.VIEW_ID_ATTRIBUTE, "viewId");
    testRunner.setProperty(CreateFilter.CELLTYPE_STATIC, "individual");
    testRunner.setProperty(CreateFilter.LABEL_STATIC, "Type");
    testRunner.setProperty(CreateFilter.PREDICATE_STATIC, "rdf:type");
    testRunner.setProperty(CreateFilter.ATTRIBUTE_NAME_FOR_FILTER_ID, "filterId");

    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);
    
    // Set attributes
    flowFile = session.putAttribute(flowFile, "viewId", view.getId());

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get result
    List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(CreateFilter.ORIGINAL);
    assertTrue("1 match", results.size() == 1);
    MockFlowFile result = results.get(0);

    String filterId = result.getAttribute("filterId");

    // Assert Filter is created in Weaver
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));

    // Load View
    Entity reloadedView = weaver.get(view.getId());
    assertEquals(reloadedView.getAttributes().get("name"), "View");
    assertNotNull(reloadedView.getRelations().get("filters"));
    
    Entity filters = weaver.get(reloadedView.getRelations().get("filters").getId());
    assertNotNull(filters.getRelations().get(filterId));
    
    // Load Filter
    Entity filter = weaver.get(filterId);
    assertEquals(filter.getAttributes().get("celltype"),  "individual");
    assertEquals(filter.getAttributes().get("label"),     "Type");
    assertEquals(filter.getAttributes().get("predicate"), "rdf:type");
  }
}
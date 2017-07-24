package com.weaverplatform.nifi;

import com.google.common.io.Resources;
import com.weaverplatform.nifi.individual.NativeQuery;
import com.weaverplatform.nifi.individual.PropertyExists;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.ShallowEntity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.model.Dataset;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.commons.io.IOUtils;
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
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class NativeQueryTest {

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
  }
  
  @Before
  public void init() throws URISyntaxException {
    // Set Nifi Weaver properties
    NiFiProperties.getInstance().put(WeaverProperties.URL, WEAVER_URL);
    NiFiProperties.getInstance().put(WeaverProperties.DATASET, WEAVER_DATASET);

    testRunner = TestRunners.newTestRunner(NativeQuery.class);
  }
  
  
  @Test
  public void testCountQuery() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    
    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    String countQuery = "SELECT (COUNT(*) as ?count) WHERE {?s ?p ?o}";

    testRunner.setProperty(NativeQuery.SELECT, "count");
    testRunner.setProperty(NativeQuery.QUERY, countQuery);

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> original   = testRunner.getFlowFilesForRelationship(NativeQuery.ORIGINAL);
    List<MockFlowFile> results    = testRunner.getFlowFilesForRelationship(NativeQuery.RESULT_ROW);

    MockFlowFile result = results.get(0);
    String resultValue = new String(testRunner.getContentAsByteArray(result));

    assertEquals(1, original.size());
    assertEquals(1, results.size());
  }
}
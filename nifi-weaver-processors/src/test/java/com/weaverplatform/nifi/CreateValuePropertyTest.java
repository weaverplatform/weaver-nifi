package com.weaverplatform.nifi;

import com.google.common.io.Resources;
import com.weaverplatform.nifi.individual.CreateValueProperty;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.model.Dataset;
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
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CreateValuePropertyTest {

  private TestRunner testRunner;

  private Weaver weaver;
  private static String WEAVER_URL;
  private static String WEAVER_DATASET;

  Entity dataset;
  Entity datasetObjects;

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

    // Create dataset
    dataset = new Dataset(weaver, WEAVER_DATASET).get(WEAVER_DATASET);
    datasetObjects = weaver.get(dataset.getRelations().get("objects").getId());

    System.out.println(new File(getClass().getClassLoader().getResource("nifi.properties").getFile()).toString());
    Properties props = System.getProperties();
    props.setProperty("nifi.properties.file.path", new File(getClass().getClassLoader().getResource("nifi.properties").getFile()).toString());
    testRunner = TestRunners.newTestRunner(CreateValueProperty.class);
  }

  @Test
  public void testOnTrigger() {

    ConcurrentHashMap<String, String> subjectAttributes = new ConcurrentHashMap<>();
    subjectAttributes.put("name", "subjectThing");
    Entity subjectEntity = weaver.add(subjectAttributes, EntityType.INDIVIDUAL, "816ee370-4274-e211-a3a8-b8ac6f902f00");
    subjectEntity.linkEntity("properties", weaver.collection().toShallowEntity());

    // Attach to dataset
    datasetObjects.linkEntity(subjectEntity.getId(), subjectEntity.toShallowEntity());
    
    try {

      //random info and simulate flowfile (with attributes) passed through to this processor in early state
      String file = "line.txt";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
      ProcessSession session = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = session.create();
      f = session.importFrom(cont, f);
      f = session.putAttribute(f, "id", "816ee370-4274-e211-a3a8-b8ac6f902f00");
      f = session.putAttribute(f, "name", "(AB CT1-N-06) Snelle doorsteek A10");


      //from nifi-envi the user specifies this dynamic attribute, which to look for on the flowfile later
      // Add properties (required)
      testRunner.setProperty(CreateValueProperty.WEAVER, WEAVER_URL);
      testRunner.setProperty(CreateValueProperty.SUBJECT_ATTRIBUTE, "id");
      testRunner.setProperty(CreateValueProperty.PREDICATE_STATIC, "rdf:label");
      testRunner.setProperty(CreateValueProperty.OBJECT_ATTRIBUTE, "name");

      // Add the flowfile to the runner
      testRunner.enqueue(f);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();

    } catch (IOException e) {
      System.out.println(e.getStackTrace());
    }
  }
}
package com.weaverplatform.nifi;

import com.google.common.io.Resources;
import com.weaverplatform.nifi.individual.PropertyExists;
import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.ShallowEntity;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class PropertyExistsTest {

  private TestRunner testRunner;

  private Weaver weaver;
  private static String WEAVER_URL;    
  private static String WEAVER_DATASET;

  private Entity dataset;
  private Entity individualWithProperties;
  private Entity individualWithoutProperties;
  
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
    testRunner = TestRunners.newTestRunner(PropertyExists.class);
    
    // Wipe weaver database first
    weaver = new Weaver();
    weaver.connect(new WeaverSocket(new URI(WEAVER_URL)));
    weaver.wipe();

    // Create dataset
    dataset = new Dataset(weaver, WEAVER_DATASET).get(WEAVER_DATASET);
    Entity objects = weaver.get(dataset.getRelations().get("objects").getId());
    
    // Create individuals
    ConcurrentMap<String, String> attributes = new ConcurrentHashMap<>();
    attributes.put("name", "Individual With Properties");
    individualWithProperties = weaver.add(attributes, EntityType.INDIVIDUAL);

    attributes = new ConcurrentHashMap<>();
    attributes.put("name", "Individual Without Properties");
    individualWithoutProperties = weaver.add(attributes, EntityType.INDIVIDUAL);

    // Give it the properties
    Entity properties = weaver.collection();
    individualWithProperties.linkEntity("properties", properties.toShallowEntity());
    
    // Add two properties
    attributes = new ConcurrentHashMap<>();
    attributes.put("predicate", "rdf:type");

    ConcurrentMap<String, ShallowEntity> relations = new ConcurrentHashMap<>();
    relations.put("subject", individualWithProperties.toShallowEntity());
    relations.put("object", individualWithoutProperties.toShallowEntity());
    Entity property1 = weaver.add(attributes, EntityType.INDIVIDUAL_PROPERTY, UUID.randomUUID().toString(), relations);
    properties.linkEntity(property1.getId(), property1.toShallowEntity());

    attributes = new ConcurrentHashMap<>();
    attributes.put("predicate", "rdfs:label");
    attributes.put("object", "RDFS Label");
    relations = new ConcurrentHashMap<>();
    relations.put("subject", individualWithProperties.toShallowEntity());
    
    Entity property2 = weaver.add(attributes, EntityType.VALUE_PROPERTY, UUID.randomUUID().toString(), relations);
    properties.linkEntity(property2.getId(), property2.toShallowEntity());

    // Attach to dataset
    objects.linkEntity(individualWithoutProperties.getId(), individualWithoutProperties.toShallowEntity());
    objects.linkEntity(individualWithProperties.getId(), individualWithProperties.toShallowEntity());
  }
  
  
  @Test
  public void testIndividualPropertyExists() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    
    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    flowFile = session.putAttribute(flowFile, "id", individualWithProperties.getId());
    testRunner.setProperty(PropertyExists.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(PropertyExists.PREDICATE_STATIC, "rdf:type");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> exists   = testRunner.getFlowFilesForRelationship(PropertyExists.EXISTS);
    List<MockFlowFile> notExits = testRunner.getFlowFilesForRelationship(PropertyExists.NOT_EXISTS);
    assertEquals(1, exists.size());
    assertEquals(0, notExits.size());
  }  
  
  @Test
  public void testValuePropertyExists() throws URISyntaxException {
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();
    
    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    flowFile = session.putAttribute(flowFile, "id", individualWithProperties.getId());
    testRunner.setProperty(PropertyExists.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(PropertyExists.PREDICATE_STATIC, "rdfs:label");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> exists   = testRunner.getFlowFilesForRelationship(PropertyExists.EXISTS);
    List<MockFlowFile> notExits = testRunner.getFlowFilesForRelationship(PropertyExists.NOT_EXISTS);
    assertEquals(1, exists.size());
    assertEquals(0, notExits.size());
  }


  @Test
  public void testPropertyExistsDynamic(){
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();

    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    flowFile = session.putAttribute(flowFile, "id", individualWithProperties.getId());
    flowFile = session.putAttribute(flowFile, "predicate", "rdfs:label");
    testRunner.setProperty(PropertyExists.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(PropertyExists.PREDICATE_ATTRIBUTE, "predicate");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> exists   = testRunner.getFlowFilesForRelationship(PropertyExists.EXISTS);
    List<MockFlowFile> notExits = testRunner.getFlowFilesForRelationship(PropertyExists.NOT_EXISTS);
    assertEquals(1, exists.size());
    assertEquals(0, notExits.size());
  }
  
  @Test
  public void testPropertyDoesNotExistsWhileHavingProperties(){
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();

    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    flowFile = session.putAttribute(flowFile, "id", individualWithProperties.getId());
    testRunner.setProperty(PropertyExists.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(PropertyExists.PREDICATE_STATIC, "rdfs:doesnothave");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> exists   = testRunner.getFlowFilesForRelationship(PropertyExists.EXISTS);
    List<MockFlowFile> notExits = testRunner.getFlowFilesForRelationship(PropertyExists.NOT_EXISTS);
    assertEquals(0, exists.size());
    assertEquals(1, notExits.size());   
  }
  
  @Test
  public void testPropertyDoesNotExistsWhileHavingNoProperties(){
    ProcessSession session = testRunner.getProcessSessionFactory().createSession();

    // Create flowFile with content
    FlowFile flowFile = session.create();
    flowFile = session.importFrom(new ByteArrayInputStream("Flowfile Content".getBytes()), flowFile);

    // Set attributes
    flowFile = session.putAttribute(flowFile, "id", individualWithoutProperties.getId());
    testRunner.setProperty(PropertyExists.INDIVIDUAL_ATTRIBUTE, "id");
    testRunner.setProperty(PropertyExists.PREDICATE_STATIC, "rdfs:doesnothave");

    // Add the flowfile to the runner
    testRunner.enqueue(flowFile);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // Get results
    List<MockFlowFile> exists   = testRunner.getFlowFilesForRelationship(PropertyExists.EXISTS);
    List<MockFlowFile> notExits = testRunner.getFlowFilesForRelationship(PropertyExists.NOT_EXISTS);
    assertEquals(0, exists.size());
    assertEquals(1, notExits.size());
  }
}
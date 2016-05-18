# Apache-Nifi
apache nifi components

A collection of processors for the NiFi platform for connecting to Weaver.

## CreateIndividual

This component allows the user to enter a dynamic attribute used later to search on the flowfile to get that same flowfile attribute.
When found, it communicates to the weaver-sdk-java to use this attribute value to create a Weaver Entity (Individual) Object.

## CreateValueProperty

This component allows the user to enter a few dynamic attribute used later to search on the flowfile to get that same flowfile attribute.
When found, it communicates to the weaver-sdk-java to use this attribute value to create a Weaver Entity (ValueProperty) Object.

## CreateIndividualProperty
...

## GetWeaverId

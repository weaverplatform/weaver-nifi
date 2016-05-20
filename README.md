# Apache-Nifi
apache nifi components

A collection of processors for the NiFi platform for connecting to Weaver.

## CreateIndividual

This component allows the user to set the component-property 'individual' to a static value or, if avaible, flowfile attribute value usage.
The static and attribute value will refer to the _META.id of an Weaver.Entity Object. In conclusion, the component either looks which value to use/retrieve and communicates to the weaver-sdk-java to create a new Weaver Entity (Individual) Object. To communicate with weaver-sdk-java, the static component-property 'weaver_url' needs to be set too.

## CreateValueProperty

In the NiFi-flow, this component is a child-component of CreateIndividual. In addition to CreateIndividual, this component extend the static and attribute component-properties to define a subject, predicate and object. If those properties are set, the component communicates with the weaver-sdk-java to use these to create a Weaver Entity (ValueProperty) Object and will link to its parent (the CreateIndivual Object).

## CreateIndividualProperty
...

## GetWeaverId

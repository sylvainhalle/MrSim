MrSim
=====

MrSim is a basic implementation of the map-reduce algorithm in Java.

## What is MapReduce?

MapReduce is a programming model for processing large data sets, and the
name of an implementation of the model by Google. MapReduce is typically
used to do distributed computing on clusters of computers. See
[Wikipedia](http://en.wikipedia.org/wiki/MapReduce) for a detailed
description of MapReduce.

There exist many implementations of the map-reduce model, the most
popular probably being [Apache Hadoop](http://hadoop.apache.org/).

## What is MrSim?

MrSim is a simple implementation of map-reduce in Java, intended for a
*pedagogical* illustration of the programming model. It originates from
frustrating experiences using other frameworks, which require a lengthy and
cumbersome setup before running even the simplest example. In most cases
those examples are entangled with technical considerations (distributed file
system, network configuration) that distract from learning the map-reduce
programming model itself.

MrSim aims at providing a simple framework to create and test map-reduce
jobs using using a minimal setup (actually no setup at all), using
straightforward implementations of all necessary concepts. This entails some
purposeful limitations to the system:

- It is not optimized in any way, and should not be used to run serious
  map-reduce computations
- It only offers sequential processing of the map-reduce tuples in a single
  process

In counterpart, MrSim offers interesting features from a pedagogical point
of view:

- It runs out of the box, simply add the classes (or the jar) to your
  classpath
- The centralized processing makes it easy to perform step-by-step debugging
  of a map-reduce job (down to the core implementatios of the framework,
  since all source code is provided)
- The map-reduce environment itself is made of **less than 250 lines of
  code**
- The examples and underlying implementation are simple and easy to
  understand

As a rule, don't expect any fancy features to be introduced if they
interfere with the system's current simplicity.

## How to use MrSim?

See the `Source/Examples` folder for some examples, and the
`Source/MapReduce/doc` folder for detailed documentation of the code.

## Who maintains MrSim?

MrSim has been developed and is currently maintained by
[Sylvain Hallé](http://www.leduotang.com/sylvain), assistant professor at
Université du Québec à Chicoutimi (Canada).

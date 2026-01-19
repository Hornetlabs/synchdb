# Debezium Runner Component Architecture - Java

## **Debezium Runner Component Diagram**

![img](../../images/synchdb-dbzrunner-component2.jpg)

Debezium Runner resides on Java side of the deployment. It is the main facilitator between embedded Debezium engine (Java) and SynchDB Worker (C). It provides several Java methods that SynchDB worker can interact via JNI library. These interactions include initializing a Debezium engine, start or stop the engine, obtain a batch of change events and mark a batch as done. These operations are essential for ensuring replication consistency. Main components are:

1. Parameter Class
2. Controller
3. Emitter
4. Batch Manager

### **1) Parameter Class**

The parameter class represents a JAVA class that include a list of exposed parameters and methods that allow SynchDB Worker to set/get the parameter values. These parameters affect how Debezium Runner and the embedded Debezium perform. These parameters have also been exposed to PostgreSQL GUCs so that SynchDB worker could invoke the respective methods to set the parameter values. This is currently the only way to pass configuration from C based SynchDB Worker to JAVA based Debezium Runner.

### **2) Controller**

The controller allows SynchDB to start or stop the Embedded Debezium Engine. This is done via several JAVA methods that SynchDB Worker can invoke to control Debezium Engine.

### **3) Emitter**

The emitter represents a JAVA method that is periodically invoked by the "Event Fetcher" component in SynchDB Worker. It is mainly responsible to pop a batch from "4) Batch Manager", formulate it into a List of JSON events as String and return it to "Event Fetcher" via JNI. If Batch Manager has no batch available, it will return NULL and no processing will happen on the "Event Fetcher" side.

### **4) Batch Manager**

The Batch Manager is mainly responsible for receiving new batches originated from Embedded Debezium Engine and stores it in its internal queue. This queue is much smaller and is different from the Batch Queue inside Embedded Debezium Engine in the component diagram. When Batch Manager's internal queue is full, a throttle control will activate to temporarily halt the event generation from Debezium side until this small batch queue has free space. A batch is taken away from the queue only when the "Emitter" receives a fetch request from "Event Fetcher" and pops a batch from batch manager.

Batch manager also assigns a batch ID to each batch popped, sent to emitter and eventually to the SynchDB worker to process. This unique ID is stored in its interal hash table along with a "committer" object associated with it. These pieces of information is important to keep track. When SynchDB worker finishes a batch, it will invoke a Mark Batch Completion method within the Batch Manager, the same batch ID is also included, which helps the Batch Manager to look up the corresponding "committer" object. The committer object is then used to notify the Embedded Debezium Engine that a batch is completed, forcing it to commit and move its internal offset forward. This ensures that the same batch will not be processed again (no duplication) in case of engine restarts.
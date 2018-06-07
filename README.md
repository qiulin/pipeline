# Pipeline
Stateful fail-tolerant data streaming library based on Akka Streams.

## Installation
```sbtshell
libraryDependencies += "com.commodityvectors" %% "pipeline" % "0.5.2"
```

## Introduction
Akka Streams is a great reactive streaming library for Java and Scala. 
However is lacks one key feature for long-running stateful streams: **recovery from failures**.
This library aims to cover that.

## How it works

Let's start with normal Akka Stream:
```
Source[A, _] ------> Flow[A, B, _] -------> Sink[B, _]
```

Pipeline create a higher level stream on top of it:
```
Source[Message[A], _] ------> Flow[Message[A], Message[B], _] -------> Sink[Message[B], _]
```

Where ```Message[A]``` can be of two types:
- UserMessage[A] - your data
- SystemMessage - control signals, managed by the library

The library makes it transparent to the user, so that he doesn't have to deal with ```Message``` type 
but continues to work and think in terms of user types ```A```, ```B```, etc. 

Based on configured intervals, a "snapshot" signal is injected into all sources and propageted through the stream. 
When it reaches a component latter can save own state, then signal follows further, up to a sink.

In case of failure the whole stream can be restored by propagating a message with the snapshot.

The process is inspired by [Chandy-Lamport algorithm](https://en.wikipedia.org/wiki/Chandy-Lamport_algorithm) 
and similar implementation in [Apache Flink](https://ci.apache.org/projects/flink/flink-docs-master/internals/stream_checkpointing.html).

## Basic concepts
### DataComponent
Every stream processing stage (DataReader, DataTransformer or DataWriter) extends DataComponent.
```Scala
trait DataComponent {

  /**
    * Called once at the beginning of component life-cycle.
    * Note that restoreState method will be called before init if there is a restore process.
    */
  def init(context: DataComponentContext): Future[Unit] = Future.successful()
}

trait DataComponentContext {

  /**
    * Identifier assigned to the component
    * @return
    */
  def id: String
}
```
### DataReader
Defines a source of the user data.
```Scala
trait DataReader[A] extends DataComponent with Closeable {

  /**
    * Fetch data asynchronously.
    * @return Number of data items fetched or 0 to signal stream end
    */
  def fetch(): Future[Int]

  /**
    * Pull the fetched data
    * @return Some data or None
    */
  def pull(): Option[A]

  override def close() = ()
}

```
### DataTransformer
Defines a transformation, like async flatMap.
```Scala
trait DataTransformer[In, Out] extends DataComponent {

  def transform(elem: In): Future[immutable.Seq[Out]]
}
```
### DataWriter
Writes data somewhere.
```Scala
trait DataWriter[A] extends DataComponent with Closeable {

  def write(elem: A): Future[Done]

  override def close() = ()
}

```
### Snapshottable
Implement this trait for every DataComponent that has state.
```Scala
trait Snapshottable {

  type Snapshot <: Serializable

  def snapshotState(snapshotId: SnapshotId,
                    snapshotTime: DateTime): Future[Snapshot]

  def restoreState(state: Snapshot): Future[Unit]
}

```
### SnapshotDao
Create your own implementation to save snapshots to your data storage.
```Scala
trait SnapshotDao {

  def truncate(): Future[Done]

  def findMetadata(snapshotId: SnapshotId): Future[Option[SnapshotMetadata]]

  def findLatestMetadata(): Future[Option[SnapshotMetadata]]

  def writeMetadata(snapshot: SnapshotMetadata): Future[Done]

  def writeData[T <: Serializable](snapshotId: SnapshotId,
                                   componentId: ComponentId,
                                   data: T): Future[ComponentSnapshotMetadata]

  def readData[T <: Serializable](snapshotId: SnapshotId,
                                  componentId: ComponentId): Future[T]
}
```
### Coordinator
```Coordinator``` manages control signals and collecting snapshots.

## Usage
1. Create actor system and materializer:

    ```Scala
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    ```

2. Create snapshot DAO instance:

    ```Scala
    implicit val dao: SnapshotDao = new FileSnapshotDao(Paths.get("./snapshots"))
    ```

3. Create and start Coordinator:

    ```Scala
    implicit val coordinator: Coordinator = Coordinator(
        dao,
        CoordinatorSettings
          .load()
          .withReader("reader") // tell coordinator what sources and sinks exist in the stream
          .withWriter("writer")
          .withSnapshotInterval(10.seconds)
    )
 
    coordinator.start() // or restore() if your app crashed and you want to recover from latest snapshot
    ```
    
4. Define and start a stream:
    ```Scala
    import com.commodityvectors.pipeline._
    Source
      .fromDataReader(new SentenceReader, "reader")
      .viaDataTransformer(new WordCount, "counter")
      .runWith(Sink.fromDataWriter(new DummySink, "writer"))
    ```
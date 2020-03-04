// in Scala
spark.conf.set("spark.sql.shuffle.partitions", 5)
val static = spark.read.json("/data/activity-data")
val streaming = spark
  .readStream
  .schema(static.schema)
  .option("maxFilesPerTrigger", 10)
  .json("/data/activity-data")


// COMMAND ----------

streaming.printSchema()

root
 |-- Arrival_Time: long (nullable = true)
 |-- Creation_Time: long (nullable = true)
 |-- Device: string (nullable = true)
 |-- Index: long (nullable = true)
 |-- Model: string (nullable = true)
 |-- User: string (nullable = true)
 |-- gt: string (nullable = true)
 |-- x: double (nullable = true)
 |-- y: double (nullable = true)
 |-- z: double (nullable = true)



// COMMAND ----------

// in Scala
// Creation_Time as event_time
val withEventTime = streaming.selectExpr(
  "*",
  "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")



// printSchema:
// root
//  |-- Arrival_Time: long (nullable = true)
//  |-- Creation_Time: long (nullable = true)
//  |-- Device: string (nullable = true)
//  |-- Index: long (nullable = true)
//  |-- Model: string (nullable = true)
//  |-- User: string (nullable = true)
//  |-- gt: string (nullable = true)
//  |-- x: double (nullable = true)
//  |-- y: double (nullable = true)
//  |-- z: double (nullable = true)
    |-- event_time:long (nullable = true)


// COMMAND ----------

// in Scala
// group by window(col("event_time"))
// window(col("event_time"), "10 minutes")
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


spark.sql("SELECT * FROM events_per_window").printSchema()


root
 |-- window: struct (nullable = false)
 |    |-- start: timestamp (nullable = true) |    
 |    |-- end: timestamp (nullable = true)
 |-- count: long (nullable = false)



sql("SELECT * FROM events_per_window")

+---------------------------------------------+-----+
|window                                       |count|
+---------------------------------------------+-----+
|[2015-02-23 10:40:00.0, 2015-02-23 10:50:00.0]|11035|
|[2015-02-24 11:50:00.0, 2015-02-24 12:00:00.0]|18854|
...
|[2015-02-23 13:40:00.0, 2015-02-23 13:50:00.0]|20870|
|[2015-02-23 11:20:00.0, 2015-02-23 11:30:00.0]|9392 |
+---------------------------------------------+-----+



// COMMAND ----------

// in Scala
// - group by event_time, user
// - window(col("event_time"), "10 minutes"), "User")

import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes"), "User").count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

// in Scala
// window(col("event_time"), "10 minutes", "5 minutes")

import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
  .count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()

 +---------------------------------------------+-----+
|window                                       |count|
+---------------------------------------------+-----+
|[2015-02-23 14:15:00.0,2015-02-23 14:25:00.0]|40375|
|[2015-02-24 11:50:00.0,2015-02-24 12:00:00.0]|56549|
...
|[2015-02-24 11:45:00.0,2015-02-24 11:55:00.0]|51898|
|[2015-02-23 10:40:00.0,2015-02-23 10:50:00.0]|33200|
+---------------------------------------------+-----+ 


// COMMAND ----------

// in Scala
// withWatermark
// window(col("event_time"), "10 minutes", "5 minutes"):
//     10-minute windows, starting every 5 minutes. 
import org.apache.spark.sql.functions.{window, col}
withEventTime
  .withWatermark("event_time", "5 hours")
  .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
  .count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


SELECT * FROM events_per_window

+---------------------------------------------+-----+
|window                                       |count|
+---------------------------------------------+-----+
|[2015-02-23 14:15:00.0,2015-02-23 14:25:00.0]|9505 |
|[2015-02-24 11:50:00.0,2015-02-24 12:00:00.0]|13159|
...
|[2015-02-24 11:45:00.0,2015-02-24 11:55:00.0]|12021|
|[2015-02-23 10:40:00.0,2015-02-23 10:50:00.0]|7685 |
+---------------------------------------------+-----+



// COMMAND ----------

// in Scala
// dropDuplicates("User", "event_time")

import org.apache.spark.sql.functions.expr

withEventTime
  .withWatermark("event_time", "5 seconds")
  .dropDuplicates("User", "event_time")
  .groupBy("User")
  .count()
  .writeStream
  .queryName("deduplicated")
  .format("memory")
  .outputMode("complete")
  .start()



+----+-----+
|User|count|
+----+-----+
|   a| 8085|
|   b| 9123|
|   c| 7715|
|   g| 9167|
|   h| 7733|
|   e| 9891|
|   f| 9206|
|   d| 8124|
|   i| 9255|
+----+-----+


// COMMAND ----------
// mapGroupsWithState. 

// This is similar to a user-defined aggregation function that takes as input an update set of data and then resolves it down to a specific key with a set of values. 
// There are several things you’re going to need to define along the way:

// Three class definitions: 
// an input definition, a state definition, and optionally an output definition.
// A function to update the state based on a key, an iterator of events, and a previous state.
// A time-out parameter (as described in the time-outs section).



// working with sensor data, let’s 
// find the first and last timestamp that a given user performed one of the activities in the dataset.  

case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
case class UserState(user:String,
  var activity:String,
  var start:java.sql.Timestamp,
  var end:java.sql.Timestamp)


// COMMAND ----------
 // define the function to update the individual state based on a single input row. 

def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
  if (Option(input.timestamp).isEmpty) {
    return state
  }
  if (state.activity == input.activity) {

    if (input.timestamp.after(state.end)) {
      state.end = input.timestamp
    }
    if (input.timestamp.before(state.start)) {
      state.start = input.timestamp
    }
  } else {
    if (input.timestamp.after(state.end)) {
      state.start = input.timestamp
      state.end = input.timestamp
      state.activity = input.activity
    }
  }

  state
}


// COMMAND ----------

 // the function below defines the way state is updated based on an epoch of rows:

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}
def updateAcrossEvents(user:String,
  inputs: Iterator[InputRow],
  oldState: GroupState[UserState]):UserState = {
  var state:UserState = if (oldState.exists) oldState.get else UserState(user,
        "",
        new java.sql.Timestamp(6284160000000L),
        new java.sql.Timestamp(6284160L)
    )
  // we simply specify an old date that we can compare against and
  // immediately update based on the values in our data

  for (input <- inputs) {
    state = updateUserStateWithEvent(state, input)
    oldState.update(state)
  }
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout
withEventTime
  .selectExpr("User as user",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
  .as[InputRow]
  .groupByKey(_.user)
  .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("update")
  .start()


SELECT * 
FROM events_per_window 
order by user, start

+----+--------+--------------------+--------------------+
|user|activity|               start|                 end|
+----+--------+--------------------+--------------------+
|   a|    bike|2015-02-23 13:30:...|2015-02-23 14:06:...|
|   a|    bike|2015-02-23 13:30:...|2015-02-23 14:06:...|
...
|   d|    bike|2015-02-24 13:07:...|2015-02-24 13:42:...|
+----+--------+--------------------+--------------------+



// # Arbitrary Stateful Processing

// EXAMPLE: COUNT-BASED WINDOWS

// ## goal
// analyzes the activity dataset from this chapter and outputs the average reading
// of each device periodically, 
// creating a window based on the count of events and outputting it
// each time it has accumulated 500 events for that device.

// based on a number of events regardless of state and event times

case class InputRow(device: String, timestamp: java.sql.Timestamp, x: Double)
case class DeviceState(device: String, var values: Array[Double],
  var count: Int)
case class OutputRow(device: String, previousAverage: Double)


// COMMAND ----------

def updateWithEvent(state:DeviceState, input:InputRow):DeviceState = {
  state.count += 1
  // maintain an array of the x-axis values
  state.values = state.values ++ Array(input.x)
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode,
  GroupState}

def updateAcrossEvents(device:String, inputs: Iterator[InputRow],
  oldState: GroupState[DeviceState]):Iterator[OutputRow] = {
  inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
    val state = if (oldState.exists) oldState.get
      else DeviceState(device, Array(), 0)

    val newState = updateWithEvent(state, input)
    if (newState.count >= 500) {
      // One of our windows is complete; replace our state with an empty
      // DeviceState and output the average for the past 500 items from
      // the old state
      oldState.update(DeviceState(device, Array(), 0))
      Iterator(OutputRow(device,
        newState.values.sum / newState.values.length.toDouble))
    }
    else {
      // Update the current DeviceState object in place and output no
      // records
      oldState.update(newState)
      Iterator()
    }
  }
}


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

withEventTime
  .selectExpr("Device as device",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp", "x")
  .as[InputRow]
  .groupByKey(_.device)
  .flatMapGroupsWithState(OutputMode.Append,
    GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("count_based_device")
  .format("memory")
  .outputMode("append")
  .start()


sql("SELECT * FROM count_based_device")

+--------+--------------------+
|  device|     previousAverage|
+--------+--------------------+
|nexus4_1|      4.660034012E-4|
|nexus4_1|0.001436279298199...|
...
|nexus4_1|1.049804683999999...|
|nexus4_1|-0.01837188737960...|
+--------+--------------------+

// ## flatMapGroupsWithState

// ### what to do?
// In this case, you’re creating sessions on the fly from a user ID and some
// time information and if you see no new event from that user in five seconds, the session
// terminates. 


// EXAMPLE: SESSIONIZATION

case class InputRow(uid:String, timestamp:java.sql.Timestamp, x:Double,
  activity:String)
case class UserSession(val uid:String, var timestamp:java.sql.Timestamp,
  var activities: Array[String], var values: Array[Double])
case class UserSessionOutput(val uid:String, var activities: Array[String],
  var xAvg:Double)


// COMMAND ----------

def updateWithEvent(state:UserSession, input:InputRow):UserSession = {
  // handle malformed dates
  if (Option(input.timestamp).isEmpty) {
    return state
  }

  state.timestamp = input.timestamp
  state.values = state.values ++ Array(input.x)
  if (!state.activities.contains(input.activity)) {
    state.activities = state.activities ++ Array(input.activity)
  }
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode,
  GroupState}

def updateAcrossEvents(uid:String,
  inputs: Iterator[InputRow],
  oldState: GroupState[UserSession]):Iterator[UserSessionOutput] = {

  inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
    val state = if (oldState.exists) oldState.get else UserSession(
    uid,
    new java.sql.Timestamp(6284160000000L),
    Array(),
    Array())
    val newState = updateWithEvent(state, input)

    if (oldState.hasTimedOut) {
      val state = oldState.get
      oldState.remove()
      Iterator(UserSessionOutput(uid,
      state.activities,
      newState.values.sum / newState.values.length.toDouble))
    } else if (state.values.length > 1000) {
      val state = oldState.get
      oldState.remove()
      Iterator(UserSessionOutput(uid,
      state.activities,
      newState.values.sum / newState.values.length.toDouble))
    } else {
      oldState.update(newState)
      oldState.setTimeoutTimestamp(newState.timestamp.getTime(), "5 seconds")
      Iterator()
    }

  }
}


// COMMAND ----------
// flatMapGroupsWithState

import org.apache.spark.sql.streaming.GroupStateTimeout

withEventTime.where("x is not null")
  .selectExpr("user as uid",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp",
    "x", "gt as activity")
  .as[InputRow]
  .withWatermark("timestamp", "5 seconds")
  .groupByKey(_.uid)
  .flatMapGroupsWithState(OutputMode.Append,
    GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("count_based_device")
  .format("memory")
  .start()



sql("SELECT * FROM count_based_device")

+--------+--------------------+
|  device|     previousAverage|
+--------+--------------------+
|nexus4_1|      4.660034012E-4|
|nexus4_1|0.001436279298199...|
...
|nexus4_1|1.049804683999999...|
|nexus4_1|-0.01837188737960...|
+--------+--------------------+




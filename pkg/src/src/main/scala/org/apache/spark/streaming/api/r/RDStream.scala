/**
 * This a hack into streaming package since some of DStream's field and methods
 * are private to streaming. 
 */

package org.apache.spark.streaming.api.r

import edu.berkeley.cs.amplab.sparkr._
import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConversions._
import scala.language.existentials

import org.apache.spark.api.java._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

object RDStream {
  
  /**
   * helper function for DStream.foreachRDD().
   */
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], rfunc: Array[Byte], 
      deserializer: String) {
    val func = (rdd: RDD[_], time: Time) => {
      val res = callRTransform(List(Some(rdd)), time, rfunc, List(deserializer))
    }
    jdstream.dstream.foreachRDD(func)
  }
  
  def callRTransform(rdds: List[Option[RDD[_]]], time: Time, rfunc: Array[Byte], 
      deserializers: List[String]): Option[RDD[Array[Byte]]] = {
    try {
      val bos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(SparkRBackend.callbackSocket.getOutputStream())
      val dis = new DataInputStream(SparkRBackend.callbackSocket.getInputStream())
      
      writeString(dos, "callback")
      writeInt(dos, rdds.size)
      rdds.foreach(x => writeObject(dos, 
          x.map(JavaRDD.fromRDD(_).asInstanceOf[AnyRef]).orNull))
      writeDouble(dos, time.milliseconds.toDouble)
      writeBytes(dos, rfunc)
      deserializers.foreach(x => writeString(dos, x))     
      dos.flush()
      Option(readObject(dis).asInstanceOf[JavaRDD[Array[Byte]]]).map(_.rdd)
    } catch {
      case e: Exception =>
//        System.err.println("R transform callback failed with " + e)
//        e.printStackTrace()
        None
    }
  }
  
  /**
   * convert list of RDD into queue of RDDs, for ssc.queueStream()
   */
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.forall(queue.add(_))
    queue
  }
}

class RDStream(
    parent: DStream[_],
    rfunc: Array[Byte],
    deserializer: String) 
    extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration
  
  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.compute(validTime)
    if (rdd.isDefined) {
      RDStream.callRTransform(List(rdd), validTime, rfunc, List(deserializer))
    } else {
      None
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * Transformed from two DStreams in R.
 */
class RTransformed2DStream(
    parent: DStream[_],
    parent2: DStream[_],
    @transient rfunc: Array[Byte],
    deserializer: String,
    deserializer2: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent, parent2)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val empty: RDD[_] = ssc.sparkContext.emptyRDD
    val rdd1 = Some(parent.getOrCompute(validTime).getOrElse(empty))
    val rdd2 = Some(parent2.getOrCompute(validTime).getOrElse(empty))
    RDStream.callRTransform(List(rdd1, rdd2), validTime, rfunc, 
        List(deserializer, deserializer2))
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to ReducedWindowedDStream
 */
class RReducedWindowedDStream(
    parent: DStream[Array[Byte]],
    @transient rreduceFunc: Array[Byte],
    @transient rinvReduceFunc: Array[Byte],
    _windowDuration: Duration,
    _slideDuration: Duration,
    deserializer: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)
  
  override def dependencies: List[DStream[_]] = List(parent)

  override val mustCheckpoint: Boolean = true

  def windowDuration: Duration = _windowDuration

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val current = new Interval(currentTime - windowDuration, currentTime)
    val previous = current - slideDuration

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val previousRDD = getOrCompute(previous.endTime)

    // for small window, reduce once will be better than twice
    if (rinvReduceFunc != null && previousRDD.isDefined
        && windowDuration >= slideDuration * 5) {

      // subtract the values from old RDDs
      val oldRDDs = parent.slice(previous.beginTime + parent.slideDuration, current.beginTime)
      val subtracted = if (oldRDDs.size > 0) {
        RDStream.callRTransform(List(previousRDD, Some(ssc.sc.union(oldRDDs))), 
            validTime, rinvReduceFunc, List(deserializer, deserializer))
      } else {
        previousRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs = parent.slice(previous.endTime + parent.slideDuration, current.endTime)
      if (newRDDs.size > 0) {
        RDStream.callRTransform(List(subtracted, Some(ssc.sc.union(newRDDs))), 
            validTime, rreduceFunc, List(deserializer, deserializer))
      } else {
        subtracted
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs = parent.slice(current.beginTime + parent.slideDuration, current.endTime)
      if (currentRDDs.size > 0) {
        RDStream.callRTransform(List(None, Some(ssc.sc.union(currentRDDs))), 
            validTime, rreduceFunc, List(deserializer, deserializer))
      } else {
        None
      }
    }
  }
  
  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
class RStateDStream(
    parent: DStream[Array[Byte]],
    @transient reduceFunc: Array[Byte],
    deserializer: String)
  extends DStream[Array[Byte]](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)
  
  override def dependencies: List[DStream[_]] = List(parent)
  
  override def slideDuration: Duration = parent.slideDuration
  
  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      RDStream.callRTransform(List(lastState, rdd), validTime, reduceFunc, 
          List(deserializer, deserializer))
    } else {
      lastState
    }
  }
  
  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

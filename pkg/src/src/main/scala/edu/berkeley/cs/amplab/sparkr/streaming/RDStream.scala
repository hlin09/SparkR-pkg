package edu.berkeley.cs.amplab.sparkr.streaming

import edu.berkeley.cs.amplab.sparkr.JVMObjectTracker
import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConversions._

import org.apache.spark.api.java._
import org.apache.spark.rdd._
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

object RDStream {
  
  /**
   * helper function for DStream.foreachRDD().
   */
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], rfunc: Array[Byte]) {
    val func = (rdd: RDD[_], time: Time) => {
      val res = callRTransform(rdd, time, rfunc)
    }
    jdstream.dstream.foreachRDD(func)
  }
  
  def callRTransform(rdd: RDD[_], time: Time, rfunc: Array[Byte]): Option[RDD[Array[Byte]]] = {
    try {
      if (JVMObjectTracker.callbackSocket != null && 
          JVMObjectTracker.callbackSocket.isConnected) {
        val bos = new ByteArrayOutputStream()
        val dos = new DataOutputStream(JVMObjectTracker.callbackSocket.getOutputStream())
        val dis = new DataInputStream(JVMObjectTracker.callbackSocket.getInputStream())
        
        writeString(dos, "callback")
        writeObject(dos, JavaRDD.fromRDD(rdd).asInstanceOf[AnyRef])
        writeDouble(dos, time.milliseconds.toDouble)
        writeBytes(dos, rfunc)
        dos.flush()
        Option(readObject(dis).asInstanceOf[JavaRDD[Array[Byte]]]).map(_.rdd)
      } else {
        None
      }      
    } catch {
      case e: Exception =>
        JVMObjectTracker.callbackSocket.close()
        JVMObjectTracker.callbackSocket = null
        System.err.println("R Callback failed with " + e)
        e.printStackTrace()
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
    rfunc: Array[Byte]) 
    extends DStream[Array[Byte]] (JVMObjectTracker.streamingContext/*parent.ssc*/) {

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration
  
  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.compute(validTime)
    if (rdd.isDefined) {
      val func = (rdd: RDD[_], time: Time) => {
        RDStream.callRTransform(rdd, time, rfunc)
      }
      func(rdd.orNull, validTime)
    } else {
      None
    }
  }

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

package edu.berkeley.cs.amplab.sparkr.streaming

import edu.berkeley.cs.amplab.sparkr.JVMObjectTracker
import edu.berkeley.cs.amplab.sparkr.SerDe._

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

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
  
  def callRTransform(rdd: RDD[_], time: Time, rfunc: Array[Byte]): Option[RDD[_]] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(JVMObjectTracker.callbackSocket.getOutputStream())
    val dis = new DataInputStream(JVMObjectTracker.callbackSocket.getInputStream())
    
    writeString(dos, "callback")
    writeObject(dos, JavaRDD.fromRDD(rdd).asInstanceOf[AnyRef])
    writeDouble(dos, time.milliseconds.toDouble)
    writeBytes(dos, rfunc)
    dos.flush()
    Option(readObject(dis).asInstanceOf[JavaRDD[_]]).map(_.rdd)
  }
}

//class RDStream(
//    parent: DStream[_],
//    @transient rfunc: RTransformFunction) 
//    extends DStream[Array[Byte]] (parent.ssc) {
//
//  val func = new TransformFunction(pfunc)
//
//  override def dependencies = List(parent)
//
//  override def slideDuration: Duration = parent.slideDuration
//  
//  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
//    val rdd = parent.getOrCompute(validTime)
//    if (rdd.isDefined) {
//      func(rdd, validTime)
//    } else {
//      None
//    }
//  }
//
//  val asJavaDStream  = JavaDStream.fromDStream(this)
//}

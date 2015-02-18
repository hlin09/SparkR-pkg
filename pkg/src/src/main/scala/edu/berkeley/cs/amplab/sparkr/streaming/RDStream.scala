package edu.berkeley.cs.amplab.sparkr.streaming

import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

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

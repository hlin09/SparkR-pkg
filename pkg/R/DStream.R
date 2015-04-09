# Author: Hao Lin (haolin@purdue.edu).

# DStream in R implemented in S4 OO system.

setOldClass("jobj")

#' @title S4 class that represents an DStream
#' @description DStream can be created using functions like
#'              \code{queueStream}, \code{textFileStream} etc.
#' @rdname DStream
#' @seealso queueStream, textFileStream
#'
#' @slot env An R environment that stores bookkeeping states of the DStream
#' @slot jdstream Java object reference to the backing JavaDStream
#' @export
setClass("DStream",
         slots = list(env = "environment",
                      jdstream = "jobj"))

setClass("TransformedDStream",
         slots = list(prev = "DStream",
                      func = "function",
                      prev_jdstream = "jobj"),
         contains = "DStream")

setMethod("initialize", "DStream", function(.Object, jdstream, serializedMode,
                                            isCached, isCheckpointed) {
  # Check that DStream constructor is using the correct version of its RDD serializedMode.
  # See RDD for detailed descriptions of three serialized mode.
  stopifnot(class(serializedMode) == "character")
  stopifnot(serializedMode %in% c("byte", "string", "row"))
  
  # We use an environment to store mutable states inside an DStream object.
  # Note that R's call-by-value semantics makes modifying slots inside an
  # object (passed as an argument into a function, such as cache()) difficult:
  # i.e. one needs to make a copy of the DStream object and sets the new slot 
  # value there.
  
  # The slots are inheritable from superclass. Here, both `env' and `jdstream' are
  # inherited from DStream, but only the former is used.
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  .Object@env$serializedMode <- serializedMode
  
  .Object@jdstream <- jdstream
  .Object
})

setMethod("initialize", "TransformedDStream", function(.Object, prev, func, 
                                                       jdstream_val) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jdstream_val <- jdstream_val
  if (!is.null(jdstream_val)) {
    # This tracks the serialization mode for jdstream_val.
    .Object@env$serializedMode <- prev@env$serializedMode
  }
  
  # NOTE: We use prev_serialized to track if prev_jdstream is serialized
  # prev_serialized is used during the delayed computation of JDStream in getJDStream.
  .Object@prev <- prev
  
  isPipelinable <- function(dstream) {
    e <- dstream@env
    !(e$isCached || e$isCheckpointed)
  }
  
  if (!inherits(prev, "TransformedDStream") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- func
    .Object@prev_jdstream <- getJDStream(prev)
    .Object@env$prev_serializedMode <- prev@env$serializedMode
    # NOTE: We use prev_serializedMode to track the serialization mode of prev_jdstream.
    # prev_serializedMode is used during the delayed computation of JRDD in getJDStream.
  } else {
    pipelinedFunc <- function(time, rdds) {
      func(time, prev@func(time, rdds))
    }
    .Object@func <- pipelinedFunc
    .Object@prev_jdstream <- prev@prev_jdstream # maintain the pipeline
    # Get if the prev_jdstream was serialized from the parent DStream.
    .Object@env$prev_serializedMode <- prev@env$prev_serializedMode
  }
  
  .Object
})

#' @rdname DStream
#' @export
#'
#' @param jdstream Java object reference to the backing JavaDStream
#' @param serializedMode Use "byte" if the RDD in DStream stores data serialized in R, 
#'                       "string" if the RDD stores strings, and "row" if the RDD stores 
#'                       the rows of a DataFrame
#' @param isCached TRUE if the DStream is cached
#' @param isCheckpointed TRUE if the DStream has been checkpointed
DStream <- function(jdstream, serializedMode = "byte", isCached = FALSE,
                isCheckpointed = FALSE) {
  new("DStream", jdstream, serializedMode, isCached, isCheckpointed)
}

TransformedDStream <- function(prev, func) {
  new("TransformedDStream", prev, func, NULL)
}

# For normal DStreams we can directly read the serializedMode
setMethod("getSerializedMode", signature(x = "DStream"), function(x) x@env$serializedMode )
# For transfromed DStreams if jdstream_val is set then serializedMode should exist
# if not we return the defaultSerialization mode of "byte" as we don't know the serialization
# mode at this point in time.
setMethod("getSerializedMode", signature(x = "TransformedDStream"),
          function(x) {
            if (!is.null(x@env$jdstream_val)) {
              return(x@env$serializedMode)
            } else {
              return("byte")
            }
          })

# The jdstream accessor function.
setMethod("getJDStream", signature(dstream = "DStream"), function(dstream) dstream@jdstream )
setMethod("getJDStream", signature(dstream = "TransformedDStream"),
          function(dstream, serializedMode = "byte") {
            if (!is.null(dstream@env$jdstream_val)) {
              return(dstream@env$jdstream_val)
            }
            
            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(time, rdd) {
              dstream@func(time, rdd)
            }
            
            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL)
            
            serializedFuncArr <- serialize(computeFunc, connection = NULL)
            
            prev_jdstream <- dstream@prev_jdstream
            
            dstreamRef <- newJObject("org.apache.spark.streaming.api.r.RDStream",
                                     callJMethod(prev_jdstream, "dstream"),
                                     serializedFuncArr,
                                     dstream@env$prev_serializedMode
#                                      packageNamesArr,
#                                      as.character(.sparkREnv[["libname"]]),
#                                      callJMethod(prev_jdstream, "classTag")
            )
            # Save the serialization flag after we create a RDStream.
            dstream@env$serializedMode <- serializedMode
            dstream@env$jdstream_val <- callJMethod(dstreamRef, "asJavaDStream") 
            dstream@env$jdstream_val
          })

setValidity("DStream",
            function(object) {
              jdstream <- getJDSream(object)
              cls <- callJMethod(jdstream, "getClass")
              className <- callJMethod(cls, "getName")
              if (grep("spark.api.java.*DStream*", className) == 1) {
                TRUE
              } else {
                paste("Invalid DStream class ", className)
              }
            })

getSlideDurationMillisec <- function(dstream) {
  dstreamRef <- callJMethod(getJDStream(dstream), "dstream")
  duration <- callJMethod(callJMethod(dstreamRef, "slideDuration"), "milliseconds")
  as.integer(duration)
}

############ Actions and Transformations ############


#' Persist a DStream with the default storage level (MEMORY_ONLY).
#'
#' @param x A DStream to cache.
#' @rdname cache-methods
#' @aliases cache,DStream-method
setMethod("cache",
          signature(x = "DStream"),
          function(x) {
            callJMethod(getJDStream(x), "cache")
            x@env$isCached <- TRUE
            x
          })

#' Persist a DStream with the specified storage level. For details of the
#' supported storage levels, refer to
#' http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence.
#'
#' @param x A DStream to persist.
#' @param newLevel The new storage level to be assigned
#' @rdname persist
#' @aliases persist,DStream-method
setMethod("persist",
          signature(x = "DStream", newLevel = "character"),
          function(x, newLevel) {
            callJMethod(getJDStream(x), "persist", getStorageLevel(newLevel))
            x@env$isCached <- TRUE
            x
          })

#' Checkpoint a DStream by marking it for checkpointing. It will be saved to a 
#' file inside the checkpoint directory set with checkpoint(ssc, directory).
#'
#' @param x A DStream to checkpoint.
#' @param interval Time interval after which generated RDD will be checkpointed.
#' @rdname checkpoint-methods
#' @aliases checkpoint,DStream-method
setMethod("checkpoint",
          signature(x = "DStream"),
          function(x, interval) {
            jrdd <- getJDStream(x)
            callJMethod(jrdd, "checkpoint", 
                        newJObject("org.apache.spark.streaming.Duration", 
                                   as.integer(interval * 1000)))
            x@env$isCheckpointed <- TRUE
            x
          })

#' Counts elements in each RDD in a DStream.
#'
#' @param x A DStream to count
#' @return A new DStream in which each RDD has a single element
#'        generated by counting each RDD of this DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' print(count(dstream))
#'}
#' @rdname count
#' @aliases count,DStream-method
setMethod("count",
          signature(x = "DStream"),
          function(x) {
            countPartition <- function(part) {
              as.integer(length(part))
            }
            countPartStream <- mapPartitions(x, countPartition)
            reduce(countPartStream, "+")
          })

#' Applies a function to each element of a DStream.
#'
#' @param X A DStream to apply the transformation.
#' @param FUN A transformation to apply on each element of the DStream.
#' @return A new DStream by applying a function to each element of a DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' partitionSum <- map(dstream, function(x) { x * 2 })
#'}
#' @rdname map
#' @aliases map,DStream,function-method
setMethod("map",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            func <- function(split, iterator) {
              lapply(iterator, FUN)
            }
            mapPartitionsWithIndex(X, func)
          })

#' Applies a function to all elements of this DStream, and then flattening
#' the results.
#'
#' @param X A DStream to apply the transformation.
#' @param FUN A transformation to apply on each element of an DStream.
#' @return A new DStream by applying a flatMap function to all elements 
#'         of a DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' partitionSum <- flatMap(dstream, function(x) { list(x, x) })
#'}
#' @rdname flatMap
#' @aliases flatMap,DStream,function-method
setMethod("flatMap",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            transform(X, function(rdd) { flatMap(rdd, FUN) })
          })

#' Apply mapPartitions() to each RDDs of this DStream.
#'
#' @param X A DStream to apply the transformation.
#' @param FUN A transformation to apply on each partition of an RDD.
#' @return A new DStream in which each RDD is generated by applying
#'         mapPartitions() to each RDDs of this DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' partitionSum <- mapPartitions(dstream, function(part) { Reduce("+", part) })
#'}
#' @rdname mapPartitionsWithIndex
#' @aliases mapPartitions,DStream,function-method
setMethod("mapPartitions",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            mapPartitionsWithIndex(X, function(s, part) { FUN(part) })
          })

#' Applies a function to each partition of the RDDs in a given DStream, while 
#' tracking the index of the original partition.
#'
#' @param X A DStream to apply the transformation.
#' @param FUN A transformation to apply on each partition of an RDD; takes the 
#'        partition index and a list of elements in the particular partition.
#' @return A new DStream by applying a function to each partition of the RDDs in
#'         a given DStream, while tracking the index of the original partition.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' prod <- mapPartitionsWithIndex(rdd, function(split, part) {
#'                                          split * Reduce("+", part) })
#'}
#' @rdname mapPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,DStream,function-method
setMethod("mapPartitionsWithIndex",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            transform(X, function(rdd) {
              mapPartitionsWithIndex(rdd, FUN)
            })
          })

#' Return a new DStream containing only the elements that satisfy predicate.
#'
#' @param x A DStream to be filtered.
#' @param f A unary predicate function.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' print(filterRDD(rdd, function (x) { x < 3 })) # c(1, 2)
#'}
#' @rdname filterRDD
#' @aliases filterRDD,DStream,function-method
setMethod("filterRDD",
          signature(x = "DStream", f = "function"),
          function(x, f) {
            filter.func <- function(part) {
              Filter(f, part)
            }
            mapPartitions(x, filter.func)
          })

#' Reduces across elements of the RDDs in a DStream.
#'
#' @param x A DStream to reduce its RDDs.
#' @param func A commutative and associative function to apply on elements
#'             of the RDDs in a DStream.
#' @return A new DStream in which each RDD has a single element generated by 
#'         reducing each RDD of this DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' print(reduce(dstream, "+"))
#'}
#' @rdname reduce
#' @aliases reduce,DStream,ANY-method
setMethod("reduce",
          signature(x = "DStream", func = "ANY"),
          function(x, func) {
            pairedStream <- map(x, function(x) { list(1L, x) })
            map(reduceByKey(pairedStream, func, 1L), function(x) { x[[2]] })
          })

#' Applies reduceByKey to each RDD in a DStream.
#'
#' This function operates on RDDs where every element is of the form list(K, V) or c(K, V).
#' and merges the values for each key using an associative reduce function.
#'
#' @param x A DStream whose RDDs to apply the combineByKey.
#' @param combineFunc An associative reduce function to use.
#' @param numPartitions Number of partitions to create.
#' @return A new DStream by applying reduceByKey to each RDD.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(list(1, 1), list(2, 2), list(1, 3)))
#' print(reduceByKey(dstream, "+", 2L))
#'}
#' @rdname reduceByKey
#' @aliases reduceByKey,DStream,integer-method
setMethod("reduceByKey",
          signature(x = "DStream", combineFunc = "ANY", numPartitions = "integer"),
          function(x, combineFunc, numPartitions) {
            transform(x, function(rdd) {
              reduceByKey(rdd, combineFunc, numPartitions)
            })
          })

#' Applies combineByKey to each RDD in a DStream.
#'
#' @param x A DStream to apply the combineByKey.
#' @param createCombiner Create a combiner (C) given a value (V)
#' @param mergeValue Merge the given value (V) with an existing combiner (C)
#' @param mergeCombiners Merge two combiners and return a new combiner
#' @param numPartitions Number of partitions to create.
#' @return A new DStream by applying combineByKey to each RDD.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(list(1, 1), list(2, 2), list(1, 3)))
#' prod <- combineByKey(rdd, function(x) { x }, "+", "+", 2L)
#'}
#' @rdname combineByKey
#' @aliases combineByKey,DStream,function-method
setMethod("combineByKey",
          signature(x = "DStream", createCombiner = "ANY", mergeValue = "ANY",
                    mergeCombiners = "ANY", numPartitions = "integer"),
          function(x, createCombiner, mergeValue, mergeCombiners, numPartitions) {
            transform(x, function(rdd) {
              combineByKey(rdd, createCombiner, mergeValue, mergeCombiners, 
                           numPartitions)
            })
          })

#' Partitions RDDs in a DStream by key.
#'
#' @param x A DStream to partition.
#' @param numPartitions Number of partitions to create.
#' @param partitionFunc A partition function to use. Uses a default hashCode
#'                      function if not provided
#' @return A copy of the DStream in which each RDD are partitioned using the 
#'         specified partitioner.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(list(1, 1), list(2, 2), list(1, 3)))
#' print(partitionBy(dstream, 2L))
#'}
#' @rdname partitionBy
#' @aliases partitionBy,DStream,integer-method
setMethod("partitionBy",
          signature(x = "DStream", numPartitions = "integer"),
          function(x, numPartitions, partitionFunc = hashCode) {
            transform(x, function(rdd) { 
              partitionBy(rdd, numPartitions, partitionFunc)
            })
          })

#' Groups values by key
#'
#' @param x A DStream to group. Should have RDDs of which each element is
#'          list(K, V) or c(K, V).
#' @param numPartitions Number of partitions to create.
#' @return A new DStream by applying groupByKey on each RDD.
#' @seealso reduceByKey
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(list(1, 1), list(2, 2), list(1, 3)))
#' print(groupBy(dstream, 2L))
#'}
#' @rdname groupByKey
#' @aliases groupByKey,DStream,integer-method
setMethod("groupByKey",
          signature(x = "DStream", numPartitions = "integer"),
          function(x, numPartitions) {
            transform(x, function(rdd) { groupByKey(rdd, numPartitions) })
          })

#' Applies a function to all values of elements of a DStream, without modifying the keys.
#'
#' @param X A DStream to apply the transformation.
#' @param FUN the transformation to apply on the value of each element in DStream 'X'.
#' @return A new DStream by applying a map function to the value of each key-value 
#'         pairs in this DStream without changing the key.
#' @rdname mapValues
#' @aliases mapValues,DStream,function-method
setMethod("mapValues",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            func <- function(x) {
              list(x[[1]], FUN(x[[2]]))
            }
            map(X, func)
          })

#' Saves each RDD in this DStream as at text file, using string representation 
#' of elements.
#'
#' @param x A DStream to save.
#' @param prefix, suffix Character vectors. The file name at each batch interval 
#'                       is generated as: "prefix-TIME_IN_MS.suffix".
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' saveAsTextFile(dstream, "QueueStream", "txt")
#'}
#' @rdname saveAsTextFile
#' @aliases saveAsTextFile,DStream
setMethod("saveAsTextFile",
          signature(x = "DStream"),
          function(x, prefix, suffix = "") {
            func <- function(time, rdd) {
              path <- rddToFileName(prefix, suffix, time)
              saveAsTextFile(rdd, path)
            }
            foreachRDD(x, func)
          })

#' Unifies two DStreams.
#' The same as union() in Spark streaming.
#'
#' @param x A DStream.
#' @param y Another DStream having the same interval (i.e., slideDuration) as 'x'.
#' @return a new DStream by unifying data of another DStream with this DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' dstream2 <- queueStream(ssc, list(11:20))
#' print(unionRDD(dstream, dstream2))
#'}
#' @rdname unionRDD
#' @aliases unionRDD,DStream-method
setMethod("unionRDD",
          signature(x = "DStream", y = "DStream"),
          function(x, y) {
            if (getSlideDurationMillisec(x) != getSlideDurationMillisec(y)) {
              stop("Two DStreams should have same slide durations.")
            }
            transformWith(x, function(rdd1, rdd2) { unionRDD(rdd1, rdd2) }, y)
          })

#' Applies 'cogroup' to RDDs in several DStreams.
#'
#' @param x A DStreams.
#' @param y Another DStream
#' @param numPartitions Number of partitions to create.
#' @return a new DStream by applying 'cogroup' between RDDs of this DStream 
#'         and `other` DStream.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(list(1, 1), list(2, 4)))
#' dstream2 <- queueStream(ssc, list(list(1, 2), list(1, 3)))
#' print(cogroup(dstream, dstream2))
#'}
#' @rdname cogroup
#' @aliases cogroup,DStream-method
setMethod("cogroup",
          "DStream",
          function(x, y, numPartitions) {
            transformWith(x, function(rdd1, rdd2) { 
              cogroup(rdd1, rdd2, numPartitions = numPartitions) 
            }, y)
          })

#' Joins RDDs in two DStreams.
#'
#' @param x A DStream whose RDDs to be joined.
#' @param y Another DStream whose RDDs to be joined.
#' @param numPartitions Number of partitions to create.
#' @return a new DStream by applying 'join' between RDDs of 'x' and 'y'.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream1 <- queueStream(ssc, list(list(list(1, 1), list(2, 4))))
#' dstream2 <- queueStream(ssc, list(list(list(1, 2), list(1, 3))))
#' join(dstream1, dstream2, 2L) # list(list(1, list(1, 2)), list(1, list(1, 3))
#'}
#' @rdname join-methods
#' @aliases join,DStream,DStream-method
setMethod("join",
          signature(x = "DStream", y = "DStream"),
          function(x, y, numPartitions) {
            transformWith(x, function(rdd1, rdd2) { 
              join(rdd1, rdd2, numPartitions = numPartitions)
            }, y)
          })

#' Left-outer-joins RDDs in two DStreams.
#'
#' @param x A DStream whose RDDs to be joined. Should be an RDD where each element is
#'          list(K, V).
#' @param y Another DStream whose RDD to be joined. Should be an RDD where each element 
#'          is list(K, V).
#' @param numPartitions Number of partitions to create.
#' @return A new DStream by applying 'left outer join' between RDDs of DStream 'x'
#'         and 'y'.
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream1 <- queueStream(ssc, list(list(list(1, 1), list(2, 4))))
#' dstream2 <- queueStream(ssc, list(list(list(1, 2), list(1, 3))))
#' leftOuterJoin(dstream1, dstream2, 2L)
#' # list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
#'}
#' @rdname join-methods
#' @aliases leftOuterJoin,RDD,RDD-method
setMethod("leftOuterJoin",
          signature(x = "DStream", y = "DStream", numPartitions = "integer"),
          function(x, y, numPartitions) {
            transformWith(x, function(rdd1, rdd2) { 
              leftOuterJoin(rdd1, rdd2, numPartitions = numPartitions)
            }, y)
          })

#' Applies a function to all RDDs in an DStream, and force evaluation.
#'
#' @param dstream A DStream to apply the function.
#' @param func A function to be applied, which can have one argument of `rdd`,
#'        or have two arguments of (`time`, `rdd`).
#' @return invisible NULL.
#' @export
#' @rdname foreachRDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' foreachRDD(dstream, function(x) { save(x, file = ...) })
#'}
setGeneric("foreachRDD", 
           function(dstream, func) { standardGeneric("foreachRDD") })

#' @rdname foreachRDD
#' @aliases foreach,DStream,function-method
setMethod("foreachRDD",
          signature(dstream = "DStream", func = "function"),
          function(dstream, func) {
            if (length(as.list(args(func))) - 1 == 1) {  # "func" has only one param.
              oldFunc <- func
              func <- function(time, rdd) {
                oldFunc(rdd)
              }
            }
            invisible(
              callJStatic("org.apache.spark.streaming.api.r.RDStream", 
                          "callForeachRDD", 
                          getJDStream(dstream), 
                          serialize(SparkR:::cleanClosure(func), connection = NULL),
                          getSerializedMode(dstream))
            )
          })

#' Prints the first num elements of each RDD generated in this DStream.
#' 
#' @param num The number of elements from the first will be printed.
#' @export
#' @rdname print
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' print(dstream, 10L)
#'}
#' @rdname print
#' @aliases print,DStream-method
setMethod("print",
          signature(x = "DStream"),
          function(x, num = 10) {
            func <- function(time, rdd) {
              taken <- take(rdd, num + 1)
              cat("-------------------------------------------\n")
              cat("Time: ", time, "\n")
              cat("-------------------------------------------\n")
              if (length(taken) > 0) {
                cat(paste(taken[1:length(taken)], collapse = "\n"))
              }
              if (length(taken) > num) {
                cat("...\n")
              } else {
                cat("\n")
              }
            }
            foreachRDD(x, func)
          })

#' Returns a new DStream in which each RDD is generated by applying a function
#' on each RDD of this DStream.
#'
#' @param dstream A DStream to apply the function.
#' @param func A function to be applied, which can have one argument of 'rdd',
#'             or have two arguments of (time, rdd).
#' @return a new transformed DStream.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' transform(dstream, function(t, rdd) { ... })
#'}
#' @rdname transform
#' @aliases transform,DStream,function-method
transform <- function(`_data`, ...) { UseMethod("transform", `_data`) }
transform.default <- base::transform
transform.DStream <- function(`_data`, func) {
  if (length(as.list(args(func))) - 1 == 1) {  # "func" has only one param.
    oldFunc <- func
    func <- function(time, rdd) {
      oldFunc(rdd)
    }
  }
  TransformedDStream(`_data`, func)
}

#' Transforms DStream with another DStream.
#'
#' @param dstream A DStream to apply the function.
#' @param func A function to be applied, which can have two argument of 
#'             (rdd1, rdd2), or have three arguments of (time, rdd1, rdd2).
#' @param other Another DStream.
#' @return a new DStream in which each RDD is generated by applying a function
#'         on each RDD of this DStream and 'other' DStream.
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.streaming.init(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' dstream2 <- queueStream(ssc, list(11:20))
#' transformWith(dstream, function(rdd1, rdd2) { ... }, dstream2)
#'}
#' @rdname transformWith
#' @aliases transformWith,DStream,function-method
setMethod("transformWith",
          signature(x = "DStream", func = "function", other = "DStream"),
          function(x, func, other) {
            if (length(as.list(args(func))) - 1 == 2) {  # "func" has only one param.
              oldFunc <- func
              func <- function(time, rdd1, rdd2) {
                oldFunc(rdd1, rdd2)
              }
            }
            dstreamRef <- newJObject("org.apache.spark.streaming.api.r.RTransformed2DStream",
                                     callJMethod(getJDStream(x), "dstream"),
                                     callJMethod(getJDStream(other), "dstream"),
                                     serialize(SparkR:::cleanClosure(func), connection = NULL),
                                     getSerializedMode(x),
                                     getSerializedMode(other)
            )
            DStream(callJMethod(dstreamRef, "asJavaDStream"))
          })

#' Returns all the RDDs in a time slice.
#'
#' @param x A DStream to apply the function.
#' @param begin The beginning timestamp of the slice.
#' @param end The ending timestamp of the slice.
#' @return a list of RDDs between 'begin' to 'end' (both included).
#' @export
#' @rdname slice
#' @aliases slice,DStream,function-method
setMethod("slice",
          signature(x = "DStream"),
          function(x, begin, end) {
            jrdds <- callJMethod(getJDStream(x), "slice", begin, end)
            resLen <- callJMethod(jrdds, "size")
            res <- lapply(0:(resLen - 1), function(index) {
              RDD(callJMethod(jrdds, "apply", index))
            })
            res
          })

validateWindowTime <- function(dstream, window, slide) {
  duration <- getSlideDurationMillisec(dstream)
  if (as.integer(window * 1000) %% duration != 0) {
    stop(paste("windowDuration must be multiple of the slide duration of", duration, "ms."))
  }
  if (!is.null(slide) && as.integer(slide * 1000) %% duration != 0) {
    stop(paste("slideDuration must be multiple of the slide duration of", duration, "ms."))
  }
}

#' Creates windowed DStream.
#' Same as window in Spark Streaming.
#'
#' @param x A DStream to apply the function.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @return a new DStream in which each RDD contains all the elements in seen in a
#'         sliding window of time over this DStream.
#' @export
#' @rdname windowDStream
#' @aliases windowDStream,DStream-method
setMethod("windowDStream",
          signature(x = "DStream"),
          function(x, windowDuration, slideDuration = NULL) {
            validateWindowTime(x, windowDuration, slideDuration)
            jwd <- newJObject("org.apache.spark.streaming.Duration", 
                              as.integer(windowDuration * 1000))
            if (is.null(slideDuration)) {
              return(DStream(callJMethod(getJDStream(x), "window", jwd), 
                             getSerializedMode(x)))
            }
            jsd <- newJObject("org.apache.spark.streaming.Duration", 
                              as.integer(slideDuration * 1000))
            DStream(callJMethod(getJDStream(x), "window", jwd, jsd), getSerializedMode(x))
          })

#' Reduces incrementally in DStream over a sliding window.
#'
#' @param x A DStream to apply the function.
#' @param reduceFunc An associative reduce function.
#' @param invReduceFunc The inverse function of 'reduceFunc'.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @return A new DStream in which each RDD has a single element generated by 
#'         reducing all elements in a sliding window over this DStream.
#' @export
#' @rdname window-method
#' @aliases reduceByWindow,DStream,function-method
setMethod("reduceByWindow",
          signature(x = "DStream"),
          function(x, reduceFunc, invReduceFunc = NULL, 
                   windowDuration, slideDuration = NULL) {
            keyed <- map(x, function(e) { list(1, e) })
            reduced <- reduceByKeyAndWindow(keyed, reduceFunc, invReduceFunc,
                                            windowDuration, slideDuration, 1L)
            map(reduced, function(pair) { pair[[2]] })
          })

#' Counts elements incrementally in DStream over a sliding window.
#' This is equivalent to window(windowDuration, slideDuration).count(), but 
#' will be more efficient if window is large.
#'
#' @param x A DStream.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @return A new DStream in which each RDD has a single element generated by 
#'         counting the number of elements in a window over this DStream.
#'         windowDuration and slideDuration are as defined in the window() operation.
#' @export
#' @rdname window-method
#' @aliases countByWindow,DStream,function-method
setMethod("countByWindow",
          signature(x = "DStream"),
          function(x, windowDuration, slideDuration = NULL) {
            reduceByWindow(map(x, function(e) { 1L }), "+", "-", 
                           windowDuration, slideDuration)
          })

#' Counts distinct elements incrementally in DStream over a sliding window.
#'
#' @param x A DStream.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @param numPartitions The number of partitions of each RDD in the new DStream.
#' @return A new DStream in which each RDD contains the count of distinct elements
#'         in RDDs in a sliding window over this DStream..
#' @export
#' @rdname window-method
#' @aliases countByValueAndWindow,DStream,function-method
setMethod("countByValueAndWindow",
          signature(x = "DStream"),
          function(x, windowDuration, slideDuration = NULL, numPartitions = 1L) {
            counted <- reduceByKeyAndWindow(map(x, function(k) { list(k, 1L) }), 
                                            "+", "-", windowDuration, slideDuration, 
                                            numPartitions)
            count(filterRDD(counted, function(pair) { pair[[2]] > 0} ))
          })

#' Counts distinct elements incrementally in DStream over a sliding window.
#'
#' @param x A DStream.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @param numPartitions The number of partitions of each RDD in the new DStream.
#' @return A new DStream in which each RDD contains the count of distinct elements
#'         in RDDs in a sliding window over this DStream..
#' @export
#' @rdname window-method
#' @aliases groupByKeyAndWindow,DStream,function-method
setMethod("groupByKeyAndWindow",
          signature(x = "DStream"),
          function(x, windowDuration, slideDuration = NULL, numPartitions = 1L) {
            listed <- mapValues(x, function(v) { list(v) })
            counted <- reduceByKeyAndWindow(
              listed, c, function(prev, old) {
                tail(prev, n = (length(prev) - length(old)))
              },
              windowDuration, slideDuration, numPartitions)
            counted
          })

#' ReduceByKey incrementally in DStream over a sliding window.
#'
#' @param x A DStream.
#' @param reduceFunc An associative reduce function.
#' @param invReduceFunc The inverse function of 'reduceFunc'.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @param numPartitions The number of partitions of each RDD in the new DStream.
#' @param filterFunc The function to filter expired key-value pairs; 
#'                   only pairs that satisfy the function are retained;
#'                   set this to null if you do not want to filter.
#' @return A new DStream by applying incremental `reduceByKey` over a sliding window.
#' @export
#' @rdname window-method
#' @aliases reduceByKeyAndWindow,DStream,function-method
setMethod("reduceByKeyAndWindow",
          signature(x = "DStream"),
          function(x, reduceFunc, invReduceFunc = NULL, windowDuration, 
                   slideDuration = NULL, numPartitions = 1L, filterFunc = NULL) {
            validateWindowTime(x, windowDuration, slideDuration)
            reduced <- reduceByKey(x, reduceFunc, numPartitions)
            func <- function(time, substractedRDD, newRDD) {
              reducedNewRDD <- reduceByKey(newRDD, reduceFunc, numPartitions)
              if (!is.null(substractedRDD)) {
                res <- reduceByKey(unionRDD(substractedRDD, reducedNewRDD), 
                                   reduceFunc, numPartitions)
              } else {
                res <- reducedNewRDD
              }
              if (!is.null(filterFunc)) {
                res <- filterRDD(res, filterFunc)
              }
              res
            }
            jwd <- newJObject("org.apache.spark.streaming.Duration", 
                              as.integer(windowDuration * 1000))
            if (is.null(slideDuration)) {
              jsd <- newJObject("org.apache.spark.streaming.Duration", 
                                getSlideDurationMillisec(x))
            } else {
              jsd <- newJObject("org.apache.spark.streaming.Duration", 
                                as.integer(slideDuration * 1000))
            }
            if (is.null(invReduceFunc)) {
              dstreamRef <- newJObject("org.apache.spark.streaming.api.r.RReducedWindowedDStream",
                                       callJMethod(getJDStream(reduced), "dstream"),
                                       serialize(SparkR:::cleanClosure(func), connection = NULL),
                                       NULL,
                                       jwd,
                                       jsd,
                                       getSerializedMode(reduced)            
              )
            } else {
              invReduceFunc <- match.fun(invReduceFunc)
              invFunc <- function(time, previousRDD, oldRDD) {
                reducedOldRDD <- reduceByKey(oldRDD, reduceFunc, numPartitions)
                joined <- leftOuterJoin(previousRDD, reducedOldRDD, numPartitions)
                mapValues(joined, function(value) {
                  if (is.null(value[[2]])) {
                    value[[1]]
                  } else {
                    invReduceFunc(value[[1]], value[[2]])
                  }
                })
              }
              dstreamRef <- newJObject("org.apache.spark.streaming.api.r.RReducedWindowedDStream",
                                       callJMethod(getJDStream(reduced), "dstream"),
                                       serialize(SparkR:::cleanClosure(func), connection = NULL),
                                       serialize(SparkR:::cleanClosure(invFunc), connection = NULL),
                                       jwd,
                                       jsd,
                                       getSerializedMode(reduced)            
              )
            }
            
            DStream(callJMethod(dstreamRef, "asJavaDStream"))
          })

#' updateStateByKey incrementally in DStream.
#'
#' @param x A DStream.
#' @param reduceFunc An associative reduce function.
#' @param invReduceFunc The inverse function of 'reduceFunc'.
#' @param windowDuration The width of the window; must be a multiple of this 
#'                       DStream's batching interval.
#' @param slideDuration The sliding interval of the window (i.e., the interval 
#'                      after which the new DStream will generate RDDs); must be 
#'                      a multiple of the batching interval of DStream 'x'.
#' @param numPartitions The number of partitions of each RDD in the new DStream.
#' @param filterFunc The function to filter expired key-value pairs; 
#'                   only pairs that satisfy the function are retained;
#'                   set this to null if you do not want to filter.
#' @return A new DStream by applying incremental `reduceByKey` over a sliding window.
#' @export
#' @rdname updateStateByKey
#' @aliases updateStateByKey,DStream,function-method
setMethod("updateStateByKey",
          signature(x = "DStream"),
          function(x, updateFunc, numPartitions = 1L) {
            reduceFunc <- function(time, stateRDD, newRDD) {
              g <- if (is.null(stateRDD)) {
                mapValues(groupByKey(newRDD, numPartitions), function(vList) {
                  list(vList, NULL)
                })
              } else {
                mapValues(cogroup(stateRDD, newRDD, numPartitions = numPartitions), 
                          function(vPair) {
                            if (length(vPair[[1]] > 0)) {
                              list(vPair[[2]], vPair[[1]][[1]])
                            } else {
                              list(vPair[[2]], NULL)
                            }
                          })
              }
              state <- mapValues(g, function(vPair) {
                updateFunc(vPair[[1]], vPair[[2]])
              })
              filterRDD(state, function(s) { !is.null(s[[2]]) })
            }
            dstreamRef <- newJObject("org.apache.spark.streaming.api.r.RStateDStream",
                                     callJMethod(getJDStream(x), "dstream"),
                                     serialize(SparkR:::cleanClosure(reduceFunc), connection = NULL),
                                     getSerializedMode(x)            
            )
            DStream(callJMethod(dstreamRef, "asJavaDStream"))
          })

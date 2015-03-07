# DStream in R implemented in S4 OO system.

setOldClass("jobj")

#' @title S4 class that represents an DStream
#' @description DStream can be created using functions like
#'              \code{textFileStream} etc.
#' @rdname DStream
#' @seealso textFileStream
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

setMethod("initialize", "DStream", function(.Object, jdstream, serialized,
                                            isCached, isCheckpointed) {
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
  .Object@env$serialized <- serialized
  
  .Object@jdstream <- jdstream
  .Object
})

setMethod("initialize", "TransformedDStream", function(.Object, prev, func, 
                                                       jdstream_val) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jdstream_val <- jdstream_val
  # This tracks if jrdd_val is serialized
  .Object@env$serialized <- prev@env$serialized
  
  # NOTE: We use prev_serialized to track if prev_jrdd is serialized
  # prev_serialized is used during the delayed computation of JRDD in getJRDD
  .Object@prev <- prev
  
  isPipelinable <- function(dstream) {
    e <- dstream@env
    !(e$isCached || e$isCheckpointed)
  }
  
  if (!inherits(prev, "TransformedDStream") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- func
    .Object@prev_jdstream <- getJDStream(prev)
    # Since this is the first step in the pipeline, the prev_serialized
    # is same as serialized here.
    .Object@env$prev_serialized <- .Object@env$serialized
  } else {
    pipelinedFunc <- function(split, iterator) {
      func(split, prev@func(split, iterator))
    }
    .Object@func <- pipelinedFunc
    .Object@prev_jdstream <- prev@prev_jdstream # maintain the pipeline
    # Get if the prev_jrdd was serialized from the parent RDD
    .Object@env$prev_serialized <- prev@env$prev_serialized
  }
  
  .Object
})

#' @rdname DStream
#' @export
#'
#' @param jdstream Java object reference to the backing JavaDStream
#' @param serialized TRUE if the DStream stores data serialized in R
#' @param isCached TRUE if the DStream is cached
#' @param isCheckpointed TRUE if the DStream has been checkpointed
DStream <- function(jdstream, serialized = TRUE, isCached = FALSE,
                isCheckpointed = FALSE) {
  new("DStream", jdstream, serialized, isCached, isCheckpointed)
}

TransformedDStream <- function(prev, func) {
  new("TransformedDStream", prev, func, NULL)
}

# The jrdd accessor function.
setGeneric("getJDStream", function(dstream, ...) { standardGeneric("getJDStream") })
setMethod("getJDStream", signature(dstream = "DStream"), function(dstream) dstream@jdstream )
setMethod("getJDStream", signature(dstream = "TransformedDStream"),
          function(dstream, dataSerialization = TRUE) {
            if (!is.null(dstream@env$jdstream_val)) {
              return(dstream@env$jdstream_val)
            }
            
            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(split, part) {
              dstream@func(split, part)
            }
            serializedFuncArr <- serialize("computeFunc", connection = NULL,
                                           ascii = TRUE)
            
            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL,
                                         ascii = TRUE)
            
            depsBin <- getDependencies(computeFunc)
            
            prev_jrdd <- dstream@prev_jrdd
            
            if (dataSerialization) {
              dstreamRef <- newJObject("edu.berkeley.cs.amplab.sparkr.RDStream",
                                   callJMethod(prev_jdstream, "dstream"),
                                   serializedFuncArr,
                                   dstream@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   callJMethod(prev_jdstream, "classTag"))
            } else {
              dstreamRef <- newJObject("edu.berkeley.cs.amplab.sparkr.StringRRDD",
                                   callJMethod(prev_jdstream, "dstream"),
                                   serializedFuncArr,
                                   dstream@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   callJMethod(prev_jdstream, "classTag"))
            }
            # Save the serialization flag after we create a RRDD
            dstream@env$serialized <- dataSerialization
            # dstreamRef$asJavaDStream()
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


############ Actions and Transformations ############


#' Return a new DStream by applying a function to each partition of the RDDs in
#' a given DStream, while tracking the index of the original partition.
#'
#' @param X The DStream to apply the transformation.
#' @param FUN the transformation to apply on each partition of an RDD; takes the 
#'        partition index and a list of elements in the particular partition.
#' @return a new DStream created by the transformation.
#' @rdname mapPartitionsWithIndex
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10, 5L)
#' prod <- mapPartitionsWithIndex(rdd, function(split, part) {
#'                                          split * Reduce("+", part) })
#' collect(prod, flatten = FALSE) # 0, 7, 22, 45, 76
#'}
# setGeneric("mapPartitionsWithIndex", function(X, FUN) {
#   standardGeneric("mapPartitionsWithIndex") })

#' @rdname mapPartitionsWithIndex
#' @aliases mapPartitionsWithIndex,DStream,function-method
setMethod("mapPartitionsWithIndex",
          signature(X = "DStream", FUN = "function"),
          function(X, FUN) {
            transform(X, function(rdd) {
              mapPartitionsWithIndex(rdd, FUN)
            })
          })

mapPartitionsWithIndex.DStream <- function(X, FUN) {
  transform(X, function(rdd) {
    mapPartitionsWithIndex(rdd, FUN)
  })
}

#' Applies a function to all RDDs in an DStream, and force evaluation.
#'
#' @param dstream The DStream to apply the function.
#' @param func The function to be applied, which can have one argument of `rdd`,
#'        or have two arguments of (`time`, `rdd`).
#' @return invisible NULL.
#' @export
#' @rdname foreachRDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.init.streaming(sc, 2L)
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
              callJStatic("edu.berkeley.cs.amplab.sparkr.streaming.RDStream", 
                          "callForeachRDD", 
                          dstream@jdstream, serialize(func, connection = NULL))
            )
          })

#' Print the first num elements of each RDD generated in this DStream.
#' 
#' @param num The number of elements from the first will be printed.
#' @export
#' @rdname foreachRDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' ssc <- sparkR.init.streaming(sc, 2L)
#' dstream <- queueStream(ssc, list(1:10))
#' print(dstream, 10L)
#'}
setGeneric("print", function(dstream, num = 10) {
  standardGeneric("print") 
})

#' @rdname print
#' @aliases print,DStream-method
setMethod("print",
          signature(dstream = "DStream", num = "numeric"),
          function(dstream, num = 10) {
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
              }
            }
            foreachRDD(dstream, func)
          })

#' Return a new DStream in which each RDD is generated by applying a function
#' on each RDD of this DStream.
#'
#' @param dstream The The DStream to apply the function.
#' @param func The function to be applied, which can have one argument of `rdd`,
#' or have two arguments of (`time`, `rdd`).
#' @return a new transformed DStream.
#' @export
#' @rdname transform
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' transform(dstream, function(x) { save(x, file=...) })
#'}
setGeneric("transform", 
           function(dstream, func) { standardGeneric("transform") })

#' @rdname transform
#' @aliases transform,DStream,function-method
setMethod("transform",
          signature(dstream = "DStream", func = "function"),
          function(dstream, func) {
            if (length(as.list(args(func))) - 1 == 1) {  # "func" has only one param.
              oldFunc <- func
              func <- function(time, rdd) {
                oldFunc(rdd)
              }
            }
            TransformedDStream(dstream, func)
          })

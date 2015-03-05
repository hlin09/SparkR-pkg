# DStream in R implemented in S4 OO system.

#' @title S4 class that represents an DStream
#' @description DStream can be created using functions like
#'              \code{textFileStream} etc.
#' @rdname DStream
#' @seealso textFileStream
#'
#' @slot env An R environment that stores bookkeeping states of the DStream
#' @slot jds Java object reference to the backing JavaDStream
#' @export
setClass("DStream",
         slots = list(env = "environment",
                      jds = "jobj"))

setClass("TransformedDStream",
         slots = list(prev = "DStream",
                      func = "function",
                      prev_jds = "jobj"),
         contains = "DStream")

setMethod("initialize", "DStream", function(.Object, jds, serialized,
                                            isCached, isCheckpointed) {
  # We use an environment to store mutable states inside an DStream object.
  # Note that R's call-by-value semantics makes modifying slots inside an
  # object (passed as an argument into a function, such as cache()) difficult:
  # i.e. one needs to make a copy of the DStream object and sets the new slot 
  # value there.
  
  # The slots are inheritable from superclass. Here, both `env' and `jds' are
  # inherited from DStream, but only the former is used.
  .Object@env <- new.env()
  .Object@env$isCached <- isCached
  .Object@env$isCheckpointed <- isCheckpointed
  .Object@env$serialized <- serialized
  
  .Object@jds <- jds
  .Object
})

setMethod("initialize", "TransformedDStream", function(.Object, prev, func, 
                                                       jds_val) {
  .Object@env <- new.env()
  .Object@env$isCached <- FALSE
  .Object@env$isCheckpointed <- FALSE
  .Object@env$jds_val <- jds_val
  # This tracks if jrdd_val is serialized
  .Object@env$serialized <- prev@env$serialized
  
  # NOTE: We use prev_serialized to track if prev_jrdd is serialized
  # prev_serialized is used during the delayed computation of JRDD in getJRDD
  .Object@prev <- prev
  
  isPipelinable <- function(rdd) {
    e <- rdd@env
    !(e$isCached || e$isCheckpointed)
  }
  
  if (!inherits(prev, "TransformedDStream") || !isPipelinable(prev)) {
    # This transformation is the first in its stage:
    .Object@func <- func
    .Object@prev_jds <- getJRDD(prev)
    # Since this is the first step in the pipeline, the prev_serialized
    # is same as serialized here.
    .Object@env$prev_serialized <- .Object@env$serialized
  } else {
    pipelinedFunc <- function(split, iterator) {
      func(split, prev@func(split, iterator))
    }
    .Object@func <- pipelinedFunc
    .Object@prev_jds <- prev@prev_jds # maintain the pipeline
    # Get if the prev_jrdd was serialized from the parent RDD
    .Object@env$prev_serialized <- prev@env$prev_serialized
  }
  
  .Object
})

#' @rdname DStream
#' @export
#'
#' @param jds Java object reference to the backing JavaDStream
#' @param serialized TRUE if the DStream stores data serialized in R
#' @param isCached TRUE if the DStream is cached
#' @param isCheckpointed TRUE if the DStream has been checkpointed
DStream <- function(jds, serialized = TRUE, isCached = FALSE,
                isCheckpointed = FALSE) {
  new("DStream", jds, serialized, isCached, isCheckpointed)
}

TransformedDStream <- function(prev, func) {
  new("TransformedDStream", prev, func, NULL)
}

# The jrdd accessor function.
setGeneric("getJDStream", function(ds, ...) { standardGeneric("getJDStream") })
setMethod("getJDStream", signature(ds = "DStream"), function(ds) ds@jds )
setMethod("getJDStream", signature(ds = "TransformedDStream"),
          function(ds, dataSerialization = TRUE) {
            if (!is.null(ds@env$jds_val)) {
              return(ds@env$jds_val)
            }
            
            # TODO: This is to handle anonymous functions. Find out a
            # better way to do this.
            computeFunc <- function(split, part) {
              ds@func(split, part)
            }
            serializedFuncArr <- serialize("computeFunc", connection = NULL,
                                           ascii = TRUE)
            
            packageNamesArr <- serialize(.sparkREnv[[".packages"]],
                                         connection = NULL,
                                         ascii = TRUE)
            
            broadcastArr <- lapply(ls(.broadcastNames),
                                   function(name) { get(name, .broadcastNames) })
            
            depsBin <- getDependencies(computeFunc)
            
            prev_jrdd <- ds@prev_jrdd
            
            if (dataSerialization) {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.RRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            } else {
              rddRef <- newJObject("edu.berkeley.cs.amplab.sparkr.StringRRDD",
                                   callJMethod(prev_jrdd, "rdd"),
                                   serializedFuncArr,
                                   rdd@env$prev_serialized,
                                   depsBin,
                                   packageNamesArr,
                                   as.character(.sparkREnv[["libname"]]),
                                   broadcastArr,
                                   callJMethod(prev_jrdd, "classTag"))
            }
            # Save the serialization flag after we create a RRDD
            rdd@env$serialized <- dataSerialization
            rdd@env$jrdd_val <- callJMethod(rddRef, "asJavaRDD") # rddRef$asJavaRDD()
            rdd@env$jrdd_val
          })

setValidity("DStream",
            function(object) {
              jrdd <- getJDSream(object)
              cls <- callJMethod(jrdd, "getClass")
              className <- callJMethod(cls, "getName")
              if (grep("spark.api.java.*RDD*", className) == 1) {
                TRUE
              } else {
                paste("Invalid DStream class ", className)
              }
            })


############ Actions and Transformations ############


#' Applies a function to all RDDs in an DStream, and force evaluation.
#'
#' @param rdd The RDD to apply the function
#' @param func The function to be applied.
#' @return invisible NULL.
#' @export
#' @rdname foreachRDD
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' rdd <- parallelize(sc, 1:10)
#' foreachRDD(dstream, function(x) { save(x, file=...) })
#'}
setGeneric("foreachRDD", 
           function(dstream, func) { standardGeneric("foreachRDD") })

#' @rdname foreachRDD
#' @aliases foreach,DStream,function-method
setMethod("foreachRDD",
          signature(dstream = "DStream", func = "function"),
          function(dstream, func) {
            callJStatic("edu.berkeley.cs.amplab.sparkr.streaming.RDStream", 
                        "callForeachRDD", 
                        dstream@jds, serialize(func, connection = NULL))
          })

setGeneric("print", function(dstream, num = 10) {
  standardGeneric("print") 
})

setMethod("print",
          signature(dstream = "DStream", num = "numeric"),
          function(dstream, num = 10) {
            func <- function(time, rdd) {
              taken <- take(rdd, num + 1)
              cat("-------------------------------------------\n")
              cat("Time: ", time, "\n")
              cat("-------------------------------------------\n")
              cat(paste(taken[1:num], collapse = "\n"))
              if (length(taken) > num) {
                cat("...\n")
              }
            }
            foreachRDD(dstream, func)
          })

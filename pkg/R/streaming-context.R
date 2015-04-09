# Spark StreamingContext driven functions

#' Creates an input stream from an queue of RDDs or list. In each batch,
#' it will process either one or all of the RDDs returned by the queue.
#' 
#' NOTE: changes to the queue after the stream is created will not be recognized.
#' 
#' @param rdds A list of RDDs.
#' @param oneAtATime A logical indicating if picking one rdd each time or 
#' picking all of them once.
#' @param default The default rdd if no more in rdds.
#' @return A DStream consisting of the queue of the given rdds.
#' @export
#' @examples
#'\dontrun{
#'  sc <- sparkR.init()
#'  ssc <- sparkR.streaming.init(sc, 1L)
#'  dstream <- queueStream(ssc, "directory/")
#'}
queueStream <- function(ssc, rdds = list(), oneAtATime = TRUE, default = NULL) {
  if (!is.null(default) && !inherits(default, "RDD")) {
    default <- parallelize(sc, default)@jrdd
  }
  jrdds <- lapply(rdds, function(rdd) {
    if (!inherits(rdd, "RDD")) {
      sc <- get(".sparkRjsc", envir = .sparkREnv)
      rdd <- parallelize(sc, rdd)
    }
    rdd@jrdd
  })
  jrdds <- convertRListToJList(jrdds)
  jqueue <- callJStatic("org.apache.spark.streaming.api.r.RDStream", 
                        "toRDDQueue", jrdds)
  if (is.null(default)) {
    jdstream <- callJMethod(ssc, "queueStream", jqueue, oneAtATime)
  } else {
    jdstream <- callJMethod(ssc, "queueStream", jqueue, oneAtATime, default)
  }  
  DStream(jdstream)
}

#' Creates an input stream that monitors a Hadoop-compatible file system
#' for new files and reads them as text files. Files must be wrriten to the
#' monitored directory by "moving" them from another location within the same
#' file system. File names starting with . are ignored.
#'
#' @param ssc Spark StreamingContext (Java API) to use.
#' @param directory A character vector of the directory path to monitor.
#' @return A DStream where each item is of type \code{character}
#' @export
#' @examples
#'\dontrun{
#'  sc <- sparkR.init()
#'  ssc <- sparkR.streaming.init(sc, 1L)
#'  textStream <- textFileStream(ssc, "directory/")
#'}
textFileStream <- function(ssc, directory) {
  DStream(callJMethod(ssc, "textFileStream", directory), serializedMode = "string")
}

#' Create an input from TCP source hostname:port. Data is received using
#' a TCP socket and stored in String format.
#'
#' @param ssc Spark StreamingContext (Java API) to use.
#' @param host A character vector of hostname to connect to for receiving data.
#' @param port Port to connect to for receiving data.
#' @param storageLevel A character vector of storage level to use for storing 
#'                     the received objects
#' @return A DStream where each item is of type \code{character}
#' @export
#' @examples
#'\dontrun{
#'  sc <- sparkR.init()
#'  ssc <- sparkR.streaming.init(sc, 1L)
#'  textStream <- socketTextStream(ssc, "localhost", 9999)
#'}
socketTextStream <- function(ssc, host, port, storageLevel = "MEMORY_AND_DISK_SER_2") {
  jStorageLevel <- getStorageLevel(storageLevel)
  DStream(callJMethod(ssc, "socketTextStream", host, as.integer(port), jStorageLevel), 
          serializedMode = "string")
}

#' Sets the context to periodically checkpoint the DStream operations for master
#' fault-tolerance. The graph will be checkpointed every batch interval.
#' 
#' @param directory A character of HDFS-compatible directory path where the 
#'                  checkpoint data will be reliably stored.
#' @export
setMethod("checkpoint",
          signature(x = "jobj"),
          function(x, directory) {
            callJMethod(x, "checkpoint", directory)
          })

#' Start the execution of the DStream.
#'
#' @param ssc The Java streaming context.
#' @export
startStreaming <- function(ssc) {
  callJMethod(ssc, "start")
}

#' Wait for the execution to stop.
#' 
#' @param ssc The Java streaming context.
#' @param timeout time to wait in seconds.
#' @export
awaitTermination <- function(ssc, timeout) {
  if (missing(timeout)) {
    callJMethod(ssc, "awaitTermination")
  } else {
    callJMethod(ssc, "awaitTermination", as.integer(timeout * 1000))
  }
}

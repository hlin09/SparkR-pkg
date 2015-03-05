textFileStream <- function(ssc, directory) {
#   jRDstream <- newJObject("edu.berkeley.cs.amplab.sparkr.streaming.CallbackTest")
  DStream(callJMethod(ssc, "textFileStream", directory))
}

startStreaming <- function(ssc) {
  callJMethod(ssc, "start")
}

awaitTermination <- function(ssc, timeout) {
  if (missing(timeout)) {
    callJMethod(ssc, "awaitTermination")
  } else {
    callJMethod(ssc, "awaitTermination", as.integer(timeout * 1000))
  }
}

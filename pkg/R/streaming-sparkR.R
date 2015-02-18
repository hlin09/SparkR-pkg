sparkR.streaming.init <- function(sc, batchDuration) {
  if (exists(".sparkRjssc", envir = .sparkREnv)) {
    cat("Re-using existing Spark Streaming Context. \
        Please stop SparkR with sparkR.stop() or restart R to create \
        a new Spark Streaming Context\n")
    return(get(".sparkRjssc", envir = .sparkREnv))
  }
  
  # Start the R callback server.
  
  
  assign(".sparkRjssc", 
         newJObject("org.apache.spark.streaming.api.java.JavaStreamingContext", 
                    sc, 
                    newJObject("org.apache.spark.streaming.Duration", 
                               as.integer(batchDuration * 1000))),
         envir = .sparkREnv
  )
  ssc <- get(".sparkRjssc", envir = .sparkREnv)
  ssc
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

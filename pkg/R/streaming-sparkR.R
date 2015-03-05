sparkR.streaming.init <- function(sc, batchDuration) {
  if (exists(".sparkRjssc", envir = .sparkREnv)) {
    cat("Re-using existing Spark Streaming Context. \
        Please stop SparkR with sparkR.stop() or restart R to create \
        a new Spark Streaming Context\n")
    return(get(".sparkRjssc", envir = .sparkREnv))
  }
  
  # Start the R callback server.
  cmd <- "Rscript"
  args <- paste("--vanilla ", 
                .sparkREnv$libname, "/SparkR/callback/streaming-callback.R", 
                sep="")
  env.vars <- c(paste("BACKEND_PORT=", .sparkREnv$sparkRBackendPort, sep=""), 
                paste("SPARKDR_RLIBDIR=", .sparkREnv$libname, sep=""),
                paste("CALLBACK_PORT=", 54321L, sep=""))
  cat("Starting the callback server: ", cmd, args, "\n")
  system2(cmd, args, env = env.vars, wait = FALSE)
  Sys.sleep(2)
  SparkR:::callJStatic("SparkRHandler", "connectCallback")
  
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

# R callback server.
cat("R callback server started.\n")

rLibDir <- Sys.getenv("SPARKDR_RLIBDIR")

# Load SparkR package.
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

# Waiting for backend to connect.
callback.port <- as.integer(Sys.getenv("CALLBACK_PORT"))
cat("Waiting for backend to connect.\n")
serverCon <- socketConnection(port = callback.port, blocking = TRUE, 
                              server = TRUE, open = "wb")
cat("Callback server connected to backend.\n")

# Setup the connection with JVM Backend.
backend.port <- as.integer(Sys.getenv("BACKEND_PORT"))
assign("sparkRBackendPort", backend.port, envir = SparkR:::.sparkREnv)
cat("Connecting backend with port number: ", backend.port, "\n")
assign(".sparkRCon", 
       socketConnection(port = backend.port, blocking = TRUE, open = "wb"), 
       envir = SparkR:::.sparkREnv)

# Receive RDDs and functions, transforms them into RRDDs.
while (TRUE) {
  cat("Waiting for requests.\n")
  tryCatch({
    cmd <- SparkR:::readString(serverCon)
    cat(cmd, "\n")
    if (cmd == "close") {
      quit(save = "no")
    } else if (cmd == "callback") {
      jrdd <- SparkR:::readObject(serverCon)
      time <- SparkR:::readDouble(serverCon)
      func <- unserialize(SparkR:::readRaw(serverCon))
      rrdd <- SparkR:::RDD(jrdd)
      res <- func(time, rrdd)
      if (inherits(res, "RDD")) {
        SparkR:::writeObject(serverCon, res$jrdd)
      } else {
        SparkR:::writeType(serverCon, "NULL")
      }
      flush(serverCon)
    }
  }#, error = function(e) {}
  )
}

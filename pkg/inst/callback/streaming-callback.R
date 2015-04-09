#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# R callback server.
cat("R callback server started.\n")

rLibDir <- Sys.getenv("SPARKDR_RLIBDIR")

# Load SparkR package.
.libPaths(c(rLibDir, .libPaths()))
suppressPackageStartupMessages(library(SparkR))
assign(".scStartTime", 0L, SparkR:::.sparkREnv)

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
#   cat("Waiting for requests.\n")
  ready <- socketSelect(list(serverCon))
  if (ready) {
    tryCatch({
      cmd <- SparkR:::readString(serverCon)
      if (cmd == "close") {
        quit(save = "no")
      } else if (cmd == "callback") {
        numRDDs <- SparkR:::readInt(serverCon)
        jrdds <- lapply(1:numRDDs, function(x) { SparkR:::readObject(serverCon) })
        time <- SparkR:::readDouble(serverCon)
        func <- unserialize(SparkR:::readRaw(serverCon))
        deserializers <- sapply(1:numRDDs, function(x) { SparkR:::readString(serverCon) })
        rrdds <- lapply(1:numRDDs, function(i) {
          if (is.null(jrdds[[i]])) {
            NULL
          } else {
            SparkR:::RDD(jrdds[[i]], serializedMode = deserializers[i])
          }          
        })
        res <- do.call(func, c(time, rrdds))
        if (inherits(res, "RDD")) {
          SparkR:::writeObject(serverCon, SparkR:::getJRDD(res))
        } else {
          SparkR:::writeObject(serverCon, NULL)
        }
        flush(serverCon)
      }
    }, error = function(e) {
#       print(e)
      quit(save = "no") 
    })
  }
} 

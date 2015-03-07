context("SparkR streaming tests")

timeout <- 10
duration <- 1L
# JavaSparkContext handle
sc <- sparkR.init()
ssc <- sparkR.init.stream(sc, duration)

.waitFor <- function(res, num) {
  start.time <- proc.time()
  while (res$size < num && (proc.time() - start.time)["elapsed"] < timeout) {
    Sys.sleep(0.01)
  }
  if (res$size < num) {
    cat("Timeout after: ", timeout, "\n")
  }
}

.collectStream <- function(dstream, num, ) {
  res <- initAccumulator()
  getOutput <- function(time, rdd) {
    if (res$size < num) {
      addToAccumulator(res, collect(rdd))
    }
  }
  foreachRDD(dstream, getOutput)
  startStream(ssc)
  .waitFor(res, num)
  res
}

test_that("", {
  
})

context("SparkR streaming tests")

# Streaming context related.
timeout <- 10
duration <- 1L
sc <- sparkR.init()
ssc <- sparkR.streaming.init(sc, duration)

# Data
nums1 <- 1:10
rdd1 <- parallelize(sc, nums1, 1L)

nums2 <- 11:20
rdd2 <- parallelize(sc, nums2, 1L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

# Helper functions.
.waitFor <- function(res, num) {
  start.time <- proc.time()
  while (res$size < num && (proc.time() - start.time)["elapsed"] < timeout) {
    Sys.sleep(0.01)
  }
  if (res$size < num) {
    cat("Timeout after: ", timeout, "\n")
  }
}

.collectStream <- function(dstream, num) {
  res <- SparkR:::initAccumulator()
  getOutput <- function(time, rdd) {
    if (res$size < num) {
      SparkR:::addItemToAccumulator(res, collect(rdd))
    }
  }
  foreachRDD(dstream, getOutput)
  startStreaming(ssc)
  .waitFor(res, num)
  res$data
}

test_that("mapPartitionsWithIndex on DStream", {
#   inputStream <- queueStream(ssc, list(rdd1, rdd2))
#   mappedStream <- mapPartitionsWithIndex(inputStream, 
#                                          function(split, part) {
#                                            split + Reduce("+", part)
#                                          })
#   expected <- list(list(55), list(155))
#   actual <- .collectStream(mappedStream, 2)
#   expect_equal(actual, expected)
})

sparkR.streaming.stop(ssc, stopSparkContext = FALSE)

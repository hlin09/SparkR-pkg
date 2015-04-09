context("SparkR streaming tests")

# Streaming context related.
timeout <- 10
duration <- 1L
resTempFile <- tempfile(pattern = "streamingTestResult")
env <- new.env()
sc <- sparkR.init()
ssc <- sparkR.streaming.init(sc, duration)

# Data
nums1 <- 1:10
rdd1 <- parallelize(sc, nums1, 2L)

nums2 <- 11:20
rdd2 <- parallelize(sc, nums2, 2L)

intPairs <- list(list(1L, -1), list(2L, 100), list(2L, 1), list(1L, 200))
intRdd <- parallelize(sc, intPairs, 2L)

intPairs2 <- list(list(1L, 1), list(2L, -100), list(2L, -1), list(1L, -200))
intRdd2 <- parallelize(sc, intPairs2, 2L)

.setup <- function(to = 10) {
  timeout <<- to
  sc <<- sparkR.init()
  ssc <<- sparkR.streaming.init(sc, duration)
  rdd1 <<- parallelize(sc, nums1, 2L)
  rdd2 <<- parallelize(sc, nums2, 2L)
  intRdd <<- parallelize(sc, intPairs, 2L)
  intRdd2 <<- parallelize(sc, intPairs2, 2L)
  resTempFile <<- tempfile(pattern = "streamingTestResult")
}

.finish <- function() {
  sparkR.streaming.stop(ssc)
  unlink(resTempFile)
}

# Helper functions.
.waitFor <- function(res, num) {
  start.time <- proc.time()
  load(file = resTempFile)
  while (res$counter < num && (proc.time() - start.time)["elapsed"] < timeout) {
    Sys.sleep(0.5)
    load(file = resTempFile)
  }
  if (res$counter < num) {
    cat("Timeout after: ", timeout, "\n")
  } else {
#     cat("\nEnough results collected\n")
  }
}

.collectStream <- function(dstream, num = 1L) {
  res <- SparkR:::initAccumulator()
  save(res, file = resTempFile)
  getOutput <- function(time, rdd) {
    load(file = resTempFile)
    if (res$counter < num) {
      SparkR:::addItemToAccumulator(res, collect(rdd))
    }
    save(res, file = resTempFile)
  }
  foreachRDD(dstream, getOutput)
  startStreaming(ssc)
  .waitFor(res, num)
  load(file = resTempFile)
  res$data[1:res$counter]
}

# Streaming context tests.

test_that("start and stop context multiple times", {
  for (i in 1:3) {
    sc <<- sparkR.init()
    ssc <<- sparkR.streaming.init(sc, duration)
    inputStream <- queueStream(ssc, list(parallelize(sc, 1:10, 2L)))
    sparkR.streaming.stop(ssc)
  }
  sparkR.streaming.stop(ssc)
})

test_that("queueStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  actual <- .collectStream(inputStream, 2)
  expected <- list(as.list(nums1), as.list(nums2))
  expect_equal(actual, expected)
  .finish()
})

# Basic operations tests.

test_that("count on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  expected <- list(list(10), list(10))
  actual <- .collectStream(count(inputStream), 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("map on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  mappedStream <- map(inputStream, function(x) { exp(x) * 2 })
  expected <- list(as.list(exp(nums1) * 2), as.list(exp(nums2) * 2))
  actual <- .collectStream(mappedStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("flatMap on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  mappedStream <- flatMap(inputStream, function(x) { list(x, x) })
  expected <- list(as.list(rep(nums1, each = 2)), as.list(rep(nums2, each = 2)))
  actual <- .collectStream(mappedStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("mapPartitions on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  mappedStream <- mapPartitions(inputStream, function(part) { Reduce("+", part) })
  expected <- list(list(15, 40), list(65, 90))
  actual <- .collectStream(mappedStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("mapPartitionsWithIndex on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  mappedStream <- mapPartitionsWithIndex(inputStream, 
                                         function(split, part) {
                                           split + Reduce("+", part)
                                         })
  expected <- list(list(15, 41), list(65, 91))
  actual <- .collectStream(mappedStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("Filter on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  filteredStream <- filterRDD(inputStream, function(x) { x %% 4 == 0 })
  expected <- list(list(4, 8), list(12, 16, 20))
  actual <- .collectStream(filteredStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("reduce on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1, rdd2))
  reducedStream <- reduce(inputStream, "+")
  expected <- list(list(55), list(155))
  actual <- .collectStream(reducedStream, 2)
  expect_equal(actual, expected)
  .finish()
})

test_that("reduceByKey on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(intRdd))
  reducedStream <- reduceByKey(inputStream, "+", 2L)
  expected <- list(list(list(2L, 101), list(1L, 199)))
  actual <- .collectStream(reducedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("combineByKey on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(intRdd))
  combinedStream <- combineByKey(inputStream, function(x) { x }, "+", "+", 2L)
  expected <- list(list(list(2L, 101), list(1L, 199)))
  actual <- .collectStream(combinedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("paritionBy on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(intRdd))
  # Partition by magnitude
  partitionByMagnitude <- function(key) { if (key >= 2) 1 else 0 }
  partitionedStream <- partitionBy(inputStream, 2L, partitionByMagnitude)
  expected <- list(list(list(1L, -1), list(1L, 200), list(2L, 100), list(2L, 1)))
  actual <- .collectStream(partitionedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("groupByKey on DStream", {
  .setup()
  inputStream <- queueStream(ssc, list(intRdd))
  groupedStream <- groupByKey(inputStream, 2L)
  expected <- list(list(list(2L, list(100, 1)), list(1L, list(-1, 200))))
  actual <- .collectStream(groupedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("unionRDD on DStreams", {
  .setup()
  inputStream <- queueStream(ssc, list(rdd1))
  inputStream2 <- queueStream(ssc, list(rdd2))
  unionStream <- unionRDD(inputStream, inputStream2)
  expected <- list(as.list(c(nums1, nums2)))
  actual <- .collectStream(unionStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("cogroup on DStreams", {
  .setup()
  inputStream <- queueStream(ssc, list(intRdd))
  inputStream2 <- queueStream(ssc, list(intRdd2))
  groupedStream <- cogroup(inputStream, inputStream2, numPartitions = 2L)
  expected <- list(list(list(2L, list(list(100, 1), list(-100, -1))), 
                        list(1L, list(list(-1, 200), list(1, -200)))))
  actual <- .collectStream(groupedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("join on DStreams", {
  .setup()
  inputStream <- queueStream(ssc, list(list(list(1L, "a"), list(2L, "b"))))
  inputStream2 <- queueStream(ssc, list(list(list(2L, "a"), list(3L, "b"))))
  joinedStream <- join(inputStream, inputStream2, numPartitions = 1L)
  expected <- list(list(list(2L, list("b", "a"))))
  actual <- .collectStream(joinedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("leftOuterJoin on DStreams", {
  .setup()
  inputStream <- queueStream(ssc, list(list(list(1L, "a"), list(2L, "b"))))
  inputStream2 <- queueStream(ssc, list(list(list(2L, "a"), list(3L, "b"))))
  joinedStream <- leftOuterJoin(inputStream, inputStream2, numPartitions = 2L)
  expected <- list(list(list(2L, list("b", "a")), list(1L, list("a", NULL))))
  actual <- .collectStream(joinedStream, 1)
  expect_equal(actual, expected)
  .finish()
})

test_that("windowDStream on DStreams", {
  .setup()
  inputStream <- queueStream(ssc, list(1:2, 1:3, 1:4, 1:5))
  windowedStream <- count(windowDStream(inputStream, 3L, 1L))
  expected <- list(list(2), list(5), list(9), list(12), list(9), list(5))
  actual <- .collectStream(windowedStream, 6)
  expect_equal(actual, expected)
  .finish()
})

test_that("countByWindow on DStreams", {
  .setup(to = 40)
  checkpoint(ssc, "checkpoints")
  inputStream <- queueStream(ssc, list(1:2, 1:3, 1:4, 1:5, 1:4))
  countWindowStream <- countByWindow(inputStream, 5L, 1L)
  expected <- list(list(2), list(5), list(9), list(14), list(18), list(16), 
                   list(13))
  actual <- .collectStream(countWindowStream, 7)
  expect_equal(actual, expected)
  .finish()
  unlink("checkpoints")
})

test_that("countByValueAndWindow on DStreams", {
  .setup(15)
  checkpoint(ssc, "checkpoints")
  inputStream <- queueStream(ssc, list(1:2, 1:3, 1:4, 1:5, 1:4))
  countWindowStream <- countByValueAndWindow(inputStream, 5L, 1L)
  expected <- list(list(2), list(3), list(4), list(5), list(5), list(5))
  actual <- .collectStream(countWindowStream, 6)
  expect_equal(actual, expected)
  .finish()
  unlink("checkpoints")
})

test_that("groupByKeyAndWindow on DStreams", {
  .setup(15)
  checkpoint(ssc, "checkpoints")
  inputStream <- queueStream(ssc, list(
    list(list(1L, 1)), list(list(1L, 2)), list(list(1L, 3)), list(list(1L, 4))))
  groupWindowStream <- groupByKeyAndWindow(inputStream, 3L, 1L)
  expected <- list(list(list(1L, list(1))), list(list(1L, list(1, 2))), 
                   list(list(1L, list(1, 2, 3))), list(list(1L, list(2, 3, 4))), 
                   list(list(1L, list(3, 4))), list(list(1L, list(4))))
  actual <- .collectStream(groupWindowStream, 6)
  expect_equal(actual, expected)
  .finish()
  unlink("checkpoints")
})

test_that("updateStateByKey on DStreams", {
  .setup()
  checkpoint(ssc, "checkpoints")
  inputStream <- queueStream(ssc, list(
    list(list(1L, 1)), list(list(1L, 2)), list(list(1L, 3)), list(list(1L, 4))))
  updateFunc <- function(vs, s) { sum(unlist(vs), s) }
  statefulSumStream <- updateStateByKey(inputStream, updateFunc, 1L)
  expected <- list(list(list(1L, 1)), list(list(1L, 3)), list(list(1L, 6)), list(list(1L, 10)))
  actual <- .collectStream(statefulSumStream, 4)
  expect_equal(actual, expected)
  .finish()
  unlink("checkpoints")
})

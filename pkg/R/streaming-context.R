textFileStream <- function(ssc, directory) {
  DStream(callJMethod(ssc, "textFileStream", directory))
}

# Utility functions for R streaming.

rddToFileName <- function(prefix, suffix, time) {
  if (is.null(suffix) || nchar(suffix) == 0) {
    paste(prefix, "-", time, sep = "")
  } else {
    paste(prefix, "-", time, ".", suffix, sep = "")
  }
}


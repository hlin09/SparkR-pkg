library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) != 1) {
  print("Usage: wordcount <directory>")
  q("no")
}

# Initialize Spark context
sc <- sparkR.init(appName = "RStreamingHDFSWordCount")
ssc <- sparkR.streaming.init(sc, 1)
lines <- textFileStream(ssc, args[[1]])

words <- flatMap(lines,
                 function(line) {
                   strsplit(line, " ")[[1]]
                 })
wordCount <- map(words, function(word) { list(word, 1L) })

counts <- reduceByKey(wordCount, "+", 2L)
print(map(counts, function(wordcount) { 
  paste(wordcount[[1]], ": ", wordcount[[2]], "\n", sep = "") 
}))

startStreaming(ssc)
awaitTermination(ssc)


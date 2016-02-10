# Author: Adib Alwani

# Function to read file and process data
readFile <- function  (filename) {
  help("read.table")
  df <- read.table(filename, fill = TRUE, col.names = c("carrier_code", "price", "flight_time", "distance_traveled"))
  jpeg(sprintf("plots/%s.jpg", df[2:2, 1:1]))
  regression1 <- lm(df$distance_traveled ~ df$price)
  #regression2 <- lm(df$flight_time ~ df$price)
  #abline(regression1, col = 3, lwd = 3)
  termplot(regression1)
  #abline(regression1, col = 3, lwd = 3)
  #abline(regression2, col = 3, lwd = 3)
  dev.off()
}

# Read output folder
filenames <- list.files("splitfiles", pattern="*.txt", full.names=TRUE)
lapply(filenames, readFile)
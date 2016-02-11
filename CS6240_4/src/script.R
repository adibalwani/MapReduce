# Author: Adib Alwani

# Function to read file and process data
processFile <- function (filename) {
	df <- read.table(filename, fill = TRUE, col.names = c("carrier_code", "price", "flight_time", "distance_traveled"))

	print(sprintf("Processing data for %s", filename))
	flush.console()

	# Linear Regression (Dependent - Price) :-
	# Independent - Distance Traveled
	jpeg(sprintf("plots/%s-distance.jpg", df[2:2, 1:1]))
	plot(df$distance_traveled, df$price)
	regression <- lm(df$price ~ df$distance_traveled)
	abline(regression, col = "red", lwd = 3)
	sink(sprintf("summary/%s-distance.txt", df[2:2, 1:1]))
	summary <- summary(regression)
	print(summary)
	flush.console()
	sink()
	dev.off()

	# Independent - Flight Time
	jpeg(sprintf("plots/%s-flight-time.jpg", df[2:2, 1:1]))
	plot(df$flight_time, df$price)
	regression <- lm(df$price ~ df$flight_time)
	abline(regression, col = "red", lwd = 3)
	sink(sprintf("summary/%s-flight-time.txt", df[2:2, 1:1]))
	summary <- summary(regression)
	print(summary)
	flush.console()
	sink()
	dev.off()
}

# Read output folder
filenames <- list.files("splitfiles", pattern = "*.txt", full.names = TRUE)
lapply(filenames, processFile)

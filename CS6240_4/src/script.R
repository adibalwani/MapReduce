# Author: Adib Alwani

# Read output file
df <- read.table("/home/adib/workspace/CS6240_4/src/finaloutput", 
                 fill = TRUE, 
                 col.names = c("carrier_code", "price", "flight_time", "distance_traveled"))

# Linear Regressions for distance traveled to price
plot(df$price, df$distance_traveled)
regression <- lm(df$distance_traveled ~ df$price)
summary(regression)
abline(regression, col = 2)

# linear regressions for flight time to price
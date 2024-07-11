library(dplyr)
library(aws.s3)
library(stringdist)
library(collections)

df <- aws.s3::s3read_using(FUN = read.csv,
                           object = "BDD_Label/Enquetes_triee.csv",
                           bucket = "bbrunel",
                           opts = list("region" = ""))

m <- matrix(nrow = nrow(df), ncol = nrow(df))

for (i in 1:nrow(df)) {
  for (j in 1:nrow(df)) {
    m[i,j] <- stringdist(df[i,]$intitule, df[j,]$intitule, method="lv")
  }
}

for (i in 1:nrow(df)) {
  for (j in 1:i) {
    m[i,j] <- NA
  }
}

w <- which(!is.na(m) & m <= 10)
saveRDS(m,'disLV.rds')

wi <- w %/% 1457 + 1
wj <- w %% 1457
t <- data.frame(x = df[wi,]$intitule, y = df[wj,]$intitule, d = m[w], i = wi, j = wj)
t <- t %>% filter(i != j & d != 0) 
View(data.frame("1" = 1:3))

l <- list()

for (i in 1:nrow(df)) {
  l[[i]] <- which(m[i,] <= 20) %% 1457
}


compoConnexe <- function(L, s=1, explored = NULL) {
  explored <- rep(F,length(L))
  toExplore <- c(s)
  conn <- c()
  while (length(toExplore) != 0) {
    for (x in L[[toExplore[1]]]) {
      if (!explored[x]) {
        toExplore <- c(toExplore, x)
        explored[x] <- T
        conn <- c(conn,x)
      }
    }
    toExplore <- toExplore[-1]
  }
  return(list(conn,explored))
}

conn <- compoConnexe(l)
View(df[conn,])

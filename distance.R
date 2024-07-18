rm(list = ls())

library(dplyr)
library(aws.s3)
library(stringdist)

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

## Star here

compoConnexe <- function(L, s=1, explored = NULL) {
  if (is.null(explored))
    explored <- rep(F,length(L))
  toExplore <- c()
  conn <- c()
  if (!explored[s]) {
    toExplore <- c(s)
    explored[s] <- T
  }
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

titreSimilaires <- function(maxLV = 10) {
  l <- list()
  for (i in 1:nrow(df)) {
    l[[i]] <- which(m[i,] <= maxLV) %% 1457
  }
  exp <- c()
  compsConn <- list()
  for (i in 1:nrow(df)) {
    r <- compoConnexe(l,i,exp)
    exp <- r[[2]]
    if(!is.null(r[[1]]))
      compsConn <- c(compsConn,list(r[[1]]))
  }
  return(compsConn)
}

lengths <- function(L) {
  l <- c()
  for (i in 1:length(L)) {
    l <- c(l,length(L[[i]]))
  }
  return(list(mean = mean(l),sd = sd(l),max = max(l)))
}

i <- 10
compsConn <- titreSimilaires(12)
while(lengths(compsConn)$sd < 5) {
  i <- i + 1
  compsConn <- titreSimilaires(i)
}
lengths(compsConn)

for (i in 1:length(compsConn)) {
  write.csv(df[compsConn[[i]],] %>% select(intitule,annee),
            "titre_sim.csv",
            append = function(i) { i > 1}(i))
}


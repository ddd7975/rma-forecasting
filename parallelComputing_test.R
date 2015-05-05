library(Rmpi)
library(snow)
temp <- 3
mpi.spawn.Rslaves()
system.time(mpi.parReplicate(100, mean(rnorm(1000000))))
system.time(sapply(1:100, function(i)mean(rnorm(1000000))))
mpi.close.Rslaves()

temp <- 3
temp2 <- 2
temp3 <- c(1:100)
cl <- makeCluster(4,type="SOCK")
clusterExport(cl, list("temp", "temp2", "temp3"))
#system.time(sapply(1:100,function(i) mean(rnorm(1000000))))
parSapply(cl,1:100,function(i) {
  temp*temp2*temp3[i]*mean(rnorm(1000000))
  })

parSapply(cl,1:100,function(i) {
  mean(rnorm(1000000))
})
stopCluster(cl)

#########################################
cl <- makeCluster(4,type="SOCK")
clusterExport(cl, list("temp", "temp2", "temp3"))
stopCluster(cl)
#########################################


kk <- function(m){
  cl <- makeCluster(4,type="SOCK")
  clusterExport(cl, list("temp", "temp2", "temp3"))
  parSapply(cl,1:100,function(i) {
    mean(rnorm(1000000))
  })
  stopCluster(cl)
}
kk(3)

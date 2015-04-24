library(Rmpi)
library(snow)

mpi.spawn.Rslaves()
system.time(mpi.parReplicate(100, mean(rnorm(1000000))))
system.time(sapply(1:100, function(i)mean(rnorm(1000000))))
mpi.close.Rslaves()


cl <- makeCluster(4,type="SOCK")
system.time(sapply(1:100,function(i) mean(rnorm(1000000))))
system.time(parSapply(cl,1:100,function(i) mean(rnorm(1000000))))
stopCluster(cl)

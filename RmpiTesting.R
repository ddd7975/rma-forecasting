library(Rmpi)
library(rjson)
library(snow)
cl <- makeCluster(4,type="MPI")
varlist <- list("compNameAppear", "dataArrC", "dat_all", "dat_com", "dat_shipping", "dat_future_shipping", 
                "ymd", "toJSON", "selectNiC", "rmaNonparametricC", "probMappingC", "evalFun")
# varlist <- list("compNameAppear", "dataArrC", "dat_all", "dat_com", "dat_shipping", "dat_future_shipping", 
#                 "ymd", "toJSON", "selectNi", "rmaNonparametric", "probMappingC")
clusterExport(cl, varlist, envir = .GlobalEnv)
pt2 <- system.time({
  output2 <- parLapply(cl, 1:5, function(pro){
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC = rmaNonparametricC)
    out <- evalFun(elected, componentName)
    return(out)
  })
})
stopCluster(cl)

pt1 <- system.time({
  output1 <- sapply(1:5, function(pro){
    print(pro)
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC = rmaNonparametricC)
    out <- evalFun(elected, componentName)
    return(out)
  })
})
library(Rmpi)
# mpi.spawn.Rslaves(nslaves=4)
# mpi.parReplicate(20, mean(rnorm(1000000)))
# mpi.close.Rslaves()

library(snow)
cl <- makeCluster(4,type="MPI")
result <- rep(NA,1000)
system.time(sapply(1:1000,function(i) mean(rnorm(10000))))
system.time(parSapply(cl,1:1000,function(i) mean(rnorm(10000))))
stopCluster(cl)

###
library(rjson)
library(snow)
cl <- makeCluster(4,type="MPI")
varlist <- list("compNameAppear", "dataArrC", "dat_all", "dat_com", "dat_shipping", "dat_future_shipping", 
                "ymd", "toJSON", "selectNiC", "rmaNonparametricC", "probMappingC")
clusterExport(cl, varlist, envir = .GlobalEnv)
pt2 <- system.time({
  output2 <- parSapply(cl, 1:5, function(pro){
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    if (!is.null(dataM)){
      datShipPro <- dataM[[3]]
      if (nrow(datShipPro) != 0){
        uniqueProduct <- as.character(unique(datShipPro$Product_Name))
        #
        minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
        dataComp_c <- dataM[[2]]
        datShipPro <- dataM[[3]]
        dat_censored1 <- dataM[[4]]
        n_break <- dataM[[5]]
        
        YMD <- ymd
        endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
        x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                               as.Date(endMonth), "months"))
        
        x <- as.character(sapply(x1, function(y){
          tmp <- strsplit(y, "-")[[1]]
          tmp[3] <- "01"
          tmp2 <- paste(tmp[1], tmp[2], tmp[3], sep="/")
          return(tmp2)
        }))
        
        # ----- split the x so that it can match the dat_shipping (because dat_shipping only record the amount by month)
        x_split <- 0
        for (i in 1:length(x)){
          tmp <- strsplit(x[i], "/")[[1]]
          x_split[i] <- paste0(tmp[1], tmp[2])
        }
        x_mid <- sapply(1:length(x), function(i){
          tmpd <- strsplit(x[i], "/")[[1]]
          tmpd[3] <- "15"
          paste(tmpd, collapse = "/")
        })
        nList <- lapply(1:length(uniqueProduct), function(i){
          datShipPro_i <- datShipPro[which(datShipPro$Product_Name == uniqueProduct[i]), ]
          n_ship <- sapply(1:(length(x_split)), function(j){
            return(max(sum(datShipPro_i[which(datShipPro_i$Shipping_DT == x_split[j]), "Qty"]), 0))
          })
          return(matrix(n_ship, nrow=1))
        })
        for (l in 1:length(nList)){
          colnames(nList[[l]]) <- x_mid[1:(length(x_mid))]
        }
        names(nList) <- uniqueProduct
        #       elected <- selectNi(dataM = dataM, YMD = ymd, minNi = 5)
        elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5)
      }else{
        elected <- matrix(0, ncol = 7, nrow = 0)
      }
    }else{
      elected <- matrix(0, ncol = 7, nrow = 0)
    }
    return(elected)
    })
})
pt1 <- system.time({
  output1 <- sapply(1:5, function(pro){
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    if (!is.null(dataM)){
      datShipPro <- dataM[[3]]
      if (nrow(datShipPro) != 0){
        uniqueProduct <- as.character(unique(datShipPro$Product_Name))
        #
        minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
        dataComp_c <- dataM[[2]]
        datShipPro <- dataM[[3]]
        dat_censored1 <- dataM[[4]]
        n_break <- dataM[[5]]
        
        YMD <- ymd
        endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
        x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                               as.Date(endMonth), "months"))
        
        x <- as.character(sapply(x1, function(y){
          tmp <- strsplit(y, "-")[[1]]
          tmp[3] <- "01"
          tmp2 <- paste(tmp[1], tmp[2], tmp[3], sep="/")
          return(tmp2)
        }))
        
        # ----- split the x so that it can match the dat_shipping (because dat_shipping only record the amount by month)
        x_split <- 0
        for (i in 1:length(x)){
          tmp <- strsplit(x[i], "/")[[1]]
          x_split[i] <- paste0(tmp[1], tmp[2])
        }
        x_mid <- sapply(1:length(x), function(i){
          tmpd <- strsplit(x[i], "/")[[1]]
          tmpd[3] <- "15"
          paste(tmpd, collapse = "/")
        })
        nList <- lapply(1:length(uniqueProduct), function(i){
          datShipPro_i <- datShipPro[which(datShipPro$Product_Name == uniqueProduct[i]), ]
          n_ship <- sapply(1:(length(x_split)), function(j){
            return(max(sum(datShipPro_i[which(datShipPro_i$Shipping_DT == x_split[j]), "Qty"]), 0))
          })
          return(matrix(n_ship, nrow=1))
        })
        for (l in 1:length(nList)){
          colnames(nList[[l]]) <- x_mid[1:(length(x_mid))]
        }
        names(nList) <- uniqueProduct
        #       elected <- selectNi(dataM = dataM, YMD = ymd, minNi = 5)
        elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5)
      }else{
        elected <- matrix(0, ncol = 7, nrow = 0)
      }
    }else{
      elected <- matrix(0, ncol = 7, nrow = 0)
    }
    return(elected)
    })
})
output <- sapply(1:10, function(pro){
  print(pro/length(compNameAppear))
  print(paste(pro, length(compNameAppear), sep = "/"))
  componentName <- compNameAppear[pro]
  tM1 <- proc.time()
  #   dataM <- dataArr(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
  dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
  tM2 <- proc.time()
  tM2 - tM1
  TC1 <- proc.time()
  if (!is.null(dataM)){
    datShipPro <- dataM[[3]]
    if (nrow(datShipPro) != 0){
      uniqueProduct <- as.character(unique(datShipPro$Product_Name))
      #
      minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
      dataComp_c <- dataM[[2]]
      datShipPro <- dataM[[3]]
      dat_censored1 <- dataM[[4]]
      n_break <- dataM[[5]]
      
      YMD <- ymd
      endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
      x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                             as.Date(endMonth), "months"))
      
      x <- as.character(sapply(x1, function(y){
        tmp <- strsplit(y, "-")[[1]]
        tmp[3] <- "01"
        tmp2 <- paste(tmp[1], tmp[2], tmp[3], sep="/")
        return(tmp2)
      }))
      
      # ----- split the x so that it can match the dat_shipping (because dat_shipping only record the amount by month)
      x_split <- 0
      for (i in 1:length(x)){
        tmp <- strsplit(x[i], "/")[[1]]
        x_split[i] <- paste0(tmp[1], tmp[2])
      }
      x_mid <- sapply(1:length(x), function(i){
        tmpd <- strsplit(x[i], "/")[[1]]
        tmpd[3] <- "15"
        paste(tmpd, collapse = "/")
      })
      nList <- lapply(1:length(uniqueProduct), function(i){
        datShipPro_i <- datShipPro[which(datShipPro$Product_Name == uniqueProduct[i]), ]
        n_ship <- sapply(1:(length(x_split)), function(j){
          return(max(sum(datShipPro_i[which(datShipPro_i$Shipping_DT == x_split[j]), "Qty"]), 0))
        })
        return(matrix(n_ship, nrow=1))
      })
      for (l in 1:length(nList)){
        colnames(nList[[l]]) <- x_mid[1:(length(x_mid))]
      }
      names(nList) <- uniqueProduct
      #       elected <- selectNi(dataM = dataM, YMD = ymd, minNi = 5)
      elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5)
    }else{
      elected <- matrix(0, ncol = 7, nrow = 0)
    }
  }else{
    elected <- matrix(0, ncol = 7, nrow = 0)
  }
  return(elected)
  TC2 <- proc.time()
  TC2 - TC1
})  
stopCluster(cl)



###
pt2 <- system.time({
for (pro in 1:100){
  print(pro/length(compNameAppear))
  print(paste(pro, length(compNameAppear), sep = "/"))
  componentName <- compNameAppear[pro]
  tM1 <- proc.time()
  #   dataM <- dataArr(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
  dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
  tM2 <- proc.time()
  tM2 - tM1
}
})

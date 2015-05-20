selectNi <- function(dataM, YMD, minNi = 5, rmaNonparametric = rmaNonparametric){
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
      dataComp_c <- dataM[[2]]
      #########
      x <- as.character(sapply(x1, function(y){
        tmp <- strsplit(y, "-")[[1]]
        tmp[3] <- "01"
        tmp2 <- paste(tmp[1], tmp[2], sep="/")
        return(tmp2)
      }))
      if (minD != 1){
        x <- c(x, YMD)
      }
      xDate <- x[1:length(x) - 1]
      
      #----- selection mechanism
      tmpEst <- 0
      tmpLower <- 0
      tmpUpper <- 0
      tmpTrendmv <- 0
      tmpEstM <- 0
      
      if (length(xDate) < 25){
        twoPeriod <- length(xDate)
      }else{
        twoPeriod <- 24
      }
      
      for (i in 2:twoPeriod){
        tmpStore <- apply(rmaNonparametric(xDate[i], dataM, minNi = minNi, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x, endMonth = endMonth), 1, sum)
        tmpEst[i] <- tmpStore[1]
        tmpLower[i] <- tmpStore[2]
        tmpUpper[i] <- tmpStore[3]
        tmpTrendmv[i] <- tmpStore[1]
        tmpEstM[i] <- tmpStore[4]
        
        if (i > 10){
          mean1 <- mean(n_break[(i - 1):(i - 5)])
          mean2 <- mean(n_break[(i - 2):(i - 6)])
          mean3 <- mean(n_break[(i - 3):(i - 7)])
          mean4 <- mean(n_break[(i - 4):(i - 8)])
          mean5 <- mean(n_break[(i - 5):(i - 9)])
          #-------------------
          
          ft1 <- mean2 - mean1
          ft2 <- mean3 - mean2
          ft3 <- mean4 - mean3
          ft4 <- mean5 - mean4
          ftMean <- mean(c(ft1, ft2, ft3, ft4))
          tmpTrendmv[i] <- mean(c(mean1 + ftMean,  sum(tmpStore[[1]])))
        }
      }
      
      if (length(xDate) > 24){
        for (i in 25:length(xDate)){
          tmpStore <- apply(rmaNonparametric(xDate[i], dataM, minNi = minNi, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x, endMonth = endMonth), 1, sum)
          tmpEst[i] <- tmpStore[1]
          tmpLower[i] <- tmpStore[2]
          tmpUpper[i] <- tmpStore[3]
          tmpTrendmv[i] <- tmpStore[1]
          tmpEstM[i] <- tmpStore[4]
          #
          mean1 <- mean(n_break[(i - 1):(i - 5)])
          mean2 <- mean(n_break[(i - 2):(i - 6)])
          mean3 <- mean(n_break[(i - 3):(i - 7)])
          mean4 <- mean(n_break[(i - 4):(i - 8)])
          mean5 <- mean(n_break[(i - 5):(i - 9)])
          
          ft1 <- mean2 - mean1
          ft2 <- mean3 - mean2
          ft3 <- mean4 - mean3
          ft4 <- mean5 - mean4
          ftMean <- mean(c(ft1, ft2, ft3, ft4))
          tmpTrendmv[i] <- mean(c(mean1 + ftMean,  sum(tmpStore[[1]])))
        }
      }
      
      EstStorage <- matrix(c(tmpEst, tmpLower, tmpUpper, tmpTrendmv, tmpEstM), ncol = 5)
      
      # -----
      Est <- EstStorage[, 1]
      Lower <- EstStorage[, 2]
      Upper <- EstStorage[, 3]
      nb <- c(as.numeric(n_break))
      #----
      MVTrend <- EstStorage[, 4]
      EstModified <- EstStorage[, 5]
      
      dataFrame <- data.frame(x = xDate, nb = nb, Est = Est, Lower = Lower, Upper = Upper, 
                              MVTrend = MVTrend, EstModified = EstModified, Empirical = dataM[[6]])
      ## use time series to let the estimation close to the truth.
      ## ind is set as 30, because the frequency in time series is set as 12, it need at least 2 period.
      ind <- 30
      est.ts <- rep(0, nrow(dataFrame))
      numOfTraceback <- 2
      if (nrow(dataFrame) >= ind){
        est.ts[1:(ind - 1)] <- dataFrame[1:(ind - 1), "EstModified"]
        #   current <- which(dataFrame[, 1] == currentDate)
        current <- nrow(dataFrame)
        if (length(current) == 0){
          for (r in ind:nrow(dataFrame)){
            tmpTab <- dataFrame[1:(r - 1), ]
            endD <- as.character(tmpTab[(r - 1), 1])
            enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
            breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
            fitB <- stl(breakTS, s.window="period")
            estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
            fitE <- stl(estTS, s.window="period")  
            diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
            dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
            est.ts[r] <- dataFrame[r, "EstModified"] - dValue
          }
        }else{
          for (r in ind:nrow(dataFrame)){
            if (r <= current){
              tmpTab <- dataFrame[1:(r - 1), ]
              endD <- as.character(tmpTab[(r - 1), 1])
              enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
              breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitB <- stl(breakTS, s.window="period")
              estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitE <- stl(estTS, s.window="period")  
              diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
              dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
              est.ts[r] <- dataFrame[r, "EstModified"] - dValue
            }else{
              tmpTab <- dataFrame[1:current, ]
              endD <- as.character(tmpTab[current, 1])
              enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
              breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitB <- stl(breakTS, s.window="period")
              estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitE <- stl(estTS, s.window="period")  
              diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
              dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
              est.ts[r] <- dataFrame[r, "EstModified"] - dValue
            }
          }
        }
      }
      
      neg <- which(est.ts < 0)
      if (length(neg) > 0){est.ts[neg] <- 0}
      dataFrame <- cbind(dataFrame, EstTs = est.ts)
      #         elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC. = rmaNonparametricC, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x)
      #       elected <- matrix(0, ncol = 7, nrow = 1)
    }else{
      dataFrame <- matrix(0, ncol = 7, nrow = 1)
    }
  }else{
    dataFrame <- matrix(0, ncol = 7, nrow = 1)
  }
  return(dataFrame)
}
selectNi2 <- function(dataM, YMD, minNi = 5, rmaNonparametricC = rmaNonparametricC){
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
      dataComp_c <- dataM[[2]]
      #########
      x <- as.character(sapply(x1, function(y){
        tmp <- strsplit(y, "-")[[1]]
        tmp[3] <- "01"
        tmp2 <- paste(tmp[1], tmp[2], sep="/")
        return(tmp2)
      }))
      if (minD != 1){
        x <- c(x, YMD)
      }
      xDate <- x[1:length(x) - 1]
      
      #----- selection mechanism
      tmpEst <- 0
      tmpLower <- 0
      tmpUpper <- 0
      tmpTrendmv <- 0
      tmpEstM <- 0
      
      if (length(xDate) < 25){
        twoPeriod <- length(xDate)
      }else{
        twoPeriod <- 24
      }
      
      for (i in 2:twoPeriod){
        tmpStore <- apply(rmaNonparametricC(xDate[i], dataM, minNi = minNi, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x, endMonth = endMonth), 1, sum)
        tmpEst[i] <- tmpStore[1]
        tmpLower[i] <- tmpStore[2]
        tmpUpper[i] <- tmpStore[3]
        tmpTrendmv[i] <- tmpStore[1]
        tmpEstM[i] <- tmpStore[4]
        
        if (i > 10){
          mean1 <- mean(n_break[(i - 1):(i - 5)])
          mean2 <- mean(n_break[(i - 2):(i - 6)])
          mean3 <- mean(n_break[(i - 3):(i - 7)])
          mean4 <- mean(n_break[(i - 4):(i - 8)])
          mean5 <- mean(n_break[(i - 5):(i - 9)])
          #-------------------
          
          ft1 <- mean2 - mean1
          ft2 <- mean3 - mean2
          ft3 <- mean4 - mean3
          ft4 <- mean5 - mean4
          ftMean <- mean(c(ft1, ft2, ft3, ft4))
          tmpTrendmv[i] <- mean(c(mean1 + ftMean,  sum(tmpStore[[1]])))
        }
      }
      
      if (length(xDate) > 24){
        for (i in 25:length(xDate)){
          tmpStore <- apply(rmaNonparametricC(xDate[i], dataM, minNi = minNi, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x, endMonth = endMonth), 1, sum)
          tmpEst[i] <- tmpStore[1]
          tmpLower[i] <- tmpStore[2]
          tmpUpper[i] <- tmpStore[3]
          tmpTrendmv[i] <- tmpStore[1]
          tmpEstM[i] <- tmpStore[4]
          #
          mean1 <- mean(n_break[(i - 1):(i - 5)])
          mean2 <- mean(n_break[(i - 2):(i - 6)])
          mean3 <- mean(n_break[(i - 3):(i - 7)])
          mean4 <- mean(n_break[(i - 4):(i - 8)])
          mean5 <- mean(n_break[(i - 5):(i - 9)])
          
          ft1 <- mean2 - mean1
          ft2 <- mean3 - mean2
          ft3 <- mean4 - mean3
          ft4 <- mean5 - mean4
          ftMean <- mean(c(ft1, ft2, ft3, ft4))
          tmpTrendmv[i] <- mean(c(mean1 + ftMean,  sum(tmpStore[[1]])))
        }
      }
      
      EstStorage <- matrix(c(tmpEst, tmpLower, tmpUpper, tmpTrendmv, tmpEstM), ncol = 5)
      
      # -----
      Est <- EstStorage[, 1]
      Lower <- EstStorage[, 2]
      Upper <- EstStorage[, 3]
      nb <- c(as.numeric(n_break))
      #----
      MVTrend <- EstStorage[, 4]
      EstModified <- EstStorage[, 5]
      
      dataFrame <- data.frame(x = xDate, nb = nb, Est = Est, Lower = Lower, Upper = Upper, 
                              MVTrend = MVTrend, EstModified = EstModified, Empirical = dataM[[6]])
      ## use time series to let the estimation close to the truth.
      ## ind is set as 30, because the frequency in time series is set as 12, it need at least 2 period.
      ind <- 30
      est.ts <- rep(0, nrow(dataFrame))
      numOfTraceback <- 2
      if (nrow(dataFrame) >= ind){
        est.ts[1:(ind - 1)] <- dataFrame[1:(ind - 1), "EstModified"]
        #   current <- which(dataFrame[, 1] == currentDate)
        current <- nrow(dataFrame)
        if (length(current) == 0){
          for (r in ind:nrow(dataFrame)){
            tmpTab <- dataFrame[1:(r - 1), ]
            endD <- as.character(tmpTab[(r - 1), 1])
            enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
            breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
            fitB <- stl(breakTS, s.window="period")
            estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
            fitE <- stl(estTS, s.window="period")  
            diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
            dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
            est.ts[r] <- dataFrame[r, "EstModified"] - dValue
          }
        }else{
          for (r in ind:nrow(dataFrame)){
            if (r <= current){
              tmpTab <- dataFrame[1:(r - 1), ]
              endD <- as.character(tmpTab[(r - 1), 1])
              enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
              breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitB <- stl(breakTS, s.window="period")
              estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitE <- stl(estTS, s.window="period")  
              diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
              dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
              est.ts[r] <- dataFrame[r, "EstModified"] - dValue
            }else{
              tmpTab <- dataFrame[1:current, ]
              endD <- as.character(tmpTab[current, 1])
              enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
              breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitB <- stl(breakTS, s.window="period")
              estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
              fitE <- stl(estTS, s.window="period")  
              diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
              dValue <- mean(diffValue[(length(diffValue) - numOfTraceback):length(diffValue)])
              est.ts[r] <- dataFrame[r, "EstModified"] - dValue
            }
          }
        }
      }
      
      neg <- which(est.ts < 0)
      if (length(neg) > 0){est.ts[neg] <- 0}
      dataFrame <- cbind(dataFrame, EstTs = est.ts)
      #         elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC. = rmaNonparametricC, uniqueProduct = uniqueProduct, nList = nList, x_mid = x_mid, x = x)
      #       elected <- matrix(0, ncol = 7, nrow = 1)
    }else{
      dataFrame <- matrix(0, ncol = 7, nrow = 1)
    }
  }else{
    dataFrame <- matrix(0, ncol = 7, nrow = 1)
  }
  return(dataFrame)
}
selectNiC <- cmpfun(selectNi2)

#
###
library(rjson)
library(snow)
cl <- makeCluster(4,type="MPI")
varlist <- list("compNameAppear", "dataArrC", "dat_all", "dat_com", "dat_shipping", "dat_future_shipping", 
                "ymd", "toJSON", "selectNiC", "rmaNonparametricC", "probMappingC")
# varlist <- list("compNameAppear", "dataArrC", "dat_all", "dat_com", "dat_shipping", "dat_future_shipping", 
#                 "ymd", "toJSON", "selectNi", "rmaNonparametric", "probMappingC")
clusterExport(cl, varlist, envir = .GlobalEnv)
pt2 <- system.time({
  output2 <- parLapply(cl, 1:10, function(pro){
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    #    
    #     elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC. = rmaNonparametricC)
    elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC = rmaNonparametricC)
    return(elected)
  })
})
stopCluster(cl)

pt1 <- system.time({
  output1 <- sapply(1:10, function(pro){
    print(pro)
    componentName <- compNameAppear[pro]
    dataM <- dataArrC(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
    elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC = rmaNonparametricC)
    return(elected)
  })
})
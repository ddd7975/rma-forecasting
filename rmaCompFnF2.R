library(Rmpi)
library(snow)
library(RODBC)
conn <- odbcConnect(dsn = "eRMA", uid = "David79", pwd = "d@v1d79")

dat_all <- sqlQuery(conn, "SELECT Product_Name, Order_DT, Barcode, Warranty_DT, Receive_DT, 
                    Ship_DT, MES_Shipping_DT, Order_No, item_No, warranty_Type
                    FROM [SBA].[dbo].[eRMA_All]")

dat_com <- sqlQuery(conn, "SELECT [Order_No]
                    ,[Item_No]
                    ,[PartNumber]
                    ,[Qty]
                    FROM [SBA].[dbo].[eRMA_Consumption_All]")

dat_shipping <- sqlQuery(conn, "SELECT [Shipping_DT], [Product_Name], [Qty]
                         FROM [SBA].[dbo].[EAI_Shipping]")

save(dat_all, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_all.RData")
save(dat_com, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_com.RData")
# save(dat_com, file = "C:/Users/David79.Tseng/Dropbox/David79.Tseng/advantechProject/RMA/dat_com.RData")
save(dat_shipping, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_future_shipping.RData")

load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_com.RData")
load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_all.RData")
load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_shipping.RData")

# ----- prediction of future (by month)
dat_future_shipping <- read.csv("C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\advantechProject\\RMA\\futureNshippingTest.csv", 
                                header = TRUE)
colnames(dat_future_shipping) <- c("Shipping_DT", "Product_Name", "Qty")
# ------------------------------
# ------------------------------
library(dplyr)
# ------------------------------
# ----- Data Arrangement
dataArr <- function(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = YMD){
  dat_all[] <- lapply(dat_all, as.character)
  dat_com[] <- lapply(dat_com, as.character)
  dat_com_i <- dat_com[which(dat_com$PartNumber == componentName), ] # decide a part number
  qty <- as.character(dat_com_i$Qty) 
  qty[which(qty == "")] <- 1 # if qty is blank, change it to 1
  qty[which(qty == "1-")] <- -1 # if qty is "1-", change it to 1
  positiveQtyNo <- which(as.numeric(qty) > 0) # 
  dat_com_i$Qty <- qty
  dat_com_iPos <- dat_com_i[positiveQtyNo, ]
  
  orderNoCom <- as.character(dat_com_iPos$Order_No) # the Order_No which qty > 0
  dat_all_i <- dat_all[which(dat_all$Order_No %in% orderNoCom), ] # pick the dat_all_i which order_No belong to 'Order_No in dat_com'
  doa <- which(dat_all_i[, "warranty_Type"] == "DOA")
  if (length(doa) != 0){
    dat_all_i <- dat_all_i[-doa, ]
  }
  dat_all_i <- dat_all_i[, -10]
  
  rdel <- which(is.na(dat_all_i$MES_Shipping_DT) == T | is.na(dat_all_i$Warranty_DT) == T | 
                  dat_all_i$MES_Shipping_DT == "" | dat_all_i$Warranty_DT == "")
  
  if (length(rdel) != 0){dat_all_i <- dat_all_i[-rdel, ]} # data for use
  
  #
  # warranty type
  #
  #cl <- makeCluster(4, type="SOCK")
  #clusterExport(cl, list = c("dat_all_i", "dat_com_iPos"), envir=environment())
  warranty_Type <- sapply(1:nrow(dat_all_i), function(i){
    orderDT <- as.character(dat_all_i[i, "Order_DT"])
    tmpOrder <- strsplit(orderDT, "\\/|\\:|\\-|\\ ")[[1]]
    if (as.numeric(tmpOrder[1]) == max(suppressWarnings(as.numeric(tmpOrder)), na.rm = T)){
      orderDate <- strptime(paste(tmpOrder[1:3], collapse = "/"), "%Y/%m/%d")
    }else{
      orderDate <- strptime(paste(tmpOrder[1:3], collapse = "/"), "%m/%d/%Y")
    }
    warrDT <- as.character(dat_all_i[i, "Warranty_DT"])
    tmpWarr <- strsplit(warrDT, "\\/|\\:|\\-|\\ ")[[1]]
    if (as.numeric(tmpWarr[1]) == max(suppressWarnings(as.numeric(tmpWarr)), na.rm = T)){
      warrantyDate <- strptime(paste(tmpWarr[1:3], collapse = "/"), "%Y/%m/%d")
    }else{
      warrantyDate <- strptime(paste(tmpWarr[1:3], collapse = "/"), "%m/%d/%Y")
    }
    if (orderDate < warrantyDate){
      return("In")
    }else{
      return("Out")
    }
  })
  
  ###--- need to speed up 
  belongQty <- sapply(1:nrow(dat_all_i), function(i){
    r <- dat_all_i[i, ]
    dattmp <- dat_com_iPos[which(dat_com_iPos$Order_No == r$Order_No & dat_com_iPos$Item_No == r$item_No), ]
    
    if (nrow(dattmp) != 0){
      if (nrow(dattmp) == 1){
        return(dattmp$Qty)
      }else{
        return(sum(as.numeric(dattmp$Qty)))
      }
    }else{
      return(NA)
    }
  })
  dataComp <- cbind(dat_all_i[which(!is.na(belongQty)), ], qty = belongQty[!is.na(belongQty)], warrantyType = warranty_Type[!is.na(belongQty)])
  #############################################
  ##                                         ##
  ## 將warranty的日資料加上MES_Shipping_DT上 ##
  ##                                         ##  
  #############################################
  warranty_ch <- as.character(dataComp$Warranty_DT)
  MES_ch <- as.character(dataComp$MES_Shipping_DT)
  w_day <- 0 
  for (i in 1:length(warranty_ch)){
    tmp <- suppressWarnings(as.numeric(strsplit(warranty_ch[i], "\\/|\\-|:| ")[[1]]))
    if (length(tmp) == 6){tmp <- tmp[1:3]}
    if (max(tmp, na.rm=T) == tmp[1]){
      w_day[i] = tmp[3]
      month <- tmp[2]
    }else{
      w_day[i] = tmp[2]
      month <- tmp[1]
    }
    if (w_day[i] == 31){w_day[i] <- 30}
    if (w_day[i] == 0){w_day[i] <- 15}
    if (as.numeric(month) == 2){w_day[i] <- 27}
  }
  
  MES_ch_withDay <- sapply(1:length(MES_ch), function(i)paste(MES_ch[i], w_day[i], sep="/"))
  #   MES_ch_withDay <- 0
  #   for (i in 1:length(MES_ch)){
  #     MES_ch_withDay[i] <- paste(MES_ch[i], w_day[i], sep="/")
  #   }
  
  MES_ch_withDay <- sapply(1:length(MES_ch), function(i)MES_ch_withDay[i] <- paste(MES_ch[i], w_day[i], sep="/"))
  shipDT <- MES_ch_withDay[MES_ch_withDay != ""]
  msh <- as.character(min(as.Date(shipDT), na.rm = T))
  minShip <- paste(strsplit(msh, split = "-")[[1]], collapse = "/")
  tmp1 <- strsplit(minShip, "/")[[1]]
  minY <- as.numeric(tmp1[1]); minM <- as.numeric(tmp1[2]); minD <- as.numeric(tmp1[3])
  if (minD >= 25){
    minD <- 24
  }
  
  time1 <- strptime(as.character(dataComp$Receive_DT), "%Y/%m/%d") # Receive date (the date that receiving the product from customer)
  time2 <- strptime(as.character(MES_ch_withDay), "%Y/%m/%d") # Shipping date (send the product at beginning)
  #----- if MES_ch_withDay has NA, then remove.
  if (length(which(is.na(time2))) != 0){
    time1 <- time1[-which(is.na(time2))]
    dataComp <- dataComp[-which(is.na(time2)), ]
    MES_ch_withDay <- MES_ch_withDay[-which(is.na(time2))]
    time2 <- time2[-which(is.na(time2))]
  }
  lf <- sapply(1:length(time1), function(i)time1[i] - time2[i])
  
  dat_tmp <- cbind(dataComp, lifeTime = as.numeric(lf), MES_Shipping_Dt_withDay = MES_ch_withDay) 
  tempdel <- which(dat_tmp$lifeTime < 0 | is.na(lf))
  if (length(tempdel) != 0){
    dataComp_c <- dat_tmp[-tempdel, ]
  }else{
    dataComp_c <- dat_tmp
  }
  lf_p <- lf[-tempdel] # life time positive
  time2_s <- as.character(time2[-tempdel]) # time1 send (exclude the wierd value)
  dat_censored1 <- dat_tmp[which(is.na(lf)), ] # Has shipping date, no receive date
  # -----
  # -----
  # -----
  endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
  x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                         as.Date(endMonth), "months"))
  x <- as.character(sapply(x1, function(y){
    tmp <- strsplit(y, "-")[[1]]
    tmp[3] <- "01"
    tmp2 <- paste(tmp[1], tmp[2], tmp[3], sep="/")
    return(tmp2)
  }))
  if (minD != 1){
    x <- c(x, YMD)
  }
  #
  # remove data which is "out of warranty".
  #
  outOfWarranty <- which(dataComp_c[, "warrantyType"] == "Out")
  if (length(outOfWarranty) != 0){
    dataOFW <- dataComp_c[-outOfWarranty, ]  
  }else{
    dataOFW <- dataComp_c
  }
  
  # ----- n_break
  receiveDt_timeform <- as.numeric(strptime(dataOFW$Receive_DT, "%Y/%m/%d"))
  numeric.qty <- as.numeric(dataOFW$qty)
  CountReturn <- function(time1, time2, dat){
    tmp <- which(time1 <= receiveDt_timeform & 
                   receiveDt_timeform < time2)
    if (length(tmp) != 0){
      n <- sum(numeric.qty[tmp])
    }else{
      n <- 0
    }
    return(n)
  }
  x_timeForm <- as.numeric(strptime(x[-length(x)], "%Y/%m/%d"))
  n_break <- 0
  for (i in 1:(length(x_timeForm))){
    n_break[i] <- CountReturn(x_timeForm[i], x_timeForm[i + 1], dataOFW)
  }
  n_break <- matrix(n_break, nrow=1)
  colnames(n_break) <- x[1:(length(x) - 1)]
  
  #-----
  compBelongProduct <- as.character(unique(dataComp_c$Product_Name))
  datShipProBefore <- dat_shipping[which(dat_shipping$Product_Name %in% compBelongProduct), 1:3]
  
  futureShipProIndex <- which(dat_future_shipping$Product_Name %in% compBelongProduct)
  if (length(futureShipProIndex) != 0){
    datShipPro <- rbind(datShipProBefore, dat_future_shipping[futureShipProIndex, ])
  }else{
    datShipPro <- datShipProBefore
  }
  
  #---- method 2: moving average
  estEmpirical <- 0
  for (i in 1:length(n_break)){
    if(i < 4){
      estEmpirical[i] <- 0
    }else{
      estEmpirical[i] <- mean(c(n_break[i - 1], n_break[i - 2], n_break[i - 3]))
    }
  }
  
  return(list(c(minY, minM, minD), dataComp_c, datShipPro, dat_censored1, n_break, estEmpirical))
}
#
# probility mapping function
probMapping <- function(tpIni, tpEnd, index, ftab, uniTimePointWithZero){
  if (tpIni %in% uniTimePointWithZero & tpEnd %in% uniTimePointWithZero){
    row1 <- ftab[which(uniTimePointWithZero == tpIni), ]
    row2 <- ftab[which(uniTimePointWithZero == tpEnd), ]
    f1 <- row1[index]; f2 <- row2[index]
    if (f1 == 0){
      iniRow1 <- iniRow2 <- which(uniTimePointWithZero == tpIni)
      while(ftab[iniRow1, index] == 0){
        if (iniRow1 == 1)break
        iniRow1 <- iniRow1 - 1
      }
      while(ftab[iniRow2, index] == 0){
        if (iniRow2 == nrow(ftab))break
        iniRow2 <- iniRow2 + 1
      }
      proportion <- (tpIni - uniTimePointWithZero[iniRow1])/(uniTimePointWithZero[iniRow2] - uniTimePointWithZero[iniRow1])
      f1 <- ftab[iniRow1, index] + proportion*(max(ftab[iniRow2, index] - ftab[iniRow1, index], 0))
    }
    #-----
    if (f2 == 0){
      endRow1 <- endRow2 <- which(uniTimePointWithZero == tpEnd)
      while(ftab[endRow1, index] == 0){
        if (endRow1 == 1)break
        endRow1 <- endRow1 - 1
      }
      while(ftab[endRow2, index] == 0){
        if (endRow2 == nrow(ftab))break
        endRow2 <- endRow2 + 1
      }
      proportion <- (tpEnd - uniTimePointWithZero[endRow1])/(uniTimePointWithZero[endRow2] - uniTimePointWithZero[endRow1])
      f2 <- ftab[endRow1, index] + proportion*(max(ftab[endRow2, index] - ftab[endRow1, index], 0))
    }
    
    prob <- f2 - f1
  }else{
    if (length(which(uniTimePointWithZero <= tpIni)) == 0){
      iniNear1 <- min(uniTimePointWithZero)
    }else{
      iniNear1 <- uniTimePointWithZero[max(which(uniTimePointWithZero <= tpIni))]  
    }
    
    iniNear2 <- uniTimePointWithZero[min(which(uniTimePointWithZero > tpIni))]
    #         f11 <- ftab[which(uniTimePoint == iniNear1), index]
    #         f12 <- ftab[which(uniTimePoint == iniNear2), index]
    f11 <- ftab[which(uniTimePointWithZero == iniNear1), index]
    f12 <- ftab[which(uniTimePointWithZero == iniNear2), index]
    
    if (f11 == 0){
      iniRow1 <- iniRow2 <- which(uniTimePointWithZero == iniNear1)
      ###
      while(ftab[iniRow1, index] == 0){
        if (iniRow1 == 1)break
        iniRow1 <- iniRow1 - 1
      }
      ###
      while(ftab[iniRow2, index] == 0){
        if (iniRow2 == nrow(ftab))break
        iniRow2 <- iniRow2 + 1
      }
      proportion <- (iniNear1 - uniTimePointWithZero[iniRow1])/(uniTimePointWithZero[iniRow2] - uniTimePointWithZero[iniRow1])
      f11 <- ftab[iniRow1, index] + proportion*max((ftab[iniRow2, index] - ftab[iniRow1, index]), 0)
    }
    if (f12 == 0){
      iniRow1 <- iniRow2 <- which(uniTimePointWithZero == iniNear2)
      while(ftab[iniRow1, index] == 0){
        if (iniRow1 == 1)break
        iniRow1 <- iniRow1 - 1
      }
      while(ftab[iniRow2, index] == 0){
        if (iniRow2 == nrow(ftab))break
        iniRow2 <- iniRow2 + 1
      }
      proportion <- (iniNear2 - uniTimePointWithZero[iniRow1])/(uniTimePointWithZero[iniRow2] - uniTimePointWithZero[iniRow1])
      f12 <- ftab[iniRow1, index] + proportion*max((ftab[iniRow2, index] - ftab[iniRow1, index]), 0)
    }      
    
    if (length(which(uniTimePointWithZero <= tpIni)) == 0){
      proportion <- 1
    }else{
      proportion <- (tpIni - iniNear1)/(iniNear2 - iniNear1)
    }
    prob_tpIni <- f11 + proportion*(f12 - f11)
    # -----
    
    if (length(which(uniTimePointWithZero <= tpIni)) == 0){
      endNear1 <- min(uniTimePointWithZero)
    }else{
      endNear1 <- uniTimePointWithZero[max(which(uniTimePointWithZero < tpEnd))]
    }
    
    endNear2 <- uniTimePointWithZero[min(which(uniTimePointWithZero > tpEnd))]
    f21 <- ftab[which(uniTimePointWithZero == endNear1), index]
    f22 <- ftab[which(uniTimePointWithZero == endNear2), index]
    
    if (f21 == 0){
      endRow1 <- endRow2 <- which(uniTimePointWithZero == endNear1)
      while(ftab[endRow1, index] == 0){
        if (endRow1 == 1)break
        endRow1 <- endRow1 - 1
      }
      while(ftab[endRow2, index] == 0){
        if (endRow2 == nrow(ftab))break
        endRow2 <- endRow2 + 1
      }
      proportion <- (endNear1 - uniTimePointWithZero[endRow1])/(uniTimePointWithZero[endRow2] - uniTimePointWithZero[endRow1])
      f21 <- ftab[endRow1, index] + proportion*max((ftab[endRow2, index] - ftab[endRow1, index]), 0)
    }
    if (f22 == 0){
      endRow1 <- endRow2 <- which(uniTimePointWithZero == endNear2)
      while(ftab[endRow1, index] == 0){
        if (endRow1 == 1)break
        endRow1 <- endRow1 - 1
      }
      while(ftab[endRow2, index] == 0){
        if (endRow2 == nrow(ftab))break
        endRow2 <- endRow2 + 1
      }
      proportion <- (endNear2 - uniTimePointWithZero[endRow1])/(uniTimePointWithZero[endRow2] - uniTimePointWithZero[endRow1])
      f22 <- ftab[endRow1, index] + proportion*max((ftab[endRow2, index] - ftab[endRow1, index]), 0)
    }      
    if (length(which(uniTimePointWithZero <= tpEnd)) == 0){
      proportion <- 1
    }else{
      proportion <- (tpEnd - endNear1)/(endNear2 - endNear1)
    }
    
    prob_tpEnd <- f21 + proportion*(f22 - f21)
    prob <- prob_tpEnd - prob_tpIni
  }
  return(prob)    
}

# making nList (need: datShipPro, YMD, )
datShipPro <- dataM[[3]]
uniqueProduct <- as.character(unique(datShipPro$Product_Name))
#
minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
dataComp_c <- dataM[[2]]
datShipPro <- dataM[[3]]
dat_censored1 <- dataM[[4]]
n_break <- dataM[[5]]

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
# ----- Nonparametric Estimation
# add input: currentDate
rmaNonparametric <- function(currentDate = currentDate, dataM, alpha = 0.05, minNi = 5){
  # ----- Make the table, 1st column is time point, 2nd column is attribute. 
  # ----- 1. attribute = 1 means failure data
  # ----- 2. attribute = 2 means censored data in failure sheet 
  # ----- 3. attribute = 3 means censored data in dat_shipping (need to multiple the amount)
  cur <- paste(currentDate, "15", sep = "/")
  nListCur <- lapply(1:length(uniqueProduct), function(u){
    nList[[u]][1, 1:which(x_mid == cur)]
  })
  x_midCur <- x_mid[1:which(x_mid == cur)]
  xCur <- x[1:which(x_mid == cur)]
  endCurrent <- seq(as.Date(paste(c(currentDate, "01"), collapse = "/")), length = 2, by = "months")[2]
  Est <- EstLower <- EstUpper <- 0
  EstM <- EstMw <- 0
  for (num in 1:length(uniqueProduct)){
    n_ship <- nListCur[[num]]
    proName <- uniqueProduct[num]
    ##
    ## for censored 
    ##
    lf_nonBroken <- as.numeric(strptime(endCurrent, "%Y-%m-%d") - strptime(x_midCur, "%Y/%m/%d"))
    dat_attr3 <- as.data.frame(cbind(lifeTime = lf_nonBroken, attribute = rep("3", length(lf_nonBroken))))
    ##
    ## ---- dataComp_c includes all the data, it need to remove the Receive_DT after YMD.
    ##
    dataComp_c_pro <- dataComp_c[which(dataComp_c$Product_Name == proName), ]
    part <- which(strptime(endCurrent, "%Y-%m-%d") - strptime(dataComp_c_pro$Receive_DT, "%Y/%m/%d") > 0)
    dataComp_c_part <- dataComp_c_pro[part, ]
    if (nrow(dataComp_c_part) != 0){
      lfBreak <- rep(dataComp_c_part$lifeTime, dataComp_c_part$qty)
      dat_attr1 <- as.data.frame(cbind(lifeTime = lfBreak, attribute = rep("1", length(lfBreak))))
      # to know which month this product is broken
      lf_Month <- strptime(endCurrent, "%Y-%m-%d") - strptime(paste(xCur, "01", sep = "/"), "%Y/%m/%d")
      belongMonth <- strptime(endCurrent, "%Y-%m-%d") - strptime(dataComp_c_part$MES_Shipping_Dt_withDay, "%Y/%m/%d")
      belongTime <- 0
      if (length(belongMonth) != 0){
        for (i in 1:length(belongMonth)){
          belongTime[i] <- lf_nonBroken[max(which(belongMonth[i] <= lf_Month))]
        }
      }
      #
      censoredPro <- dat_censored1[which(dat_censored1$Product_Name == proName), ]
      if (nrow(censoredPro) != 0){
        lf_censored1 <- strptime(rep(currentDate, nrow(censoredPro)), "%Y/%m/%d") - strptime(censoredPro$MES_Shipping_Dt_withDay, "%Y/%m/%d")
        ##
        ## ---- Because lf_censored1 is build before input the YMD, so it may include the shipping date after than the YMD, so the value will be negative.
        ##
        lf_censored1_part <- lf_censored1[which(lf_censored1 > 0)]
        dat_attr2 <- as.data.frame(cbind(lifeTime = as.numeric(lf_censored1_part), attribute = rep("2", length(lf_censored1_part))))
      }else{
        dat_attr2 <- NULL
      }
      tmpTable <- rbind(dat_attr1, dat_attr2, dat_attr3)
      tmpTable[, 1] <- as.numeric(as.character(tmpTable[, 1]))  
      tmpTableOrder <- tmpTable[order(tmpTable[, 1]), ]
      ind <- which(strptime(x_mid, "%Y/%m/%d") < strptime(endCurrent, "%Y-%m-%d"))
      n <- sum(n_ship[ind])
      ##
      ## ----- combine the same lifeTime and same attribute together
      ##
      uniTimePoint <- unique(tmpTableOrder[, 1])  
      failureTable <- matrix(0, ncol = 7, nrow = (length(uniTimePoint) + 1))
      failureTable[1, 3] <- n
      ########
      ########
      ########
      for (i in 2:(length(uniTimePoint) + 1)){
        #for (i in 38:38){
        tab <- tmpTableOrder[which(tmpTableOrder[, 1] == uniTimePoint[i - 1]), ]
        failureTable[i, 1] <- sum((tab[, 2] == 1))      # di
        failureTable[i, 2] <- sum((tab[, 2] == 2))      # ri (in failure data)
        
        # attr == 3
        if (3 %in% tab$attribute){
          locCensor <- which(lf_nonBroken == tab[1, 1])
          nCensor <- n_ship[locCensor]
          failureTable[i, 2] <- failureTable[i, 2] + nCensor     # ri (attr 3)
        }
      }
      rownames(failureTable) <- c(0, uniTimePoint)
      colnames(failureTable) <- c("di", "ri", "ni", "1-pi", "F(ti)", "Lower", "Upper")
      
      for (l in 1:length(belongTime)){
        loc <- which(belongTime[l] == c(0, uniTimePoint))
        tmp[l] <- loc
        failureTable[loc, 2] <- failureTable[loc, 2] - 1
      }
      
      if (min(failureTable[, 2]) < 0){
        failureTable[1, 3] <- failureTable[1, 3] - sum(failureTable[which(failureTable[, 2] < 0), 2])
        failureTable[which(failureTable[, 2] < 0), 2] <- 0
      }
      
      for (i in 2:(length(uniTimePoint) + 1)){
        failureTable[i, 3] <- failureTable[i - 1, 3] - sum(failureTable[i - 1, 1] + failureTable[i - 1, 2])    # ni  
      }
      
      ########
      ########
      ########
      if (min(failureTable[, 3]) < 0){
        failureTable[, 3] <- failureTable[, 3] - min(failureTable[, 3])
      }
      # -----
      for (i in 1:nrow(failureTable)){
        if (failureTable[i, 1] > 0 & failureTable[i, 3] > 0){
          failureTable[i, 4] <- 1 - (failureTable[i, 1] / failureTable[i, 3])
        }
      }
      
      nonZero <- which(failureTable[, 1] != 0)
      if (length(nonZero) == 0){
        sti <- 0
        fti <- 1
      }else{
        sti <- numeric(length = length(nonZero) + 1)
        sti[1] <- 1
        for (j in 2:(length(nonZero) + 1)){
          sti[j] <- sti[j - 1] * failureTable[which(rownames(failureTable) == names(nonZero[j - 1])), 4]
        }
        sti <- sti[-1]
        fti <- 1 - sti
      }
      failureTable[which(failureTable[, 1] != 0), 5] <- fti
      
      nj <- failureTable[which(failureTable[, 1] != 0), 3]
      p <- 1 - failureTable[which(failureTable[, 1] != 0), 4]
      tmp1 <- p/(n*(1-p))
      var_Fti  <- 0
      for (i in 1:length(fti)){
        var_Fti[i] <- (sti[i])^2*sum(tmp1[1:i])
      }
      z <- qnorm(1 - alpha)
      # ----- 1. log transformation
      w <- exp(z*sqrt(var_Fti)/(fti*(1 - fti)))
      Fti_Lower <- fti/(fti + (1 - fti)*w)
      Fti_Upper <- fti/(fti + (1 - fti)/w)
      
      # ----- 2. large sample (assume it is normal distribution)
      #   Fti_Lower <- max(fti - z*sqrt(var_Fti), 0)
      #   Fti_Upper <- min(fti + z*sqrt(var_Fti), 1)
      failureTable[which(failureTable[, 1] != 0), 6] <- Fti_Lower
      failureTable[which(failureTable[, 1] != 0), 7] <- Fti_Upper
      failureTable[which(failureTable[, 4] == 0 & failureTable[, 1] > 0), 6:7] <- 1
      
      # ----- If the number of sacrificer are less, it may effect the estimation a lot.(failure probability will too high.)
      # ----- It still need discussion.
      lastDiLargeN <- which(failureTable[, 3] > minNi & failureTable[, 1] > 0)
      if (length(lastDiLargeN) != 0){
        for (j in 5:7){
          for (m in max(lastDiLargeN):nrow(failureTable)){
            failureTable[m, j] <- failureTable[lastDiLargeN[length(lastDiLargeN)], j]
          }
        }
        
        ##
        ## failureTable has only one di, so the F(ti) will be the same, it will let the prob all be zero.
        ##
        for (j in 5:7){
          if (failureTable[1, 1] == 0){
            if (length(which(failureTable[, j] > 0)) > 0){
              for (r in 2:(min(lastDiLargeN) - 1)){
                proportion <- c(0, uniTimePoint)[r]/(c(0, uniTimePoint)[min(lastDiLargeN)])
                failureTable[r, 5] <- proportion*failureTable[min(lastDiLargeN), 5]
              }
            }
          }
        }
      }
      ## ----- Start to build the process of calculation 
      ## unit: day
      ## Add the currently ()
      timePoint <- rev(c(strptime(endMonth, "%Y-%m-%d") - strptime(colnames(n_ship), "%Y/%m/%d"), 0))
      ##
      ## probMapping is a function that can map a given timeRange(tpIni, tpEnd) to failureTable to get the probability.
      ##
      uniTimePointWithZero <- c(0, uniTimePoint)
      timePoint <- c(0, rev(lf_nonBroken))
      failureTableModified <- failureTable
      if (sum(failureTable[, 1] != 0) > 10){
        Y <- failureTableModified[which(failureTableModified[, 1] != 0), 5]
        X <- as.numeric(rownames(failureTableModified)[which(failureTableModified[, 1] != 0)])
        #
        # remove the max value
        #
        X <- X[-which(Y == max(Y))]
        Y <- Y[-which(Y == max(Y))]
        fit <- lm(Y ~ X)
        #
        # model:y = ax + b
        #
        a <- fit$coefficients[2]; b <- fit$coefficients[1]
        #
        # last di location 
        #
        lastDi <- max(which(failureTable[, 1] != 0))
        fr <- a*uniTimePointWithZero[(lastDi+1):nrow(failureTable)] + b
        failureTableModified[(lastDi+1):nrow(failureTable), 5] <- fr
      }
      
      if (sum(failureTable[, 1] == 0) == nrow(failureTable)){
        probVector <- probVectorM <- probVectorLower <- probVectorUpper <- rep(0, length(timePoint) - 1)
      }else{
        #
        # ----- Estimation
        #
        probVector <- 0
        for (i in 1:(length(timePoint) - 1)){
          probVector[i] <- probMapping(timePoint[i], timePoint[i + 1], 5, failureTable, uniTimePointWithZero)
        }
        
        probVectorM <- 0
        for (i in 1:(length(timePoint) - 1)){
          probVectorM[i] <- probMapping(timePoint[i], timePoint[i + 1], 5, failureTableModified, uniTimePointWithZero)
        }
        #
        # ----- LowerBound
        #
        probVectorLower <- 0
        for (i in 1:(length(timePoint) - 1)){
          probVectorLower[i] <- 0
        }
        #
        # ----- UpperBound
        #
        probVectorUpper <- 0
        for (i in 1:(length(timePoint) - 1)){
          probVectorUpper[i] <- 0
        }
      }
      lenLimit <- length(which(probVector < 1))
      est <- sum((rev(n_ship) * probVector)[1:lenLimit], na.rm = T)
      estLower <- sum((rev(n_ship) * probVectorLower)[1:lenLimit], na.rm = T)
      estUpper <- sum((rev(n_ship) * probVectorUpper)[1:lenLimit], na.rm = T)
      #
      timeDiff <- strptime(paste(currentDate, "/01", sep = ""), "%Y/%m/%d") - strptime(rev(colnames(n_ship)), "%Y/%m/%d")
      restrict <- which(timeDiff < 720)
      estM <- sum((rev(n_ship)*probVectorM)[restrict], na.rm = T)
    }else{
      est = estLower = estUpper = estM = 0
    }
    Est[num] <- est
    EstLower[num] <- estLower
    EstUpper[num] <- estUpper
    #
    # failure rate modified
    #
    EstM[num] <- estM
    
  }
  return(list(Est, EstLower, EstUpper, EstM))
}
# ----- Selection mechanism
selectNi <- function(dataM, YMD, minNi = 5, currentDate){
  n_break <- dataM[[5]]
  minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
  
  endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
  x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                         as.Date(endMonth), "months"))
  x <- as.character(sapply(x1, function(y){
    tmp <- strsplit(y, "-")[[1]]
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
    tmpStore <- rmaNonparametric(xDate[i], dataM, minNi = minNi)
    tmpEst[i] <- sum(tmpStore[[1]])
    tmpLower[i] <- sum(tmpStore[[2]])
    tmpUpper[i] <- sum(tmpStore[[3]])
    tmpTrendmv[i] <- sum(tmpStore[[1]])
    tmpEstM[i] <- sum(tmpStore[[4]])
    
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
      tmpStore <- rmaNonparametric(xDate[i], dataM, minNi = minNi)
      tmpEst[i] <- sum(tmpStore[[1]])
      tmpEstM[i] <- sum(tmpStore[[4]])
      
      tmpLower[i] <- sum(tmpStore[[2]])
      tmpUpper[i] <- sum(tmpStore[[3]])
      
      #-----
      tmpTrendmv[i] <- sum(tmpStore[[1]])
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
  est.ts[1:(ind - 1)] <- dataFrame[1:(ind - 1), "EstModified"]
  current <- which(dataFrame[, 1] == currentDate)
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
      dValue <- mean(diffValue[(length(diffValue) - 1):length(diffValue)])
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
        dValue <- mean(diffValue[(length(diffValue) - 1):length(diffValue)])
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
        dValue <- mean(diffValue[(length(diffValue) - 1):length(diffValue)])
        est.ts[r] <- dataFrame[r, "EstModified"] - dValue
      }
    }
  }
  
  neg <- which(est.ts < 0)
  if (length(neg) > 0){est.ts[neg] <- 0}
  dataFrame <- cbind(dataFrame, EstTs = est.ts)
  return(dataFrame)
}
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------
#---------------------------------------------------------------------------

# input1: date
currentDate <- "2015/07" # for simulation
ymd <- "2015/07"        # for last estimation
# input2: component name
componentName <- "1400000907"
componentName <- "1400004141"
componentName <- "1410022431"
componentName <- "AIMB-210G2-S6A1E" # not bad! golden is better!
componentName <- "PCM-3375F-L0A1E" # not bad! golden is better!
componentName <- "SQF-P10S2-4G-ETE" # not good...under estimate
componentName <- "AIMB-762G2-00A1E" # need to take some time
componentName <- "96DR-512M333NN-AP2" # time consuming
componentName <- "PCA-6006LV-00B1" # ni = 65, failure rate too high... not good
componentName <- "PCM-9375F-J0A1E" # red and golden are all over estimate 
componentName <- "PCA-6003V-00A2E" # under estimate, no much difference between red and golden, take some time
componentName <- "96MI-3OP-P2-AV" # amount too small
componentName <- "PCA-6187VE-00A2E"
componentName <- "1410006740" # can't judge which one is better, cum of gold will get up in the end
componentName <- "9698967103E" # golden's est is good, a little bit underestimate
componentName <- "PPC-102S-BARE-T" # n_break is zero...(remove DOA and out of warranty), golden est is zero!
componentName <- "IPC-610P4-250-E" # n_break is zero...(remove DOA and out of warranty), golden est is zero!
componentName <- "1701440159"
componentName <- "XZFR-S-5158" # data too narrow
componentName <- "1330000985"
componentName <- "96VG-256M-P-SP"
componentName <- "PCI-1721-AE" # too small
componentName <- "96HD750G-ST-SG7K" # too small
componentName <- "1124518191"
componentName <- "1653000016"
componentName <- "9680013278"
componentName <- "14S4860600"

t1 = proc.time()
tM1 <- proc.time()
dataM <- dataArr(dat_all = dat_all, dat_com = dat_com, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = ymd)
tM2 <- proc.time()
tM2 - tM1
datShipPro <- dataM[[3]]
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
elected <- selectNi(dataM = dataM, YMD = ymd, minNi = 5, currentDate = currentDate)
t2 = proc.time()
t2 - t1
##
##
plot(1:nrow(elected), elected[, "nb"], 
     xlab = "Date", ylab = "Amount", main = componentName,
     pch = 16, type = "b", lty = 2, 
     ylim = c(min(c(as.numeric(unlist(elected[, c(2, 3, 5, 6, 7)])))), max(c(as.numeric(unlist(elected[, c(2, 3, 5, 6, 7)]))))))
lines(1:nrow(elected), elected[, "Est"], col = "red", lwd = 2, type = "o")
lines(1:nrow(elected), elected[, "Empirical"], col = "blue", lwd = 2, type = "o")
lines(1:nrow(elected), elected[, "MVTrend"], col = "darkolivegreen", lwd = 2, type = "o")
lines(1:nrow(elected), elected[, "EstModified"], col = "darkgoldenrod", lwd = 2, type = "o")
lines(1:nrow(elected), elected[, "EstTs"], col = "purple", lwd = 2, type = "o")
legend("topleft", c("True", "Empirical", "Nonparametric", "MVTrend", "LinearEst", "LinearTsEst"), 
       lty = c(2, 1, 1, 1, 1, 1), col =c("black", "blue",  "red", "darkolivegreen", "darkgoldenrod", "purple"), 
       lwd = c(2, 2, 2, 2, 2, 2))  

cumulatedNon <- cumsum(elected[, "Est"] - elected[, "nb"])
cumulatedEmp <- cumsum(elected[, "Empirical"] - elected[, "nb"])
cumulatedMVTrend <- cumsum(elected[, "MVTrend"] - elected[, "nb"])
cumulatedM <- cumsum(elected[, "EstModified"] - elected[, "nb"])
cumulatedTs <- cumsum(elected[, "EstTs"] - elected[, "nb"])
##
##
plot(1:nrow(elected), rep(0, nrow(elected)), type = "l", pch = 0, 
     ylim = c(min(c(cumulatedNon, cumulatedEmp, cumulatedMVTrend, cumulatedM, cumulatedTs)), 
              max(c(cumulatedNon, cumulatedEmp, cumulatedMVTrend, cumulatedM, cumulatedTs))),
     xlab = "Date", ylab = "Cumulated difference", 
     main = componentName)
lines(1:nrow(elected), cumulatedNon, col = "red", lwd = 2)
lines(1:nrow(elected), cumulatedEmp, col = "blue", lwd = 2)
lines(1:nrow(elected), cumulatedMVTrend, col = "darkolivegreen", lwd = 2)
lines(1:nrow(elected), cumulatedM, col = "darkgoldenrod", lwd = 2)
lines(1:nrow(elected), cumulatedTs, col = "purple", lwd = 2)
legend("bottomleft", c("True", "Empirical", "Nonparametric", "MVTrend", "LinearEst"), 
       lty = c(2, 1, 1, 1, 1, 1), col =c("black", "blue",  "red", "darkolivegreen", "darkgoldenrod", "purple"), 
       lwd = c(2, 2, 2, 2, 2, 2))  
##
##
mse <- c(sum((as.numeric(elected[, "Empirical"]) - as.numeric(elected[, "nb"]))^2),
         sum((as.numeric(elected[, "Est"]) - as.numeric(elected[, "nb"]))^2),
         sum((as.numeric(elected[, "MVTrend"]) - as.numeric(elected[, "nb"]))^2),
         sum((as.numeric(elected[, "EstModified"]) - as.numeric(elected[, "nb"]))^2),
         sum((as.numeric(elected[, "EstTs"]) - as.numeric(elected[, "nb"]))^2))
averageShortage <- c(abs(mean(cumulatedEmp)),
                     abs(mean(cumulatedNon)),
                     abs(mean(cumulatedMVTrend)),
                     abs(mean(cumulatedM)),
                     abs(mean(cumulatedTs)))

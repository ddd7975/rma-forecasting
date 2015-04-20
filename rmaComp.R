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

# save(dat_all, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_all.RData")
# save(dat_com, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_com.RData")
# # save(dat_com, file = "C:/Users/David79.Tseng/Dropbox/David79.Tseng/advantechProject/RMA/dat_com.RData")
# save(dat_shipping, file = "C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_future_shipping.RData")

load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_com.RData")
load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_all.RData")
load("C:/Users/David79.Tseng/Dropbox/HomeOffice/rmaTest/dat_shipping.RData")

# ----- prediction of future (by month)
dat_future_shipping <- read.csv("C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\advantechProject\\RMA\\futureNshippingTest.csv", 
                                header = TRUE)
colnames(dat_future_shipping) <- c("Shipping_DT", "Product_Name", "Qty")
# ------------------------------
# ------------------------------
# ------------------------------
# ----- Data Arrangement
dataArr <- function(dat_all = dat_all, dat_shipping = dat_shipping, dat_future_shipping = dat_future_shipping, componentName = componentName, YMD = YMD){
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
  warranty_Type <- 0
  for (i in 1:nrow(dat_all_i)){
    orderDT <- as.character(dat_all_i[i, "Order_DT"])
    tmpOrder <- strsplit(orderDT, "\\/|\\:|\\-|\\ ")[[1]]
    tmpOrder <- paste(tmpOrder[1:3], collapse = "/")
    warrDT <- as.character(dat_all_i[i, "Warranty_DT"])
    tmpWarr <- strsplit(warrDT, "\\/|\\:|\\-|\\ ")[[1]]
    tmpWarr <- paste(tmpWarr[1:3], collapse = "/")
    if (as.numeric(strsplit(orderDT, "\\/|\\:|\\-|\\ ")[[1]][1]) == max(as.numeric(strsplit(orderDT, "\\/|\\:|\\-|\\ ")[[1]]))){
      orderDate <- strptime(tmpOrder, "%Y/%m/%d")
    }else{
      orderDate <- strptime(tmpOrder, "%m/%d/%Y")
    }
    if (as.numeric(strsplit(warrDT, "\\/|\\:|\\-|\\ ")[[1]][1]) == max(as.numeric(strsplit(warrDT, "\\/|\\:|\\-|\\ ")[[1]]), na.rm = T)){
      warrantyDate <- strptime(tmpWarr, "%Y/%m/%d")
    }else{
      warrantyDate <- strptime(tmpWarr, "%m/%d/%Y")
    }
    
    
    if (orderDate < warrantyDate){
      warranty_Type[i] <- "In"
    }else{
      warranty_Type[i] <- "Out"
    }
  }
  
  belongQty <- 0
  for (i in 1:nrow(dat_all_i)){
    print(i/nrow(dat_all_i))
    r <- dat_all_i[i, ]
    # same order_no and same item_no
    dattmp <- dat_com_iPos[which(dat_com_iPos$Order_No == as.character(r$Order_No) & dat_com_iPos$Item_No == as.character(r$item_No)), ]
    
    if (nrow(dattmp) != 0){
      if (nrow(dattmp) == 1){
        belongQty[i] <- dattmp$Qty
      }else{
        belongQty[i] <- sum(as.numeric(dattmp$Qty))
      }
    }else{
      belongQty[i] <- NA
    }
  }
  belongQty <- as.numeric(belongQty)
  dataComp <- cbind(dat_all_i[which(!is.na(belongQty)), ], qty = belongQty[!is.na(belongQty)], warrantyType = warranty_Type[!is.na(belongQty)])
  
  #############################################
  ##                                         ##
  ## 將warranty的日資料加上MES_Shipping_DT上 ##
  ##                                         ##  
  #############################################
  warranty_ch <- as.character(dataComp$Warranty_DT)
  MES_ch <- as.character(dataComp$MES_Shipping_DT)
  w_day <- 0 # Warranty day
  for (i in 1:length(warranty_ch)){
    #     print(i/length(warranty_ch))
    print(i/length(warranty_ch))
    tmp <- as.numeric(strsplit(warranty_ch[i], "\\/|\\-|:| ")[[1]])
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
  
  MES_ch_withDay <- 0
  for (i in 1:length(MES_ch)){
    print(i/length(MES_ch))
    MES_ch_withDay[i] <- paste(MES_ch[i], w_day[i], sep="/")
  }
  
  #   shipDT <- dat_pca$Ship_DT[which(dat_pca$Ship_DT != "")]
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
  lf <- 0
  for (i in 1:length(time1)){
    print(i/length(time1))
    lf[i] <- time1[i] - time2[i]
  }
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
  # ----- n_break
  CountReturn <- function(time1, time2, dat){
    tc1 <- as.numeric(strptime(time1, "%Y/%m/%d"))
    tc2 <- as.numeric(strptime(time2, "%Y/%m/%d"))
    tmp <- which(tc1 <= as.numeric(strptime(dat$Receive_DT, "%Y/%m/%d")) & 
                   as.numeric(strptime(dat$Receive_DT, "%Y/%m/%d")) < tc2)
    n <- sum(dat[tmp, "qty"])
    return(n)
  }
  endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
  x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
                         as.Date(endMonth), "months"))
  #   x1 <- as.character(seq(as.Date(paste(c(minY, minM, minD), collapse = "/")), 
  #                          Sys.Date(), "months"))
  x <- as.character(sapply(x1, function(y){
    tmp <- strsplit(y, "-")[[1]]
    tmp[3] <- "01"
    tmp2 <- paste(tmp[1], tmp[2], tmp[3], sep="/")
    return(tmp2)
  }))
  if (minD != 1){
    x <- c(x, YMD)
  }
  n_break <- 0
  #
  # remove data which is "out of warranty".
  #
  outOfWarranty <- which(dataComp_c[, "warrantyType"] == "Out")
  if (length(outOfWarranty) != 0){
    dataOFW <- dataComp_c[-outOfWarranty, ]  
  }else{
    dataOFW <- dataComp_c
  }
  
  for (i in 1:(length(x) - 1)){
    print(i/(length(x) - 1))
    n_break[i] <- CountReturn(x[i], x[i + 1], dataOFW)
  }
  n_break <- matrix(n_break, nrow=1)
  colnames(n_break) <- x[1:(length(x) - 1)]
  
  #-----
  compBelongProduct <- as.character(unique(dataComp_c$Product_Name))
  datShipProBefore <- dat_shipping[which(dat_shipping$Product_Name %in% compBelongProduct), ]
  
  futureShipProIndex <- which(dat_future_shipping$Product_Name %in% compBelongProduct)
  if (length(futureShipProIndex) != 0){
    datShipPro <- rbind(datShipProBefore, dat_future_shipping[futureShipProIndex, ])
  }
  
  #---- method 2: moving average
  estEmpirical <- 0
  for (i in 1:length(n_break)){
    print(i/length(n_break))
    if(i < 4){
      estEmpirical[i] <- 0
    }else{
      estEmpirical[i] <- mean(c(n_break[i - 1], n_break[i - 2], n_break[i - 3]))
    }
  }
  
  return(list(c(minY, minM, minD), dataComp_c, datShipPro, dat_censored1, n_break, dat_future_shipping_pro, estEmpirical))
}
# ----- Nonparametric Estimation
rmaNonparametric <- function(YMD, dataM, alpha = 0.05, minNi = 5){
  minY <- dataM[[1]][1]; minM <- dataM[[1]][2]; minD <- dataM[[1]][3]
  dataComp_c <- dataM[[2]]
  datShipPro <- dataM[[3]]
  dat_censored1 <- dataM[[4]]
  n_break <- dataM[[5]]
  
  endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[2]
  #   endMonth <- seq(as.Date(paste(c(YMD, "01"), collapse = "/")), length = 2, by = "months")[1]
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
  
  uniqueProduct <- as.character(unique(datShipPro$Product_Name))
  nList <- list()
  for (i in 1:length(uniqueProduct)){
    print(i/length(uniqueProduct))
    proName <- uniqueProduct[i]
    datShipPro_i <- datShipPro[which(datShipPro$Product_Name == proName), ]
    n_ship <- 0
    for (j in 1:(length(x_split) - 1)){
      xi <- x_split[j]
      n_ship[j] <- sum(datShipPro_i[which(datShipPro_i$Shipping_DT == xi), "Qty"])
      if (n_ship[j] < 0)n_ship[j] <- 0
    }
    n_ship <- matrix(n_ship, nrow=1)
    nList[[i]] <- n_ship
  }
  
  x_mid <- sapply(1:length(x), function(i){
    tmpd <- strsplit(x[i], "/")[[1]]
    tmpd[3] <- "15"
    paste(tmpd, collapse = "/")
  })
  for (l in 1:length(nList)){
    colnames(nList[[l]]) <- x_mid[1:(length(x_mid) - 1)]
  }
  if (is.null(dataM[[6]]) == FALSE){
    for (n in 1:nrow(dataM[[6]])){
      n_ship[which(colnames(n_ship) == dataM[[6]][n, 1])] <- as.numeric(dataM[[6]][n, 3])
    }
  }
  names(nList) <- uniqueProduct
  # ----- Make the table, 1st column is time point, 2nd column is attribute. 
  # ----- 1. attribute = 1 means failure data
  # ----- 2. attribute = 2 means censored data in failure sheet 
  # ----- 3. attribute = 3 means censored data in dat_shipping (need to multiple the amount)
  Est <- EstLower <- EstUpper <- 0
  EstM <- EstMw <- 0
  for (num in 1:length(uniqueProduct)){
    n_ship <- nList[[num]]
    proName <- uniqueProduct[num]
    ##
    ## for caculate censored 
    ##
    lf_nonBroken <- as.numeric(strptime(endMonth, "%Y-%m-%d") - strptime(as.character(colnames(n_ship)), "%Y/%m/%d"))
    dat_attr3 <- as.data.frame(cbind(lifeTime = lf_nonBroken, attribute = rep("3", length(lf_nonBroken))))
    ##
    ## ---- dataComp_c includes all the data, it need to remove the Receive_DT after YMD.
    ##
    dataComp_c_pro <- dataComp_c[which(dataComp_c$Product_Name == proName), ]
    part <- which(strptime(x[length(x)], "%Y/%m/%d") - strptime(dataComp_c_pro$Receive_DT, "%Y/%m/%d") > 0)
    dataComp_c_part <- dataComp_c_pro[part, ]
    dat_attr1 <- as.data.frame(cbind(lifeTime = dataComp_c_part$lifeTime, attribute = rep("1", length(dataComp_c_part$lifeTime))))
    ##
    ##
    censoredPro <- dat_censored1[which(dat_censored1$Product_Name == proName), ]
    if (nrow(censoredPro) != 0){
      lf_censored1 <- strptime(rep(YMD, nrow(censoredPro)), "%Y/%m/%d") - strptime(censoredPro$MES_Shipping_Dt_withDay, "%Y/%m/%d")
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
    ind <- which(strptime(colnames(n_ship), "%Y/%m/%d") < strptime(endMonth, "%Y-%m-%d"))
    n <- sum(n_ship[ind])
    ##
    ## ----- combine the same lifeTime and same attribute together
    ##
    uniTimePoint <- unique(tmpTableOrder[, 1])  
    failureTable <- matrix(0, ncol = 7, nrow = (length(uniTimePoint) + 1))
    failureTable[1, 3] <- n
    
    for (i in 2:(length(uniTimePoint) + 1)){
      #       print(i/(length(uniTimePoint) + 1))
      tab <- tmpTableOrder[which(tmpTableOrder[, 1] == uniTimePoint[i - 1]), ]
      failureTable[i, 1] <- sum((tab[, 2] == 1))      # di
      failureTable[i, 2] <- sum((tab[, 2] == 2))      # ri
      
      # attr == 3
      if (3 %in% tab$attribute){
        censorDate <- colnames(n_ship)[which(tab[which(tab[, 2] == 3), 1] == lf_nonBroken)]
        dieBetween <- which(strptime(censorDate, format = "%Y/%m/%d") < strptime(dataComp_c_part$MES_Shipping_Dt_withDay, "%Y/%m/%d") & 
                              strptime(dataComp_c_part$MES_Shipping_Dt_withDay, "%Y/%m/%d") < seq(from = strptime(censorDate, format = "%Y/%m/%d"), length = 2, by = "months")[2])
        if (length(dieBetween) != 0){
          qtyBetween <- sum(as.numeric(dataComp_c_part[dieBetween, "qty"]))
          if (n_ship[which(colnames(n_ship) == censorDate)] != 0){
            nShip_censore <- n_ship[which(colnames(n_ship) == censorDate)] - qtyBetween
          }else{
            nShip_censore <- 0
          }
          failureTable[i, 2] <- failureTable[i, 2] + nShip_censore     # ri (attr 3)
        }
      }
      
      failureTable[i, 3] <- failureTable[i - 1, 3] - sum(failureTable[i - 1, 1] + failureTable[i - 1, 2])    # ni
    }
    
    rownames(failureTable) <- c(0, uniTimePoint)
    colnames(failureTable) <- c("di", "ri", "ni", "1-pi", "F(ti)", "Lower", "Upper")
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
        print(j/(length(nonZero) + 1))
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
    probMapping <- function(tpIni, tpEnd, index, ftab){
      if (tpIni %in% uniTimePoint & tpEnd %in% uniTimePoint){
        row1 <- ftab[which(uniTimePoint == tpIni), ]
        row2 <- ftab[which(uniTimePoint == tpEnd), ]
        f1 <- row1[index]; f2 <- row2[index]
        if (f1 == 0){
          iniRow1 <- iniRow2 <- which(uniTimePoint == tpIni)
          while(ftab[iniRow1, index] == 0){
            if (iniRow1 == 1)break
            iniRow1 <- iniRow1 - 1
          }
          while(ftab[iniRow2, index] == 0){
            if (iniRow2 == nrow(ftab))break
            iniRow2 <- iniRow2 + 1
          }
          proportion <- (tpIni - uniTimePoint[iniRow1])/(uniTimePoint[iniRow2] - uniTimePoint[iniRow1])
          f1 <- ftab[iniRow1, index] + proportion*(ftab[iniRow2, index] - ftab[iniRow1, index])
        }
        #-----
        if (f2 == 0){
          endRow1 <- endRow2 <- which(uniTimePoint == tpEnd)
          while(ftab[endRow1, index] == 0){
            if (endRow1 == 1)break
            endRow1 <- endRow1 - 1
          }
          while(ftab[endRow2, index] == 0){
            if (endRow2 == nrow(ftab))break
            endRow2 <- endRow2 + 1
          }
          proportion <- (tpEnd - uniTimePoint[endRow1])/(uniTimePoint[endRow2] - uniTimePoint[endRow1])
          f2 <- ftab[endRow1, index] + proportion*(ftab[endRow2, index] - ftab[endRow1, index])
        }
        
        prob <- f2 - f1
      }else{
        if (length(which(uniTimePoint <= tpIni)) == 0){
          iniNear1 <- min(uniTimePoint)
        }else{
          iniNear1 <- uniTimePoint[max(which(uniTimePoint <= tpIni))]  
        }
        
        iniNear2 <- uniTimePoint[min(which(uniTimePoint > tpIni))]
        f11 <- ftab[which(uniTimePoint == iniNear1), index]
        f12 <- ftab[which(uniTimePoint == iniNear2), index]
        
        if (f11 == 0){
          iniRow1 <- iniRow2 <- which(uniTimePoint == iniNear1)
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
          proportion <- (iniNear1 - uniTimePoint[iniRow1])/(uniTimePoint[iniRow2] - uniTimePoint[iniRow1])
          f11 <- ftab[iniRow1, index] + proportion*(ftab[iniRow2, index] - ftab[iniRow1, index])
        }
        if (f12 == 0){
          iniRow1 <- iniRow2 <- which(uniTimePoint == iniNear2)
          while(ftab[iniRow1, index] == 0){
            if (iniRow1 == 1)break
            iniRow1 <- iniRow1 - 1
          }
          while(ftab[iniRow2, index] == 0){
            if (iniRow2 == nrow(ftab))break
            iniRow2 <- iniRow2 + 1
          }
          proportion <- (iniNear2 - uniTimePoint[iniRow1])/(uniTimePoint[iniRow2] - uniTimePoint[iniRow1])
          f12 <- ftab[iniRow1, index] + proportion*(ftab[iniRow2, index] - ftab[iniRow1, index])
        }      
        
        if (length(which(uniTimePoint <= tpIni)) == 0){
          proportion <- 1
        }else{
          proportion <- (tpIni - iniNear1)/(iniNear2 - iniNear1)
        }
        prob_tpIni <- f11 + proportion*(f12 - f11)
        # -----
        
        if (length(which(uniTimePoint <= tpIni)) == 0){
          endNear1 <- min(uniTimePoint)
        }else{
          endNear1 <- uniTimePoint[max(which(uniTimePoint < tpEnd))]
        }
        
        endNear2 <- uniTimePoint[min(which(uniTimePoint > tpEnd))]
        f21 <- ftab[which(uniTimePoint == endNear1), index]
        f22 <- ftab[which(uniTimePoint == endNear2), index]
        
        if (f21 == 0){
          endRow1 <- endRow2 <- which(uniTimePoint == endNear1)
          while(ftab[endRow1, index] == 0){
            if (endRow1 == 1)break
            endRow1 <- endRow1 - 1
          }
          while(ftab[endRow2, index] == 0){
            if (endRow2 == nrow(ftab))break
            endRow2 <- endRow2 + 1
          }
          proportion <- (endNear1 - uniTimePoint[endRow1])/(uniTimePoint[endRow2] - uniTimePoint[endRow1])
          f21 <- ftab[endRow1, index] + proportion*(ftab[endRow2, index] - ftab[endRow1, index])
        }
        if (f22 == 0){
          endRow1 <- endRow2 <- which(uniTimePoint == endNear2)
          while(ftab[endRow1, index] == 0){
            if (endRow1 == 1)break
            endRow1 <- endRow1 - 1
          }
          while(ftab[endRow2, index] == 0){
            if (endRow2 == nrow(ftab))break
            endRow2 <- endRow2 + 1
          }
          proportion <- (endNear2 - uniTimePoint[endRow1])/(uniTimePoint[endRow2] - uniTimePoint[endRow1])
          f22 <- ftab[endRow1, index] + proportion*(ftab[endRow2, index] - ftab[endRow1, index])
        }      
        if (length(which(uniTimePoint <= tpEnd)) == 0){
          proportion <- 1
        }else{
          proportion <- (tpEnd - endNear1)/(endNear2 - endNear1)
        }
        
        prob_tpEnd <- f21 + proportion*(f22 - f21)
        prob <- prob_tpEnd - prob_tpIni
      }
      return(prob)    
    }
    failureTableModified <- failureTable
    if (sum(failureTable[, 1] != 0) > 10){
      y <- failureTableModified[which(failureTableModified[, 1] != 0), 5]
      x <- as.numeric(rownames(failureTableModified)[which(failureTableModified[, 1] != 0)])
      #
      # remove the max value
      #
      x <- x[-which(y == max(y))]
      y <- y[-which(y == max(y))]
      fit <- lm(y ~ x)
      #
      # model:y = ax + b
      #
      a <- fit$coefficients[2]; b <- fit$coefficients[1]
      #
      # last di location 
      #
      lastDi <- max(which(failureTable[, 1] != 0))
      fr <- a*uniTimePoint[(lastDi+1):nrow(failureTable)] + b
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
        probVector[i] <- probMapping(timePoint[i], timePoint[i + 1], 5, failureTable)
      }
      
      probVectorM <- 0
      for (i in 1:(length(timePoint) - 1)){
        probVectorM[i] <- probMapping(timePoint[i], timePoint[i + 1], 5, failureTableModified)
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
    
    Est[num] <- est
    EstLower[num] <- estLower
    EstUpper[num] <- estUpper
    #
    # failure rate modified
    #
    timeDiff <- strptime(paste(YMD, "/01", sep = ""), "%Y/%m/%d") - strptime(rev(colnames(n_ship)), "%Y/%m/%d")
    restrict <- which(timeDiff < 720)
    
    estM <- sum((rev(n_ship)*probVectorM)[restrict], na.rm = T)
    EstM[num] <- estM
  }
  return(list(Est, EstLower, EstUpper, EstM))
}
# ----- Selection mechanism
selectNi <- function(dataM, YMD, maxNi = 5){
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
  EstStorage <- list()
  for (k in 1:maxNi){
    print(k/maxNi)
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
      tmpStore <- rmaNonparametric(xDate[i], dataM, minNi = k)
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
        tmpStore <- rmaNonparametric(xDate[i], dataM, minNi = k)
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
    
    EstStorage[[k]] <- matrix(c(tmpEst, tmpLower, tmpUpper, tmpTrendmv, tmpEstM), ncol = 5)
  }
  
  output <- lapply(1:maxNi, function(k){
    sum((EstStorage[[k]][-(length(EstStorage[[k]][, 1])), 1] - as.numeric((c(0, n_break))[1:(length(xDate) - 1)]))^2)
  })
  
  minNi <- min(which(unlist(output) == min(unlist(output))))
  # -----
  Est <- EstStorage[[minNi]][, 1]
  Lower <- EstStorage[[minNi]][, 2]
  Upper <- EstStorage[[minNi]][, 3]
  nb <- c(as.numeric(n_break))
  #----
  MVTrend <- EstStorage[[minNi]][, 4]
  EstModified <- EstStorage[[minNi]][, 5]
  
  dataFrame <- data.frame(x = xDate, nb = nb, Est = Est, Lower = Lower, Upper = Upper, 
                          MVTrend = MVTrend, EstModified = EstModified, Empirical = dataM[[7]])
  ## use time series to let the estimation close to the truth.
  ## ind is set as 30, because the frequency in time series is set as 12, it need at least 2 period.
  ind <- 30
  est.ts <- rep(0, nrow(dataFrame))
  est.ts[1:(ind - 1)] <- dataFrame[1:(ind - 1), "EstModified"]
  for (r in ind:nrow(dataFrame)){
    #     print(r/nrow(dataFrame))
    tmpTab <- dataFrame[1:(r - 1), ]
    endD <- as.character(tmpTab[(r - 1), 1])
    enddate <- as.numeric(strsplit(endD[length(endD)], "/")[[1]])
    breakTS <- ts(tmpTab[, "nb"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
    fitB <- stl(breakTS, s.window="period")
    estTS <- ts(tmpTab[, "EstModified"], start=c(minY, minM), end=c(enddate[1], enddate[2]), frequency=12) 
    fitE <- stl(estTS, s.window="period")  
    diffValue <- (fitE$time.series[, "trend"] - fitB$time.series[, "trend"])
    dValue <- mean(diffValue[(length(diffValue) - 4):length(diffValue)])
    est.ts[r] <- dataFrame[r, "EstModified"] - dValue
  }
  neg <- which(est.ts < 0)
  if (length(neg) > 0){est.ts[neg] <- 0}
  dataFrame <- cbind(dataFrame, EstTs = est.ts)
  return(dataFrame)
}
# ----- ggplot 
ggRma <- function(elected){
  library(ggplot2)
  lp <- ggplot(elected, aes(x, group = 1))
  lp + geom_line(linetype="dashed", aes(y = tmpEst, color = 'Estimation')) + 
    labs(x = "Date", y = "Amount") + 
    ggtitle(productName) + 
    geom_point(aes(y = tmpEst, color = 'Estimation')) + 
    geom_line(linetype="dashed", aes(y = nb, color = 'True')) + 
    geom_point(aes(y = nb, color = 'True')) + 
    geom_ribbon(aes(ymin=tmpLower, ymax=tmpUpper), alpha=0.2, fill = 'red') + 
    # 
    geom_point(aes(y = Empirical, color = 'Empirical')) + 
    geom_line(linetype="dashed", aes(y = nb, color = 'True')) + 
    #
    scale_colour_manual(name=c(""), values = c("Estimation"="red", "True"="black"), 
                        labels = c('Estimation', 'True')) + 
    theme(legend.title = element_text(size=16)) + 
    theme(legend.text = element_text(size=16)) +
    theme(axis.text.x = element_text(size=10, angle = 45)) + 
    theme(plot.title = element_text(size=20)) + 
    theme(axis.title.x = element_text(size = 15)) + 
    theme(axis.title.y = element_text(size = 15, angle = 90))
}


EmpiricalValue <- 0
NonparametricValue <- 0
MAValue <- 0
MVTrendValue <- 0
EstModified <- 0
Differnce <- list()
for (i in 1:length(pN)){
  ymd <- "2015/02" # input 1
  #   productName = pN[i]
  #   componentName <- "1254000882"
  #   componentName <- "96FMCFI-1G-CT-SS1"
  #   componentName <- "PCE-5124G2-00A1E"
  #   componentName <- "AIMB-742VE-00A2E"
  #   componentName <- "96HD160G-I-SG7K3" # non performance bad
  #   componentName <- "14S4860600" # time consuming
  #   componentName <- "1330000985"
  #   componentName <- "96DR-512M400NN-TR"
  #   unique(dat_com$PartNumber)[100:200]
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
  componentName <- "XZFR-S-5158" # golden will over estimate, need to check
  componentName <- "1330000985"
  
  dataM <- dataArr(dat_all = dat_all, dat_shipping = dat_shipping, dat_future_shipping = NULL, componentName = componentName, YMD = ymd)
  elected <- selectNi(dataM = dataM, YMD = ymd, maxNi = 1)
  
  #   par(mfrow = c(2, 1))
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
  
  plot(1:nrow(elected), rep(0, nrow(elected)), type = "l", pch = 0, 
       ylim = c(min(c(cumulatedNon, cumulatedEmp, cumulatedMVTrend, cumulatedM)), 
                max(c(cumulatedNon, cumulatedEmp, cumulatedMVTrend, cumulatedM))),
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
  
  
  Differnce[[i]] <- data.frame(Empirical = cumulatedEmp, Nonparametric = cumulatedNon, MA = cumulatedMA, 
                               ZLEMA = cumulatedZLEMA, Fusion = cumulatedFusion, MVTrend = cumulatedMVTrend)
  EmpiricalValue[i] <- sum((as.numeric(elected[, "Empirical"]) - as.numeric(elected[, "nb"]))^2)
  NonparametricValue[i] <- sum((as.numeric(elected[, "Est"]) - as.numeric(elected[, "nb"]))^2)
  MVTrendValue[i] <- sum((as.numeric(elected[, "MVTrend"]) - as.numeric(elected[, "nb"]))^2)
  EstModified[i] <- sum((as.numeric(elected[, "EstModified"]) - as.numeric(elected[, "nb"]))^2)
  EstTs[i] <- sum((as.numeric(elected[, "EstTs"]) - as.numeric(elected[, "nb"]))^2)
}


tab <- data.frame(Empirical = round(EmpiricalValue, 3), 
                  Nonparametric = round(NonparametricValue, 3),
                  MA = round(MAValue, 3), 
                  ZLEMA = round(ZLEMAValue, 3), 
                  Fusion = round(FusionValue, 3), 
                  MVTrend = round(MVTrendValue, 3))
# tab[1, ] <- c(90042.33, 75532.32, 37580.24)
mini <- apply(tab, 1, min)
tab <- cbind(tab, mini)
rownames(tab) <- pN
write.csv(tab, "C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\advantechProject\\RMA\\CompareWithEmpirical\\DistanceCompareV2.csv")



#remove train data
p <- 0.45
trainMonth <- round(nrow(elected)*p)
sum(elected$nb[1:trainMonth])/sum(elected$nb)

sum((as.numeric(elected[1:trainMonth, "Empirical"]) - as.numeric(elected[1:trainMonth, "nb"]))^2)
sum((as.numeric(elected[1:trainMonth, "Est"]) - as.numeric(elected[1:trainMonth, "nb"]))^2)
sum((as.numeric(elected[1:trainMonth, "MVTrend"]) - as.numeric(elected[1:trainMonth, "nb"]))^2)
sum((as.numeric(elected[1:trainMonth, "EstModified"]) - as.numeric(elected[1:trainMonth, "nb"]))^2)
sum((as.numeric(elected[1:trainMonth, "EstTs"]) - as.numeric(elected[1:trainMonth, "nb"]))^2)

testNb <- elected$nb[trainMonth:nrow(elected)]

testNon <- elected$Est[trainMonth:nrow(elected)]
testEmp <- elected$Empirical[trainMonth:nrow(elected)]
testMVTrend <- elected$MVTrend[trainMonth:nrow(elected)]
testModified <- elected$EstModified[trainMonth:nrow(elected)]
testTs <- elected$EstTs[trainMonth:nrow(elected)]
cumNon <- cumsum(testNon - testNb)
cumEmp <- cumsum(testEmp - testNb)
cumMVTrend <- cumsum(testMVTrend - testNb)
cumModified <- cumsum(testModified - testNb)
cumTs <- cumsum(testTs - testNb)


plot(1:nrow(elected), rep(0, nrow(elected)), type = "l", 
     ylim = c(min(cumNon, cumEmp, cumMVTrend, cumModified, cumTs), max(cumNon, cumEmp, cumMVTrend, cumModified, cumTs)), 
     xlab = "Date", ylab = "cumulated difference", 
     main = componentName)
lines(trainMonth:nrow(elected), cumNon, col = "red")
lines(trainMonth:nrow(elected), cumEmp, col = "blue")
lines(trainMonth:nrow(elected), cumMVTrend, col = "darkolivegreen")
lines(trainMonth:nrow(elected), cumModified, col = "darkgoldenrod")
lines(trainMonth:nrow(elected), cumTs, col = "purple")

# 當月餘料
remainNon <- elected$Est - elected$nb
remainEmp <- elected$Empirical - elected$nb
remainMVTrend <- elected$MVTrend - elected$nb
remainMod <- elected$EstModified - elected$nb
remainTs <- elected$EstTs - elected$nb

plot(1:length(remainNon), rep(0, length(remainNon)), type = "l", 
     ylim = c(min(c(remainEmp, remainMVTrend, remainNon, remainMod, remainTs)), max(c(remainEmp, remainMVTrend, remainNon, remainMod, remainTs))), 
     xlab = "Date", ylab = "remaining", 
     main = "1410022431")
lines(1:length(remainNon), remainNon, col = "red")
lines(1:length(remainNon), remainEmp, col = "blue", lwd = 1)
lines(1:length(remainNon), remainMVTrend, col = "darkolivegreen", lwd = 1)
lines(1:length(remainNon), remainMod, col = "darkgoldenrod", lwd = 1)
lines(1:length(remainNon), remainTs, col = "purple", lwd = 1)

var(remainEmp)
var(remainMVTrend)
var(remainNon)

# 缺料次數
trainMonth <- round(nrow(elected)*p)
sum(elected$nb[1:trainMonth])/sum(elected$nb)

remainNon <- elected$Est - elected$nb
remainEmp <- elected$Empirical - elected$nb
remainMVTrend <- elected$MVTrend - elected$nb

sum(remainNon[trainMonth:nrow(elected)] < 0)/length(remainNon)
sum(remainEmp[trainMonth:nrow(elected)] < 0)/length(remainNon)
sum(remainMVTrend[trainMonth:nrow(elected)] < 0)/length(remainNon)

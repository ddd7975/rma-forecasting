## 先把所有函數執行完，以及讀入dat_all, dat_com, dat_shipping, dat_future_shipping這些資料
library(RODBC)
conn <- odbcConnect(dsn = "eRMA", uid = "David79", pwd = "d@v1d79")
dat_all <- sqlQuery(conn, "SELECT Product_Name, Order_DT, Barcode, Warranty_DT, Receive_DT, 
                    Ship_DT, MES_Shipping_DT, Order_No, item_No, warranty_Type, Front_Loc
                    FROM [SBA].[dbo].[eRMA_All]")

dat_com <- sqlQuery(conn, "SELECT [Order_No]
                    ,[Item_No]
                    ,[PartNumber]
                    ,[Qty]
                    FROM [SBA].[dbo].[eRMA_Consumption_All]")

dat_shipping <- sqlQuery(conn, "SELECT [Shipping_DT], [Product_Name], [Qty], [Location]
                         FROM [SBA].[dbo].[EAI_Shipping]")
conn <- odbcConnect(dsn = "ACL_EAI_ACL", uid = "david79", pwd = "dtG79")
dat_future_shipping <- sqlQuery(conn, "select aa.item_no,
                                       Ship_Site,
                                       fact_zone as [Region],
                                       substring(convert(char,efftive_DATE,112),1,6) as [Shipping_DT],
                                       cust_country,
                                       aa.site_id as [site_id],
                                       yesNo [bookOrNot],
                                       bb.DescDesc,
                                       sum(qty) as [Qty]
                                       from zBacklog aa (nolock) left join dimension.dbo.mara2 bb (nolock) 
                                       on aa.item_no = bb.matnr  
                                       where efftive_date between '2015/7/1' and '2018/12/31'  
                                       and tran_type = 'Backlog'  
                                       and fact_1234 = '1'  
                                       and bomseq >= 0  and breakdown <= 0  
                                       and itp_find <> 2  
                                       and itp_find <> 9   
                                       and ( qty <> 0 or us_amt <> 0 ) 
                                       group by aa.item_no,Ship_Site,fact_zone,
                                       substring(convert(char,efftive_DATE,112),1,6),
                                       cust_country,aa.site_id,yesNo,bb.DescDesc 
                                       order by 1,2,3,4,5
                                       ")

save(dat_all, file = "C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rma-forecasting\\rdata\\dat_all.RData")
save(dat_com, file = "C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rma-forecasting\\rdata\\dat_com.RData")
save(dat_shipping, file = "C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rma-forecasting\\rdata\\dat_shipping.RData")
save(dat_future_shipping, file = "C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rma-forecasting\\rdata\\dat_future_shipping.RData")

## scratch data by Region from raw data (dat_all, dat_com, dat_shipping, dat_future_shipping)
ATSC <- c("AAU", "ABR", "ACL", "ADS", "AID", "AIN", "AINS", "AJP", "AKR", "ALNC", "AMY", "APOS", "ASG", "ATH")
atsc.all <- which(dat_all$Front_Loc %in% ATSC)
atsc.shippping <- which(dat_shipping$Location %in% ATSC)
atsc.shippping_future <- which(dat_future_shipping$site_id %in% ATSC)

dat_all_ATSC <- dat_all[atsc.all, ]
dat_shipping_ATSC <- dat_shipping[atsc.shippping, ]
dat_future_shipping_ATSC <- dat_future_shipping[atsc.shippping_future, c("Shipping_DT", "item_no", "Qty")]

nrow(dat_all_ATSC)/nrow(dat_all)
nrow(dat_shipping_ATSC)/nrow(dat_shipping)
nrow(dat_future_shipping_ATSC)/nrow(dat_future_shipping)

###
# input1: date
nowDate <- "2015/07" # for simulation
twoYearDate <- seq(as.Date(paste(c(nowDate, "01"), collapse = "/")), length = 26, by = "months")
ymd <- paste(strsplit(as.character(max(twoYearDate)), "-")[[1]][1:2], collapse = "/")
listfile <- read.csv("C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rmaInventoryList.csv", header = TRUE)
compName <- as.character(listfile[, 1])
nonAppearIndex <- which(compName %in% unique(dat_com$PartNumber))
compNameAppear <- compName[nonAppearIndex]
if ("" %in% compNameAppear){compNameAppear <- compNameAppear[-which(compNameAppear == "")]}

output1 <- lapply(11:20, function(pro){
  print(pro)
  componentName <- compNameAppear[pro]
  dataM <- dataArrC(dat_all = dat_all_ATSC, dat_com = dat_com, dat_shipping = dat_shipping_ATSC, dat_future_shipping = dat_future_shipping_ATSC, componentName = componentName, YMD = ymd)
  elected <- selectNiC(dataM = dataM, YMD = ymd, minNi = 5, rmaNonparametricC = rmaNonparametricC)
  #out <- evalFun(elected, componentName, nowDate)
  out <- cbind(compName = rep(componentName, nrow(elected)), elected)
  return(out)
})





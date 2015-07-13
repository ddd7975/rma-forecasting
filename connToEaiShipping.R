library(RODBC)
conn <- odbcConnect(dsn = "ACL_EAI_ACL", uid = "david79", pwd = "dtG79")

dat_future_shipping <- sqlQuery(conn, "select aa.item_no,
                                       Ship_Site,
                                       fact_zone as [Region],
                                       substring(convert(char,efftive_DATE,112),1,6),
                                       cust_country,
                                       aa.site_id as [公司別],
                                       yesNo [是否book到],
                                       bb.DescDesc,
                                       sum(qty) as [qty]
                                       from zBacklog aa (nolock) left join dimension.dbo.mara2 bb (nolock) 
                                       on aa.item_no = bb.matnr  
                                       where efftive_date between '2015/8/1' and '2018/12/31'  
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



nrow(dat_future_shipping)
length(unique(dat_future_shipping$item_no))

head(dat_future_shipping)

dat_future_shipping <- dat_future_shipping[, c(1, 4, 7)]



###
listfile <- read.csv("C:\\Users\\David79.Tseng\\Dropbox\\David79.Tseng\\git-respository\\rma-forecasting\\rmaInventoryList.csv", header = TRUE)
compName <- as.character(listfile[, 1])
length(unique(dat_future_shipping[, 1])[(which(unique(dat_future_shipping[, 1]) %in% compName))])
length(unique(dat_future_shipping[, 1])[(which(unique(dat_future_shipping[, 1]) %in% dat_com$PartNumber))])
length(which(compName %in% unique(dat_com$PartNumber))) #list 中的product在歷史資料的種類數

###
ATSC <- c("AAU", "ABR", "ACL", "ADS", "AID", "AIN", "AINS", "AJP", "AKR", "ALNC", "AMY", "APOS", "ASG", "ATH")
# which(unique(dat_future_shipping$公司別) %in% ATSC)

dat_future_shipping_ATSC <- dat_future_shipping[which(dat_future_shipping$公司別 %in% ATSC), ]

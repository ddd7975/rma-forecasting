library(RODBC)
conn <- odbcConnect(dsn = "ACL_EAI_ACL", uid = "david79", pwd = "dtG79")

dat_future_shipping <- sqlQuery(conn, "select aa.item_no,
                                       Ship_Site,
                                       fact_zone as [Region],
                                       substring(convert(char,efftive_DATE,112),1,6) as [Date],
                                       cust_country,
                                       yesNo [是否book到],
                                       sum(qty)
                                       from zBacklog aa (nolock) left join dimension.dbo.mara2 bb (nolock) 
                                       on aa.item_no = bb.matnr  
                                       where efftive_date between '2015/7/1' and '2015/7/31'  
                                                                             and tran_type = 'Backlog'  
                                                                             and fact_1234 = '1'  
                                                                             and bomseq >= 0  and breakdown <= 0  
                                                                             and itp_find <> 2  
                                                                             and itp_find <> 9   
                                                                             and ( qty <> 0 or us_amt <> 0 ) 
                                       group by aa.item_no,Ship_Site,fact_zone,
                                       substring(convert(char,efftive_DATE,112),1,6),
                                       cust_country,aa.site_id,yesNo,bb.DescDesc 
                                       order by 1,2,3,4,5")



nrow(dat_future_shipping)
length(unique(dat_future_shipping$item_no))
length(unique(dat_future_shipping[,1]))



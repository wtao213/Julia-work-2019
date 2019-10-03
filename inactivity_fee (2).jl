##


## import all the packages might need
ENV["COLUMNS"]=240
ENV["LINES"] = 50

using CSV
using FreqTables
using StatsBase
using Plots
using ODBC
using DataFrames
using JLD2
using Dates
using Tables
using CategoricalArrays

##
df =CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_apr_jun42k_full.csv",missingstring ="NULL")

[names(df) eltypes(df)]

## histogram plot
 histogram(df[!,:MTD], fillalpha=0.4,linealpha=0.1,title="Tenure",  xlabel="Month",
      ylabel="Client Count",nbins=0:1:155,legend=false)

summarystats(df[!,:MTD])
mode(df[!,:MTD])
freqtable(df[!,:MTD])

histogram(df[!,:join_age], fillalpha=0.4,linealpha=0.1,title="Join age",  xlabel="Month",
           ylabel="Client Count",legend=false)


## put the customer in groups
df[!,:inactivity_fee_grp] = [ x==0 ? "no charge" : x>=24.95 ? "full charge" : "partial charge"
                              for x in df[!,:inactivity_fee_charged]]

df[!,:attrition] = [x==0 ? "stay" : "attrition" for x in df[!,:closed_during_quater]]

## write
CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_apr_jun42k_full_v2.csv",df)



## check the freq table
freqtable(df[!,:inactivity_fee_grp])
freqtable(df[!,:inactivity_fee_grp],df[!,:MTD])



## distribution by charged groups
histogram(df[!,:MTD],group= df[!,:inactivity_fee_grp], fillalpha=0.4,linealpha=0.1,title="Tenure",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155)

histogram(df[!,:MTD],group= df[!,:attrition], fillalpha=0.4,linealpha=0.1,title="Tenure",  xlabel="Month",
          ylabel="Client Count",nbins=0:1:155)

## look at customer tenure distrivution by charged type
no_charge        = df[df.inactivity_fee_grp .== "no charge", :]
full_chrage      = df[df.inactivity_fee_grp .== "full charge", :]
partial_charge   = df[df.inactivity_fee_grp .== "partial charge", :]

p1=histogram(no_charge[!,:MTD], fillalpha=0.4,linealpha=0.1,title="No charge",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)
p2=histogram(full_chrage[!,:MTD], fillalpha=0.4,linealpha=0.1,title="full charge",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)
p3=histogram(partial_charge[!,:MTD], fillalpha=0.4,linealpha=0.1,title="partial charge",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)
plot(p1,p2,p3, layout=(3,1),size=(600,800),tickfontsize=6)


summarystats(no_charge[!,:MTD])
summarystats(full_chrage[!,:MTD])
summarystats(partial_charge[!,:MTD])

mode(no_charge[!,:MTD])
mode(full_chrage[!,:MTD])
mode(partial_charge[!,:MTD])


freqtable(full_chrage[!,:MTD])


## attrition or not by distribution
stay      = df[df.attrition .== "stay", :]
attrition = df[df.attrition .== "attrition", :]
p1=histogram(stay[!,:MTD], fillalpha=0.4,linealpha=0.1,title="stay",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)
p2=histogram(attrition[!,:MTD], fillalpha=0.4,linealpha=0.1,title="attrition",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)
plot(p1,p2, layout=(2,1),size=(600,800),tickfontsize=6)















## pivot table in julia
df2=CSV.read("C:\\Users\\012790\\Desktop\\trade_ct_full_2m.csv")

[ names(df2) eltypes(df2)]

## trade=unstack(df2,:PrimaryClientID,:yr,:trade_time)

trade=unstack(df2,:PrimaryClientID,:time_ind,:trade_time)
CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_trade_full259k_v2.csv",trade)




## join the customer with the trade info
## when left join there are 41,487 customers, when inner, there are 36,912 customers left
## which is 11% of the emailed person who never trade
client_4_6_trade = join(df,trade,on=:PrimaryClientID,kind=:left)

CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_4_6_trade42k.csv",client_4_6_trade)





###########################################################################################################
## replicate everything with the list from oct list
list =CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\list_oct.csv")

## among the  42k client, 25k are duplicate customer from March
dup =join(list,client_4_6_trade,on= :Client_ID => :PrimaryClientID,kind=:inner)

## trade info for oct cusotmer 36,813 for inner join, and
client_oct_trade = join(list,trade,on=:Client_ID => :PrimaryClientID, kind=:left)

CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_oct_trade42k.csv", client_oct_trade)

list=nothing
trade=nothing
df2=nothing













###################################################################################
## Sep, 30th
## import data from planed oct
df= CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_oct_trade42k.csv")
df=client_oct_trade
## get data info
[names(df) eltypes(df)]


freqtable(df[!,:Number_Of_Accounts])


cus= CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\client_full_308k.csv")

## look at overall tenure by customer
[names(cus) eltypes(cus)]
summarystats(cus[!,:MTD])
freqtable(cus[!,:MTD])
histogram(cus[!,:MTD], fillalpha=0.4,linealpha=0.1,title="MTD for all cusotmers",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)

## find the customer act info,41528 customer left
cus_oct=join(df,cus,on=:Client_ID => :PrimaryClientID, kind =:inner,makeunique=true)

######
## import inactivity_Fee info from database
## data preparation
fee= CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\inactivity_fee_customer_90k.csv")
fee= nothing
## becareful about the sequence! of writing your number

cus_oct_v2 = join(cus_oct,fee,on=:Client_ID=>:PrimaryClientID, kind =:left,makeunique=true)

CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\cus_oct.csv", cus_oct_v2)

# import equity
equity=CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\equity_0701_0909_308k.csv")

a= nothing

cus_oct_v2=join(cus_oct_v2,equity,on=:Client_ID=>:PrimaryClientID, kind =:inner,makeunique=true)

## add in second part info
cus_oct_v2=CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\cus_oct.csv",missingstrings=["NULL",""])

cus_p2 = CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\backup_data\\cus_info_p2.csv",missingstrings=["NULL",""])

a=join(cus_oct_v2,cus_p2,on=:Client_ID =>:PrimaryClientID,kind=:inner)


CSV.write("C:\\Users\\012790\\Desktop\\inactivity_fee\\cus_oct_v4.csv", a)












##########################################################################################
## start from here, actual analysis the oct cust
#########################################################################################
cus_oct_v2=CSV.read("C:\\Users\\012790\\Desktop\\inactivity_fee\\cus_oct_v4.csv")
[names(cus_oct_v2) eltypes(cus_oct_v2)]


#############################
## demographic info
## look at customer MTD
summarystats(cus_oct_v2[!,:MTD])
quantile(cus_oct_v2[!,:MTD],(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1))
nquantile(cus_oct_v2[!,:MTD],10)
histogram(cus_oct_v2[!,:MTD], fillalpha=0.4,linealpha=0.1,title="Tenure",  xlabel="Month",
     ylabel="Client Count",nbins=0:1:155,legend=false)



## change label and plot in groups
freqtable(cus_oct_v2[!,:group])

label=["Corhort1 new customer","Corhort2 will get 1st charge","Corhort3 charged 2-3 times","Corhort4 charged 4+ times","Corhort5 accounts low cash"]
histogram(cus_oct_v2[!,:MTD],group = cus_oct_v2[!,:group] , fillalpha=0.4,linealpha=0.1,title="Tenure by Corhort",  xlabel="Month",
          ylabel="Client Count",nbins=0:1:155,label=label)
## get the mode
mode(cus_oct_v2[!,:MTD])
summarystats(cus_oct_v2[!,:MTD])
freqtable(cus_oct_v2[!,:MTD])


## age when join, age currently
a=cus_oct_v2[!,:join_age]
a=[ismissing(x) ? missing : x<26 ? missing : x for x in a]
summarystats(a)
a= nothing

summarystats(cus_oct_v2[!,:join_age])
histogram(cus_oct_v2[!,:join_age], fillalpha=0.4,linealpha=0.1,title="Age when Join",  xlabel="Age",
     ylabel="Client Count",nbins=26:1:93,legend=false)

summarystats(cus_oct_v2[!,:age_today])
histogram(cus_oct_v2[!,:age_today], fillalpha=0.4,linealpha=0.1,title="Current Age",  xlabel="Age now",
          ylabel="Client Count",nbins=26:1:93,legend=false)








##############################################################
## look at how many times be charged for the oct customer
freqtable(cus_oct_v2[!,:charged_time])
freqtable(cus_oct_v2[!,:group])

summarystats(cus_oct_v2[!,:charged_amt_ttl])
## the charged fee distribution plot
histogram(cus_oct_v2[!,:charged_amt_ttl], fillalpha=0.4,linealpha=0.1,title="Charged Fee",  xlabel="CAD",
     ylabel="Client Count",nbins=0:25:850,legend=false)

## change missing to 0
cus_oct_v2[!,:charged_time] = [ ismissing(x) ? 0 : x for x in cus_oct_v2[!,:charged_time]]
histogram(cus_oct_v2[!,:charged_time], fillalpha=0.4,linealpha=0.1,title="Charged Times",  xlabel="Count",
          ylabel="Client Count",nbins=0:1:30,legend=false)


###########################################################
## distibution of equity at start and 0909
summarystats(cus_oct_v2[!,:equity_0701])
histogram(cus_oct_v2[!,:equity_0701], fillalpha=0.4,linealpha=0.1,title="Equity 0701",  xlabel="Equity CAD",
          ylabel="Client Count",nbins=-400:100:6000,legend=false)

histogram(cus_oct_v2[!,:equity_0701],group=cus_oct_v2[!,:group] ,fillalpha=0.4,linealpha=0.1,title="Equity 0701",  xlabel="Equity CAD",
       ylabel="Client Count",nbins=-400:100:6000)


summarystats(cus_oct_v2[!,:equity_0909])
histogram(cus_oct_v2[!,:equity_0909], fillalpha=0.4,linealpha=0.1,title="Equity 0909",  xlabel="Equity CAD",
            ylabel="Client Count",nbins=-400:100:6000,legend=false)

histogram(cus_oct_v2[!,:equity_0909],group=cus_oct_v2[!,:group] ,fillalpha=0.4,linealpha=0.1,title="Equity 0909",  xlabel="Equity CAD",
        ylabel="Client Count",nbins=-400:100:6000)


summarystats(cus_oct_v2[!,:equity_max_after2016])
quantile(skipmissing(cus_oct_v2[!,:equity_max_after2016]),0.98)
nquantile(skipmissing(cus_oct_v2[!,:equity_max_after2016]),10)
histogram(cus_oct_v2[!,:equity_max_after2016], fillalpha=0.4,linealpha=0.1,title="Max equity ever since 2016",  xlabel="Equity",
             ylabel="Client Count",nbins=0:1000:60000,legend=false)

## group them into ever bigger than 5000k
cus_oct_v2[!,:equity_ever5k] = [ ismissing(x) ? missing : x<5000 ? "never greater 5k" : "greater 5k before" for x in cus_oct_v2[!,:equity_max_after2016]]
freqtable(cus_oct_v2[!,:equity_ever5k])

cus_oct_v2[!,:ever_trade] = [x<=5 ? "always passive" : "trade 5+ before" for x in cus_oct_v2[!,:trade_max_q]]
freqtable(cus_oct_v2[!,:ever_trade])
freqtable(cus_oct_v2[!,:equity_ever5k],cus_oct_v2[!,:ever_trade])
######################################################################
## distibution of customer trade info
summarystats(cus_oct_v2[!,:trade_ttl])
quantile(cus_oct_v2[!,:trade_ttl],0.98)
histogram(cus_oct_v2[!,:trade_ttl], fillalpha=0.4,linealpha=0.1,title="Total Trade",  xlabel="Trade times",
          ylabel="Client Count",nbins=0:10:500,legend=false)
histogram(cus_oct_v2[!,:trade_ttl],group= cus_oct_v2[!,:group], fillalpha=0.4,linealpha=0.1,title="Total Trade by cohort",  xlabel="Trade times",
                    ylabel="Client Count",nbins=0:10:500)



summarystats(cus_oct_v2[!,:trade_passing_yr])
quantile(cus_oct_v2[!,:trade_passing_yr],0.98)
histogram(cus_oct_v2[!,:trade_passing_yr], fillalpha=0.4,linealpha=0.1,title="Trade for passing year",  xlabel="Trade times",
          ylabel="Client Count",nbins=0:1:50,legend=false)


summarystats(cus_oct_v2[!,:trade_max_q])
quantile(cus_oct_v2[!,:trade_max_q],0.98)
histogram(cus_oct_v2[!,:trade_max_q], fillalpha=0.4,linealpha=0.1,title="Maximum trade per quarter",  xlabel="Trade times",
          ylabel="Client Count",nbins=0:1:50,legend=false)


## add in some layer of trade to analyzed trade info
cus_oct_v2[!,:trade_type_ttl] = [ x==0 ? "Never Trade" : "Trade before" for x in cus_oct_v2[!,:trade_ttl]]
freqtable(cus_oct_v2[!,:trade_type_ttl])







###################################################################
## start to look at cross attributes
plot(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],seriestype= :scatter,
        markeralpha = 0.1,markercolor = :green, title="Age vs. Tenure")


## try heatmap, and change colorgradient
##plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=20
##      ,c=ColorGradient([:green,:yellow,:blue]))
## :blues , :viridis, :magma , :ingerno, :plasma, :'color's (:reds,:greens etc)
plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=20,xlabel="Age",ylabel="Tenure"
        ,c=ColorGradient(:blues),title="Age vs.Tenure")

## get quantile to segment customers
nquantile(skipmissing(cus_oct_v2[!,:age_today]),20)
nquantile(cus_oct_v2[!,:tenure],20)


## corss look trade vs age and tenure cus_oct_v2[!,:trade_ttl]
[names(cus_oct_v2) eltypes(cus_oct_v2)]

## this one teach me what happend when two attributes are indipendent
plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:trade_max_q],nbins=20,xlabel="Age",ylabel="Trade",
        c=ColorGradient(:blues),title="Age vs. max Trade per quater")

plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:trade_passing_yr],nbins=20,xlabel="Age",ylabel="Tenure",
        ylims=(0,500),c=ColorGradient(:blues),title="Age vs.trade last 12 month")

## heatmap for age vs tenure   trade vs equity
plot1 = histogram2d(cus_oct_v2[!,:age_today],cus_oct_v2[!,:tenure],nbins=20,xlabel="Age",ylabel="Tenure"
                ,c=ColorGradient(:blues),title="Age vs.Tenure")


##look at quantile remove outlier
summarystats(cus_oct_v2[!,:trade_max_q])
summarystats(cus_oct_v2[!,:equity_max_after2016])

quantile(cus_oct_v2[!,:trade_max_q],0.98)
quantile(skipmissing(cus_oct_v2[!,:equity_max_after2016]),0.98)
a=cus_oct_v2[:,[:trade_max_q,:equity_max_after2016,:age_today,:MTD]]
a=a[completecases(a),:]
## a=a[(a.equity_max_after2016.<=10000) .& (a.trade_max_q .<=50),:]
a=a[(a.equity_max_after2016.<=10000) .& (a.equity_max_after2016.>=0),:]

plot1 = histogram2d(a[!,:trade_max_q],a[!,:equity_max_after2016],nbins=25,
                        xlabel="Trade",ylabel="Equity"
                        ,c=ColorGradient(:blues),title="max trade vs. max equity")
##

plot1 = histogram2d(a[!,:age_today],a[!,:equity_max_after2016],nbins=30,
                        xlabel="Age",ylabel="Max Equity"
                        ,c=ColorGradient(:blues),title="Age vs. max equity")

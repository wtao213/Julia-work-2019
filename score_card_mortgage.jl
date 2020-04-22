## ###########################################################
# start date: April,2nd,2020
# prepare template for scorecard
# given certain attributes, we could divide the attrrite in different groups
# then assigne score to each groups
# finally rank every customer by total score

ENV["COLUMNS"]=240
ENV["LINES"] = 50

using CSV
using FreqTables
using StatsBase
using Statistics
using Plots
using ODBC
using DataFrames
using JLD2
using Dates
using CategoricalArrays

gr()


##
# read the fulll version

# df= CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\contribution_cus_base_full_157k_v2.csv",missingstrings= ["NULL",""])
# new version updated data
df= CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_info_april_308k.csv",missingstrings= ["NULL",""])

[names(df) eltype.(eachcol(df))]

 df = df[df.age_today .!== missing,:]
 df = df[(df.age_today .>= 18) .& (df.age_today .<= 100),:]
# if only remain customer has tenure >= 3, then their are 260431 customer left
# df = df[df.MTD .>= 3,:]
# if fitler on equity !== missing then 243,634 left

# delete
# df = nothing



#######################################################
# part 1: set up template
# very initial version, only use age, account number, equity to score


################################
# step 1: distribution plot for the attritute to get an idea for how to cut


# example
histogram(collect(skipmissing(df[(df.overal_seg .== "high score"),:EquityCAD09avg])), fillalpha = 0.4, linealpha = 0.1,
    title = "Equity for High score",  xlabel = "Equity" ,ylabel = "Client Count"
   ,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:400000
   ,nbins = 0:5000:400000,legend = false)


#########################################################
# distribution of age
summarystats(df[!,:age_today])
histogram(df[!,:age_today],fillalpha = 0.4, linealpha = 0.1,legend = false,xticks = 20:5:100
      ,title = "Age distribution",  xlabel = "Age" ,ylabel = "Client Count"  )

# want to have cut point at 25,30,40,50,65

# quantile cust
# age_cuts = quantile(df.age_today,[0.25,0.5,0.75])
# age_cuts = [-Inf; unique(quantile(df.age_today, 0.25:0.25:0.75)); Inf]
# absolute cut
#
age_cuts = [25,30,40,50,65]
df.age_ca = cut(df.age_today, age_cuts, extend = true)
freqtable(df.age_ca)
levels(df.age_ca)

# assign score to different level
# df[!,:age_score] = [x=="[30, 40)" ? 10 : x in ("[25, 30)","[40, 50)") ? 8 :
#                  x in ("[14, 25)","[50, 65)") ? 5 : 0  for x in df[!,:age_ca]]

# method 2: using map function to transfer your data,easy and clean to see
mycode = Dict(
              "[18, 25)" => 5,
              "[25, 30)" => 8,
              "[30, 40)" => 10,
              "[40, 50)" => 8,
              "[50, 65)" => 5,
              "[65, 99]"=> 0,
                ##      "" => missing,
                  )
df[!,:age_score]= [get(mycode, s, missing) for s in df.age_ca]




#########################################################
# distribution of Equity
# 21745 outof 307941 customers has no equity avg
summarystats(df[!,:EquityCAD03avg])
quantile(skipmissing(df[!,:EquityCAD03avg]),0.98)
quantile(skipmissing(df[!,:EquityCAD03avg]),0:0.1:1)
histogram(collect(skipmissing(df[!,:EquityCAD03avg])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Equity distribution",  xlabel = "Age" ,ylabel = "Client Count"
      ,nbins = 0:5000:400000,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:400000)

# absolute cut
# becareful about the bounds, if no inf or -inf easy get #undef
equity_cuts = [-Inf,1000,5000,10000,50000,Inf]
df[!,:Equity_ca] = cut(df[!,:EquityCAD03avg],equity_cuts, extend = true)
levels(df[!,:Equity_ca])

mycode = nothing
mycode = Dict(
            "[-Inf, 1000.0)"      => 0,
            "[1000.0, 5000.0)"    => 4,
            "[5000.0, 10000.0)"   => 6,
            "[10000.0, 50000.0)"  => 8,
            "[50000.0, Inf]"      => 10,
             missing             => -99,
                  )
df[!,:Equity_score]= [get(mycode, s, missing) for s in df[!,:Equity_ca]]




#########################################################
# distribution of active accounts
summarystats(df[!,:act_acct])
act_cuts = unique(quantile(df[!,:act_acct], 0:0.1:1))
df[!,:act_ca] =  cut(df[!,:act_acct], act_cuts, extend = true)
freqtable(df.act_ca)

levels(df.act_ca)
levels(df[!,:act_acct])


mycode = nothing
mycode = Dict(
            "[1.0, 2.0)"   => 2,
            "[2.0, 3.0)"   => 7,
            "[3.0, 15.0]"  => 10,
                ##      "" => missing,
            )
df[!,:act_score]= [get(mycode, s, missing) for s in df[!,:act_ca]]




# sum up the total score
df[!,:ttl_score] = df[!,:age_score] + df[!,:Equity_score] + df[!,:act_score]


#
# export file
CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_244k.csv",df)










###########################################################
# part 2: want to add in attributes contain missing value
# April 6th,2020

#df= CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_308k.csv")
df = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_v3.csv")
[names(df) eltype.(eachcol(df))]




## attributes with missing, look at trade last quarter
summarystats(df[!,:trade_time_lq])
quantile(skipmissing(df[!,:trade_time_lq]),[0.97,0.98,0.99])
quantile(skipmissing(df[!,:trade_time_lq]),0:0.1:1)
# distribution of attributes
histogram(collect(skipmissing(df[!,:trade_time_lq])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Trading Times Last Quarter",  xlabel = "Trading Times" ,ylabel = "Client Count"
      ,nbins = 0:2:200,xticks = 0:20:200)



# want to cut at missing, 1,5,10,30
 trade_cuts =[1,5,10,30,maximum(skipmissing(df[!,:trade_time_lq]))]
# trade_cuts =[1,5,10,30]
# trade_cuts = unique(quantile(df[!,:act_acct], 0:0.1:1))
df[!,:trade_ca] =  cut(df[!,:trade_time_lq], trade_cuts, extend = true)

levels(df[!,:trade_ca])
typeof(df[!,:trade_ca])
# select!(df,Not(:trade_ca))

#
mycode =nothing
# mycode = Dict(
#            "[1, 5)"  => 2,
#            "[5, 10)"   => 5,
#            "[10, 30)"  => 8,
#            "[30, 20557]" => 10,
#              missing  => 0,
#            )


mycode = Dict(
                "[1, 5)"  => 1,
                "[5, 10)"   => 2,
                "[10, 30)"  => 3,
                "[30, 20557]" => 4,
                 missing  => 0,
                )
df[!,:trade_score]= [get(mycode, s, missing) for s in df[!,:trade_ca]]

freqtable(df[!,:trade_score])




## add in tuenrue info as well
summarystats(df[!,:MTD])
quantile(skipmissing(df[!,:MTD]),[0.97,0.98,0.99])
quantile(skipmissing(df[!,:MTD]),0:0.1:1)


histogram(collect(skipmissing(df[!,:MTD])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Tenure by Month",  xlabel = "Months" ,ylabel = "Client Count")
#      ,nbins = 0:2:200,xticks = 0:20:200)

#
tenure_cuts =[0,3,36,60,maximum(skipmissing(df[!,:MTD]))]
df[!,:tenure_ca] =  cut(df[!,:MTD], tenure_cuts, extend = true)
levels(df[!,:tenure_ca])
typeof(df[!,:tenure_ca])

#
mycode =nothing
mycode = Dict(
            "[0, 3)"    => - 99,
            "[3, 36)"   => 10,
            "[36, 60)"  => 7,
            "[60, 191]" => 4,
            )
df[!,:tenure_score]= [get(mycode, s, missing) for s in df[!,:tenure_ca]]








#### problem is can't using freqtable to the categorical array, but could check score after
df[!,:ttl_score_v2] = df[!,:trade_score] + df[!,:ttl_score] + df[!,:tenure_score]


## export data
CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_v3.csv",df)
CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_308k.csv",df)







#################################################################################
## part 3: rank the customers total number, and look at the  customer profile
## start date: April, 7th,2020
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

[names(df) eltype.(eachcol(df))]
##
df[!,:rank_gp] = rank(df[!,:ttl_score],10)

freqtable(df[!,:rank_gp])



#################################
# dataframe summary
df[!,:rank_gp] = rank(df[!,:ttl_score],10)
sm_df = by(df, :rank_gp,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq] =>
            x->(age_avg = mean(x.age_today) , equity_avg=mean(skipmissing(x.EquityCAD03avg)),
                act_avg = mean(x.act_acct), tenure = mean(x.MTD), N=length(x.act_acct)
               ,tading = mean(skipmissing(x.trade_time_lq))))


##  look at score two version
df[!,:rank_gp2] = rank(df[!,:ttl_score_v2],10)

sm_df = by(df, :rank_gp2,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq] =>
            x->(age_avg = mean(x.age_today) , equity_avg=mean(skipmissing(x.EquityCAD03avg)),
                act_avg = mean(x.act_acct), tenure = mean(x.MTD), N=length(x.act_acct)
               ,tading = mean(skipmissing(x.trade_time_lq))))























#########################################################################
# April,21 2020
# part 4: checking the KYC info of the customers
#




# import data:
cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_info_318k_0421.csv",missingstrings=["","NULL"])
[names(cus) eltype.(eachcol(cus))]


cus = cus[cus.age_today .!== missing,:]
cus = cus[(cus.age_today .>= 18) .& (cus.age_today .<= 100),:]
## look at attributes distribution
summarystats(cus[!,:EquityCAD03avg])
quantile(skipmissing(cus[!,:EquityCAD03avg]),0.98)
quantile(skipmissing(cus[!,:EquityCAD03avg]),0:0.1:1)
histogram(collect(skipmissing(cus[!,:EquityCAD03avg])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Equity distribution",  xlabel = "Equity in March 2020" ,ylabel = "Client Count"
      ,nbins = 0:5000:400000,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:400000)


# Income
# feels mediun-high has higher proportion
summarystats(cus[!,:Income])
quantile(skipmissing(cus[!,:Income]),0.98)
quantile(skipmissing(cus[!,:Income]),0:0.1:1)
histogram(collect(skipmissing(cus[!,:Income])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Income distribution",  xlabel = "Income" ,ylabel = "Client Count"
      ,nbins = 0:5000:400000,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:300000)



# :LiquidAsset
summarystats(cus[!,:LiquidAsset])
quantile(skipmissing(cus[!,:LiquidAsset]),0.98)
quantile(skipmissing(cus[!,:LiquidAsset]),0:0.1:1)
histogram(collect(skipmissing(cus[!,:LiquidAsset])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Liquid Asset",  xlabel = "Liquid Asset" ,ylabel = "Client Count"
      ,nbins = 0:5000:400000,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:50000:500000)



# :NetWorth
summarystats(cus[!,:NetWorth])
quantile(skipmissing(cus[!,:NetWorth]),0.98)
quantile(skipmissing(cus[!,:NetWorth]),0:0.1:1)
histogram(collect(skipmissing(cus[!,:NetWorth])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Net Worth Distribution",  xlabel = "Net Worth" ,ylabel = "Client Count"
      ,nbins = 0:10000:800000,xformatter = x->string("\$",Int(x/1e3),"K"),xticks = 0:100000:800000)




## :MaritalStatus freqcheck
a = freqtable(cus[!,:MaritalStatus])
prop(a)


## try to do the rank again on this new version data

## rank 1:
# age
age_cuts = [25,30,40,50,65]
cus.age_ca = cut(cus.age_today, age_cuts, extend = true)
freqtable(cus.age_ca)
levels(cus.age_ca)

#
mycode = Dict(
              "[18, 25)" => 5,
              "[25, 30)" => 8,
              "[30, 40)" => 10,
              "[40, 50)" => 8,
              "[50, 65)" => 5,
              "[65, 99]"=> 0,
                ##      "" => missing,
                  )
cus[!,:age_score]= [get(mycode, s, missing) for s in cus.age_ca]



## rank 2:
# equity
equity_cuts = [-Inf,1000,5000,10000,50000,Inf]
cus[!,:Equity_ca] = cut(cus[!,:EquityCAD03avg],equity_cuts, extend = true)
levels(cus[!,:Equity_ca])

mycode = nothing
mycode = Dict(
            "[-Inf, 1000.0)"      => 0,
            "[1000.0, 5000.0)"    => 4,
            "[5000.0, 10000.0)"   => 6,
            "[10000.0, 50000.0)"  => 8,
            "[50000.0, Inf]"      => 10,
             missing             => -99,
                  )
cus[!,:Equity_score]= [get(mycode, s, missing) for s in cus[!,:Equity_ca]]



## rank 3:
# active account number

summarystats(cus[!,:act_acct])
act_cuts = unique(quantile(cus[!,:act_acct], 0:0.1:1))
cus[!,:act_ca] =  cut(cus[!,:act_acct], act_cuts, extend = true)
freqtable(cus.act_ca)

levels(cus.act_ca)
levels(cus[!,:act_acct])


mycode = nothing
mycode = Dict(
            "[1.0, 2.0)"   => 2,
            "[2.0, 3.0)"   => 7,
            "[3.0, 15.0]"  => 10,
                ##      "" => missing,
            )
cus[!,:act_score]= [get(mycode, s, missing) for s in cus[!,:act_ca]]




# sum up the total score
cus[!,:ttl_score] = cus[!,:age_score] + cus[!,:Equity_score] + cus[!,:act_score]


## rank 4:

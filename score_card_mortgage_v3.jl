## ###########################################################
# start date: April,24nd,2020
# prepare template for scorecard
# given certain attributes, we could divide the attrrite in different groups
# then assigne score to each groups
# finally rank every customer by total score

ENV["COLUMNS"]=240
ENV["LINES"] = 100

using CSV
using FreqTables
using StatsBase
using Statistics
using Plots
using ODBC
using DataFrames
using StatsPlots
using Dates
using CategoricalArrays

gr()






## import data

# cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_info_318k_0421.csv",missingstrings=["","NULL"])
cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_356k_junraw.csv",missingstrings=["","NULL"])

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
# updated on April 29, add more bin on higher end
equity_cuts = [-Inf,1000,5000,10000,50000,100000,150000,200000,Inf]
cus[!,:Equity_ca] = cut(cus[!,:EquityCAD06avg],equity_cuts, extend = true)
levels(cus[!,:Equity_ca])

mycode = nothing
mycode = Dict(
            "[-Inf, 1000.0)"      => 0,
            "[1000.0, 5000.0)"    => 4,
            "[5000.0, 10000.0)"   => 6,
            "[10000.0, 50000.0)"  => 8,
            "[50000.0, 100000.0)" => 10,
            "[100000.0, 150000.0)"=> 12,
            "[150000.0, 200000.0)"=> 14,
            "[200000.0, Inf]"     => 16,
             missing              => -99,
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
# trade last quarter
summarystats(cus[!,:trade_time_lq])
trade_cuts =[1,5,10,30,maximum(skipmissing(cus[!,:trade_time_lq]))]
cus[!,:trade_ca] =  cut(cus[!,:trade_time_lq], trade_cuts, extend = true)

mycode =nothing
mycode = Dict(
                "[1, 5)"  => 1,
                "[5, 10)"   => 2,
                "[10, 30)"  => 3,
                "[30, 25812]" => 4,
                 missing  => 0,
                )
cus[!,:trade_score]= [get(mycode, s, missing) for s in cus[!,:trade_ca]]

freqtable(cus[!,:trade_score])



## rank 5: Tenure
tenure_cuts =[0,2,36,60,maximum(skipmissing(cus[!,:MTD]))]
cus[!,:tenure_ca] =  cut(cus[!,:MTD], tenure_cuts, extend = true)
levels(cus[!,:tenure_ca])
typeof(cus[!,:tenure_ca])

#
mycode =nothing
mycode = Dict(
            "[0, 2)"    => - 99,
            "[2, 36)"   => 10,
            "[36, 60)"  => 7,
            "[60, 193]" => 4,
            )
cus[!,:tenure_score]= [get(mycode, s, missing) for s in cus[!,:tenure_ca]]


## rank 6: income
summarystats(cus[!,:Income])
unique(quantile(skipmissing(cus[!,:Income]), 0:0.1:1))

income_cuts =[0,15000,45000,60000,80000,100000,150000,250000,350000,maximum(skipmissing(cus[!,:Income]))]
cus[!,:income_ca] =  cut(cus[!,:Income], income_cuts, extend = true)
levels(cus[!,:income_ca])
typeof(cus[!,:income_ca])

#
mycode =nothing
mycode = Dict(
            "[0.0, 15000.0)"         => 0,
            "[15000.0, 45000.0)"     => 1,
            "[45000.0, 60000.0)"     => 2,
            "[60000.0, 80000.0)"     => 7,
            "[80000.0, 100000.0)"    => 8,
            "[100000.0, 150000.0)"   => 9,
            "[150000.0, 250000.0)"   => 10,
            "[250000.0, 350000.0)"   => 11,
            "[350000.0, 1.2e10]"     => 0,
            missing                  => 0,
            )
cus[!,:income_score]= [get(mycode, s, missing) for s in cus[!,:income_ca]]






## get rank version 2
cus[!,:ttl_score_v2] = cus[!,:trade_score] + cus[!,:ttl_score] + cus[!,:tenure_score] + cus[!,:income_score]



##  look at score two version
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end




[names(cus) eltype.(eachcol(cus))]
cus[!,:rank_gp2] = rank(cus[!,:ttl_score_v2],10)
CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr_v2.csv",cus)

sm_df = by(cus, :rank_gp2,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq] =>
            x->(age_avg = mean(x.age_today) , equity_avg=mean(skipmissing(x.EquityCAD03avg)),
                act_avg = mean(x.act_acct), tenure = mean(x.MTD), N=length(x.act_acct)
               ,tading = mean(skipmissing(x.trade_time_lq))))

## add in self reported info to get an idea
# do some clearence to the income


sm_df = by(cus, :rank_gp2
          ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
                ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
                ,act_avg      = mean(x.act_acct)
                ,tenure       = mean(x.MTD)
                ,N            = length(x.act_acct)
                ,tading       = mean(skipmissing(x.trade_time_lq))
                ,Income_mean  = mean(skipmissing(x.Income))
                ,Income_median  = median(skipmissing(x.Income))
                ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
                ,NetWorth     = median(skipmissing(x.NetWorth))
                ))











#############################################################################
## sorce card version 3 start here:
# 1. update the income score, lower it
# 2. check whether KYC job status make sense or not, correlation test with age
# then add in KYC job and check validation against survey results


########################################################
# part 5: start date: April,23rd, 2020
# additional correlation test
#
# cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr.csv")
cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr_v3.csv")
[names(cus) eltype.(eachcol(cus))]



## new version of dataframe
## innerjoin(a, b, on = :ID => :IDNew)

## add in KYC job info, and survey result

suv = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\survey_interested.csv")
kyc = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_job_KYC.csv", missingstrings=["NULL",""])

# cus_v2 = join(cus, suv, on = :PrimaryClientID, kind = :left)
cus_v2 = leftjoin(cus, suv, on = :PrimaryClientID)


# cus_v3 = leftjoin(cus, suv, on = :ID => :IDNew)
cus_v3 = join(cus_v2, kyc, on = :PrimaryClientID => :UserID, kind = :left)

CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr_v3.csv",cus_v3)

cus_v2 = nothing
cus_v3 = nothing

########################################################
## data check

freqtable(cus[!,:survey_gp])
freqtable(cus[!,:survey_gp],cus[!,:rank_gp2])




## heatmap of income vs. equity
# heatmap of income vs. age
#
histogram2d(cus[!,:Income],cus[!,:age_today],nbins=25,xlabel="Overall Satisfy",ylabel="refer friends",
                c=cgrad(:blues),title="Overall Satisfy vs. refer friends")

# have to remove missing first
a= cus[(cus.Income .!== missing) .& (cus.Income .<= 250000) ,:]

histogram2d(a[!,:age_today],a[!,:Income]
            ,nbins=20,xlabel="Age Today",ylabel="SR Income"
            ,yformatter = x->string("\$",Int(x/1e3),"K"),yticks = 0:25000:250000
            ,xticks= 20:5:100
            ,c=ColorGradient(:blues),title="Age Today vs. SR Income")



## heatmap of income vs. equity
# if remove missing of income and equity03avg, then only 287k left
quantile(skipmissing(cus[!,:EquityCAD03avg]),0:0.1:1)
quantile(skipmissing(cus[!,:EquityCAD03avg]),0.99)
a= nothing
a= cus[(cus.Income .!== missing) .& (cus.EquityCAD03avg .!== missing) ,:]
a= a[(a.EquityCAD03avg .<= 200000) .& (a.Income .<= 250000) .& (a.EquityCAD03avg .>= 0),:]

histogram2d(a[!,:EquityCAD03avg],a[!,:Income]
            ,nbins=20,xlabel="Equity in March",ylabel="KYC Income"
            ,yformatter = x->string("\$",Int(x/1e3),"K"),yticks = 0:25000:250000
            ,xformatter = x->string("\$",Int(x/1e3),"K")
            ,c=cgrad(:blues),title="KYC Income Vs. Equity in March")



################
## line plot based on median quantile number, income vs age today
[names(cus) eltype.(eachcol(cus))]

# look at reported data at median level to avoid the impact of outliers
sm_df = by(cus, :rank_gp2
          ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(N            = length(x.act_acct)
                ,age_avg       = mean(x.age_today)
                ,tenure       = mean(x.MTD)
                ,act_avg      = mean(x.act_acct)
                ,tading       = mean(skipmissing(x.trade_time_lq))
                ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
                ,equity_mid   = median(skipmissing(x.EquityCAD03avg))
                ,NetWorth     = median(skipmissing(x.NetWorth))
                ,Income       = median(skipmissing(x.Income))
                ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
                ))

sm_df = nothing
## get dataset by age groups
ai_df = by(cus,:age_ca
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_Income       = median(skipmissing(x.Income))
            ,Income_10       = quantile(skipmissing(x.Income),0.10)
            ,Income_25       = quantile(skipmissing(x.Income),0.25)
            ,Income_75       = quantile(skipmissing(x.Income),0.75)
            ,Income_90       = quantile(skipmissing(x.Income),0.90)
            ,Income_98       = quantile(skipmissing(x.Income),0.98)
            ))



## by real age
ai_df = by(cus,:age_today
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_Income       = median(skipmissing(x.Income))
            ,Income_10       = quantile(skipmissing(x.Income),0.10)
            ,Income_25       = quantile(skipmissing(x.Income),0.25)
            ,Income_75       = quantile(skipmissing(x.Income),0.75)
            ,Income_90       = quantile(skipmissing(x.Income),0.90)
            ,Income_98       = quantile(skipmissing(x.Income),0.98)
            ))



## version 1: age bin group
## plot on ai_df, and we need sort first
# this is one column: sort!(ai_df[!,:age_ca])
ai_df = sort!(ai_df,(:age_ca))

plot(ai_df[!,:age_ca], ai_df[!,:Income_90],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
      ,title="Age vs. KYC Self reported Income"
      ,legend=:topleft)
plot!(ai_df[!,:age_ca], ai_df[!,:median_Income],  label = ("Median Income"))
plot!(ai_df[!,:age_ca], ai_df[!,:Income_10],  label = ("Income 10%"))
plot!(ai_df[!,:age_ca], ai_df[!,:Income_25],  label = ("Income 25%"))
plot!(ai_df[!,:age_ca], ai_df[!,:Income_75],  label = ("Income 75%"))




## version 2: real age
# only look at customers' age less than 90
ai_df = sort!(ai_df,(:age_today))
b = ai_df[ai_df.age_today .<= 80 ,:]

plot(b[!,:age_today], b[!,:Income_90],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
      ,xticks = 20:5:80
      ,title=" KYC Self reported Income Trajectory"
      ,xlabel="Age",ylabel="KYC Income"
      ,legend=:best
      )
plot!(b[!,:age_today], b[!,:median_Income],  label = ("Median Income"))
plot!(b[!,:age_today], b[!,:Income_10],      label = ("Income 10%"))
plot!(b[!,:age_today], b[!,:Income_25],      label = ("Income 25%"))
plot!(b[!,:age_today], b[!,:Income_75],      label = ("Income 75%"))

b = nothing





#
## Age vs. average equity in march
#
summarystats(cus[!,:age_today])
summarystats(cus[!,:EquityCAD03avg])
quantile(skipmissing(cus[!,:EquityCAD03avg]),[0.01,0.99])
a = nothing
a = cus[(cus.EquityCAD03avg .!== missing) .& (cus.EquityCAD03avg .>= 0) .& (cus.EquityCAD03avg .<= 450000),:]


et_df   = by(a,:age_today
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_Equity   = median(skipmissing(x.EquityCAD03avg))
            ,Equity_10       = quantile(skipmissing(x.EquityCAD03avg),0.10)
            ,Equity_25       = quantile(skipmissing(x.EquityCAD03avg),0.25)
            ,Equity_75       = quantile(skipmissing(x.EquityCAD03avg),0.75)
            ,Equity_90       = quantile(skipmissing(x.EquityCAD03avg),0.90)
            ,Equity_98       = quantile(skipmissing(x.EquityCAD03avg),0.98)
            ))


# equity vs. age
et_df = sort!(et_df,(:age_today))
b = nothing
b = et_df[et_df.age_today .<= 80 ,:]

plot(b[!,:age_today], b[!,:Equity_90],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
      ,xticks = 20:5:80
      ,title =" Equity Trajectory"
      ,xlabel="Age",ylabel="March Equity"
      ,legend=:topleft
      )
plot!(b[!,:age_today], b[!,:median_Equity],  label = ("Median Equity"))
plot!(b[!,:age_today], b[!,:Equity_10],      label = ("Equity 10%"))
plot!(b[!,:age_today], b[!,:Equity_25],      label = ("Equity 25%"))
plot!(b[!,:age_today], b[!,:Equity_75],      label = ("Equity 75%"))







######################################################
# part 6: validate the survey customer
# :survey_group, total 451 survey, and now for still active there are 425

freqtable(cus[!,:survey_group])
t = freqtable(cus[!,:survey_group],cus[!,:rank_gp2])


suvey_df = by(cus,:survey_group
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth,:rank_gp2] =>
            x->(
             age_avg      = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_Income     = median(skipmissing(x.Income))
            ,rank_gp2_10       = quantile(skipmissing(x.rank_gp2),0.10)
            ,rank_gp2_25       = quantile(skipmissing(x.rank_gp2),0.25)
            ,rank_gp2_50       = quantile(skipmissing(x.rank_gp2),0.50)
            ,rank_gp2_75       = quantile(skipmissing(x.rank_gp2),0.75)
            ,rank_gp2_90       = quantile(skipmissing(x.rank_gp2),0.90)
            ,rank_gp2_98       = quantile(skipmissing(x.rank_gp2),0.98)
            ))



## distrivution plot
#
histogram(cus[cus.survey_gp .== "interested",:rank_gp2],fillalpha = 0.4, linealpha = 0.1
      ,nbins = 1:10,xlabel ="Rank Score", ylabel = "customer count"
      ,title = "Rank score of survey customers",legend = false)







## correlation test for age vs. working status

[names(cus) eltype.(eachcol(cus))]

freqtable(cus[!,:ET_EmploymentType])

employe_df = by(cus,:ET_EmploymentType,[:age_today] =>
             x->(
             N            = length(x.age_today)
             ,age_avg     = mean(x.age_today)
             ,age_median  = median(x.age_today)
             ,age_max     = maximum(x.age_today)
             ,age_min     = minimum(x.age_today)
             ))



# change
job = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_340k_jun_job.csv",missingstrings=["","NULL"])
[names(job ) eltype.(eachcol(job ))]

# left join
cus = leftjoin(cus,job, on =:PrimaryClientID)

cus2 = nothing
job = nothing

cus[!,:ET_EmploymentType] = [ismissing(x) ? "Unknown2" : x for x in cus[!,:ET_EmploymentType]]
# generate a revise version
cus[!,:Emplyment_refine] = [x=="Unknown"   ? "Unreliable" : x=="Unknown2"   ?
            "Unreliable" : (x=="Student" && y>=45)  ? "Unreliable" : (x=="Retired" && y<=45) ? "Unreliable" : x
            for (x,y) in zip(cus[!,:ET_EmploymentType],cus[!,:age_today])]

freqtable(cus[!,:Emplyment_refine])

## historgram
#  !!!  edit bin
histogram(cus[!,:age_today],group = cus[!,:Emplyment_refine]
      ,fillalpha = 0.4, linealpha = 0.1
      ,nbins = 18:1:80,xlabel ="Age", ylabel = "customer count"
      ,title = "Employment Status Age Distribution")

##
histogram(cus[cus.ET_EmploymentType .== "Student",:age_today]
      ,fillalpha = 0.4, linealpha = 0.1
      ,nbins = 18:1:80,xlabel ="Age", ylabel = "customer count"
      ,title = "Employment Status Age Distribution")




###########################################################
## score version3:

income_cuts =[0,15000,45000,60000,80000,100000,150000,250000,350000,maximum(skipmissing(cus[!,:Income]))]
cus[!,:income_ca] =  cut(cus[!,:Income], income_cuts, extend = true)
levels(cus[!,:income_ca])
typeof(cus[!,:income_ca])

#
mycode =nothing
mycode = Dict(
            "[0.0, 15000.0)"         => 0,
            "[15000.0, 45000.0)"     => 1,
            "[45000.0, 60000.0)"     => 2,
            "[60000.0, 80000.0)"     => 7,
            "[80000.0, 100000.0)"    => 8,
            "[100000.0, 150000.0)"   => 9,
            "[150000.0, 250000.0)"   => 10,
            "[250000.0, 350000.0)"   => 11,
            "[350000.0, 1.2e10]"  => 0,
            missing                  => 0,
            )
cus[!,:income_score_v2]= [get(mycode, s, missing) for s in cus[!,:income_ca]]


## add store for cus[!,:Emplyment_refine]
freqtable(cus[!,:Emplyment_refine])
levels(cus[!,:Emplyment_refine])

mycode =nothing
mycode = Dict(
            "Employed"         => 4,
            "Homemaker"        => 1,
            "Retired"          => 0,
            "Self-Employed"    => 1,
            "Student"          => 2,
            "Unemployed"       => 0,
            "Unreliable"       => 0,
            )
cus[!,:employ_score]= [get(mycode, s, missing) for s in cus[!,:Emplyment_refine]]


## get score for version 3 first round
cus[!,:score_version_3_0] = cus[!,:trade_score] + cus[!,:ttl_score] + cus[!,:tenure_score]
                              + cus[!,:income_score_v2] + cus[!,:employ_score]

summarystats(cus[!,:score_version_3_0])
##  look at score two version
function rank(x::AbstractVector,k::Integer)
      ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

cus[!,:rank_ver_3_0] = rank(cus[!,:score_version_3_0],10)

freqtable(cus[!,:rank_ver_3_0])

freqtable(cus_v2[!,:survey_gp],cus_v2[!,:rank_ver_3_0])
## export your data
#CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr_ver_3_0.csv",cus)

CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_score_full_355k_sr_ver_3_0.csv",cus_v2)
## version 3_0 customer profile
[names(cus) eltype.(eachcol(cus))]

sm_df = by(cus, :rank_ver_3_0
          ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->( N            = length(x.act_acct)
                ,age_avg      = mean(x.age_today)
                ,tenure       = mean(x.MTD)
                ,act_avg      = mean(x.act_acct)
                ,tading       = mean(skipmissing(x.trade_time_lq))
                ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
                ,equity_mid   = median(skipmissing(x.EquityCAD03avg))
                ,NetWorth     = median(skipmissing(x.NetWorth))
                ,Income       = median(skipmissing(x.Income))
                ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
                ))

## check agianst survey_gp
freqtable(cus[!,:rank_ver_3_0],cus[!,:survey_gp])


##
sm_df = by(cus, :rank_gp2
          ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->( N            = length(x.act_acct)
                ,age_avg      = mean(x.age_today)
                ,tenure       = mean(x.MTD)
                ,act_avg      = mean(x.act_acct)
                ,tading       = mean(skipmissing(x.trade_time_lq))
                ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
                ,equity_mid   = median(skipmissing(x.EquityCAD03avg))
                ,NetWorth     = median(skipmissing(x.NetWorth))
                ,Income       = median(skipmissing(x.Income))
                ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
                ))

freqtable(cus[!,:rank_gp2],cus[!,:survey_gp])

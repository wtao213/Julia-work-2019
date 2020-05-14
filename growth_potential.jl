############################################################
#   Customer potential
#   1) income vs age
#   2) asset vs age
#   3) KYC asset vs. QFG equity



ENV["COLUMNS"]=240
ENV["LINES"] = 50

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


## import data, using the score_cards full version info
cus = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage\\cus_score_full_318k_sr_v3.csv")
[names(cus) eltype.(eachcol(cus))]


########################################################
# 1) income vs age
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


#
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



## version 2: smaller age bin
maximum(cus[!,:age_today])
minimum(cus[!,:age_today])

age_cuts = collect(20:5:90)
age_cuts = append!([18],age_cuts)

cus[!,:age_bin] =  cut(cus[!,:age_today], age_cuts, extend = true)
levels(cus[!,:age_bin])
typeof(cus[!,:age_bin])



ai_df = by(cus,:age_bin
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

# sort and plot
ai_df = sort!(ai_df,(:age_bin))
b = ai_df[ai_df.age_avg .<= 80 ,:]



plot(b[!,:age_bin], b[!,:Income_90],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
#      ,xticks = 20:5:80
      ,xrotation = 45
      ,title =" KYC Self reported Income Trajectory"
      ,xlabel="Age",ylabel="KYC Income"
      ,legend= false
      )
plot!(b[!,:age_bin], b[!,:median_Income],  label = ("Median Income"))
plot!(b[!,:age_bin], b[!,:Income_10],      label = ("Income 10%"))
plot!(b[!,:age_bin], b[!,:Income_25],      label = ("Income 25%"))
plot!(b[!,:age_bin], b[!,:Income_75],      label = ("Income 75%"))



## add in statcan data
stascan = DataFrame(Age = ["16-24","25-34","35-44","45-54","55-64","65+"],
                    median_income = [ 12700,39500,50300,50300,42200,29700],
                    Avg_income = [16600,45700,60100,64500,54600,41400]
)


plot(stascan[!,:Age], stascan[!,:median_income],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
      ,xticks = 20:5:80
      ,xrotation = 45
      ,title =" KYC Self reported Income Trajectory"
      ,xlabel="Age",ylabel="KYC Income"
      ,legend= false
      )


## using dictionay to map two age bin
levels(stascan[!,:Age])
levels(b[!,:age_bin])

mycode = Dict(
                "[1, 5)"  => 1,
                "[18, 20)" => "16-24",
                "[20, 25)" => "16-24",
                "[25, 30)" => "25-34",
                "[30, 35)" => "25-34",
                "[35, 40)" => "35-44",
      "[40, 45)"=> "35-44",
      "[45, 50)"=> "45-54",
      "[50, 55)"=> "45-54",
      "[55, 60)"=> "55-64",
      "[60, 65)"=> "55-64",
      "[65, 70)"=> "65+",
      "[70, 75)"=> "65+",
      "[75, 80)"=> "65+",
      "[80, 85)"=> "65+",
      "[85, 90)"=> "65+",
      "[90, 99]"=> "65+",
                )
b[!,:stat_match]= [get(mycode, s, missing) for s in b[!,:age_bin]]

# new dataframe b
b2= join(b,stascan, on= :stat_match =>:Age, kind= :left )

## new plotting
b2 = sort!(b2,(:age_bin))
plot(b2[!,:age_bin], b2[!,:Income_90],  label = ("Income 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:20000:200000
#      ,xticks = 20:5:80
      ,xrotation = 45
      ,title =" KYC Self reported Income Trajectory"
      ,xlabel="Age",ylabel="KYC Income"
      ,legend= false
      )
plot!(b2[!,:age_bin], b2[!,:median_Income],  label = ("Median Income"))
plot!(b2[!,:age_bin], b2[!,:Income_10],      label = ("Income 10%"))
plot!(b2[!,:age_bin], b2[!,:Income_25],      label = ("Income 25%"))
plot!(b2[!,:age_bin], b2[!,:Income_75],      label = ("Income 75%"))

plot!(b2[!,:age_bin], b2[!,:median_income],linestyle = :dot, linewidth = 4,label = ("Stats Canada"))



##  2) asset vs age

equity_df = by(cus,:age_bin
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_Equity       = median(skipmissing(x.EquityCAD03avg))
            ,Equity_10       = quantile(skipmissing(x.EquityCAD03avg),0.10)
            ,Equity_25       = quantile(skipmissing(x.EquityCAD03avg),0.25)
            ,Equity_75       = quantile(skipmissing(x.EquityCAD03avg),0.75)
            ,Equity_90       = quantile(skipmissing(x.EquityCAD03avg),0.90)
            ,Equity_98       = quantile(skipmissing(x.EquityCAD03avg),0.98)
            ))


# distribution plot
# sort and plot
equity_df = sort!(equity_df,(:age_bin))
b = equity_df[equity_df.age_avg .<= 80 ,:]



plot(b[!,:age_bin], b[!,:Equity_90],  label = ("Equity90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
      ,yticks = 0:50000:800000
      ,ylims = 0:50000:800000
#      ,xticks = 20:5:80
      ,xrotation = 45
      ,title ="Questrade Equity"
      ,xlabel="Age",ylabel="Questrade Equity"
      ,legend= false
      )
plot!(b[!,:age_bin], b[!,:Equity_75],      label = ("Equity 75%"))
plot!(b[!,:age_bin], b[!,:median_Equity],  label = ("Median Equity"))
plot!(b[!,:age_bin], b[!,:Equity_25],      label = ("Equity 25%"))
plot!(b[!,:age_bin], b[!,:Equity_10],      label = ("Equity 10%"))










## :LiquidAsset
#

LiquidAsset_df = by(cus,:age_bin
            ,[:age_today,:EquityCAD03avg ,:act_acct ,:MTD, :trade_time_lq,:Income,:LiquidAsset,:NetWorth] =>
            x->(age_avg       = mean(x.age_today)
            ,equity_avg   = mean(skipmissing(x.EquityCAD03avg))
            ,act_avg      = mean(x.act_acct)
            ,tenure       = mean(x.MTD)
            ,N            = length(x.act_acct)
            ,tading       = mean(skipmissing(x.trade_time_lq))
            ,LiquidAsset  = median(skipmissing(x.LiquidAsset))
            ,NetWorth     = median(skipmissing(x.NetWorth))
            ,median_LiquidAsset       = median(skipmissing(x.LiquidAsset))
            ,LiquidAsset_10       = quantile(skipmissing(x.LiquidAsset),0.10)
            ,LiquidAsset_25       = quantile(skipmissing(x.LiquidAsset),0.25)
            ,LiquidAsset_75       = quantile(skipmissing(x.LiquidAsset),0.75)
            ,LiquidAsset_90       = quantile(skipmissing(x.LiquidAsset),0.90)
            ,LiquidAsset_98       = quantile(skipmissing(x.LiquidAsset),0.98)
            ))


#
# sort and plot
LiquidAsset_df = sort!(LiquidAsset_df,(:age_bin))
b = LiquidAsset_df[LiquidAsset_df.age_avg .<= 80 ,:]

plot(b[!,:age_bin], b[!,:LiquidAsset_90],  label = ("LiquidAsset 90%")
      ,yformatter = x->string("\$",Int(x/1e3),"K")
#      ,yticks = 0:20000:200000
,yticks = 0:100000:800000
#      ,xticks = 20:5:80
      ,xrotation = 45
      ,title ="KYC Liquid Asset"
      ,xlabel="Age",ylabel="KYC Liquid Asset"
      ,legend= false
      )
plot!(b[!,:age_bin], b[!,:LiquidAsset_75],      label = ("LiquidAsset 75%"))
plot!(b[!,:age_bin], b[!,:median_LiquidAsset],  label = ("Median LiquidAsset"))
plot!(b[!,:age_bin], b[!,:LiquidAsset_25],      label = ("LiquidAsset 25%"))
plot!(b[!,:age_bin], b[!,:LiquidAsset_10],      label = ("LiquidAsset 10%"))






###
# 3) KYC asset vs. QFG equity
[names(cus) eltype.(eachcol(cus))]

summarystats(cus[!,:LiquidAsset])
quantile(skipmissing(cus[!,:LiquidAsset]),0.99)
summarystats(cus[!,:EquityCAD03avg])
b=nothing


## clim limit color
b= cus[(cus.LiquidAsset .!== missing) .& (cus.EquityCAD03avg  .!== missing) .& (cus.EquityCAD03avg .>= 0) .& (cus.LiquidAsset .>= 0) .& (cus.EquityCAD03avg .<= 200000) .& (cus.LiquidAsset .<= 800000) ,:]

histogram2d(b[!,:EquityCAD03avg],b[!,:LiquidAsset]
            ,nbins=20,xlabel="Questrade Equity",ylabel="KYC Liquid Asset"
            ,yformatter = x->string("\$",Int(x/1e3),"K")
            ,xformatter = x->string("\$",Int(x/1e3),"K")
#            ,yticks = 0:25000:250000
#            ,xticks= 20:5:100
            ,clim =(0,2500)
            ,c=cgrad(:blues),title="Large Consolidation Opportunity")


b= cus[(cus.LiquidAsset .!== missing) .& (cus.EquityCAD03avg  .!== missing), :]
# version 2 is not remove, is replace
b[!,:equity_outside] = b[!,:LiquidAsset] - b[!,:EquityCAD03avg]
b[!,:equity_outside] = .minimum(10000000,b[!,:LiquidAsset]) .- b[!,:EquityCAD03avg]



quantile(b[!,:equity_outside],0.998)
a= b[b.equity_outside .> 0,:]
a= b[(b.equity_outside .> 0) .& (b.LiquidAsset .<= 10000000),:]

sum(a[!,:equity_outside])

CSV.write("C:\\Users\\012790\\Desktop\\cus_equity.csv",a)

histogram(b[!,:equity_outside])

















##########################################################################
# part 2: I want to draw customer age distribution plot vs canada info
#
#


#########
[names(cus) eltype.(eachcol(cus))]

canada = CSV.read("C:\\Users\\012790\\Desktop\\customer_growth_potential\\stat_canada_age.csv")


[names(canada) eltype.(eachcol(canada))]
[names(ai_df) eltype.(eachcol(ai_df))]

ai_df[!,:age_pct] = ai_df[!,:N]/sum(ai_df[!,:N])




a= sort!(ai_df,:age_today)
a= a[a.age_today .<= 80 ,:]

#maybe for canada, I only want pct on age 18-100, same as QT
b=canada[(canada.Age .<= 80).&(canada.Age .>= 18),:]
b[!,:age_pct] = b[!,:Total]/sum(b[!,:Total])

bar(a[!,:age_today], a[!,:age_pct],  label = ("Qeustrade Customer Base")
      ,yformatter = x->string(x*100,"%")
      ,fillalpha = 0.4, linealpha = 0.1
#      ,yticks = 0:20000:200000
#,yticks = 0:100000:800000
      ,xticks = 20:5:80
      ,title ="Age Distribution"
      ,xlabel="Age",ylabel="Percent"
#      ,legend= false
      )

# plot of canadian
bar!(b[!,:Age], b[!,:age_pct], label = ("Canadian Population")
      ,yformatter = x->string(x*100,"%")
      ,fillalpha = 0.4, linealpha = 0.1
      ,xticks = 20:5:80
      ,title ="Age  Distribution"
      ,xlabel="Age",ylabel="Percent"
#      ,legend= false
      )

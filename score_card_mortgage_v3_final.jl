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
cus = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_356k_junraw.csv",missingstrings=["","NULL"]))

[names(cus) eltype.(eachcol(cus))]

## only kept customer within age range 18 to 100
cus = cus[cus.age_today .!== missing,:]
cus = cus[(cus.age_today .>= 18) .& (cus.age_today .<= 100),:]

## manipulate Employment Type
cus[!,:ET_EmploymentType] = [ismissing(x) ? "Unknown2" : x for x in cus[!,:ET_EmploymentType]]
# generate a revise version
cus[!,:Emplyment_refine] = [x=="Unknown"   ? "Unreliable" : x=="Unknown2"   ?
            "Unreliable" : (x=="Student" && y>=45)  ? "Unreliable" : (x=="Retired" && y<=45) ? "Unreliable" : x
            for (x,y) in zip(cus[!,:ET_EmploymentType],cus[!,:age_today])]

freqtable(cus[!,:Emplyment_refine])

## look at attributes distribution
# this only run when need to define bin size, could change to any attributes
summarystats(cus[!,:EquityCAD03avg])
quantile(skipmissing(cus[!,:EquityCAD03avg]),0.98)
quantile(skipmissing(cus[!,:EquityCAD03avg]),0:0.1:1)
histogram(
      collect(skipmissing(cus[!, :EquityCAD03avg])),
      fillalpha = 0.4,
      linealpha = 0.1,
      legend = false,
      title = "Equity distribution",
      xlabel = "Equity in March 2020",
      ylabel = "Client Count",
      nbins = 0:5000:400000,
      xformatter = x -> string("\$", Int(x / 1e3), "K"),
      xticks = 0:50000:400000,
)



## try to do the rank again on this new version data
# binning all metircs
# rank 1:
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
levels(cus[!,:income_ca]) ## maximum bin might change become of the max income will change over time
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
            "[350000.0, 1.2e10]"  => 0, # double check this bin when rerun
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




######################################################################
## ranking our cusomter base

function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

## get score for version 3 first round
cus[!,:score_version_3_0] = cus[!,:trade_score] + cus[!,:age_score] + cus[!,:Equity_score] + cus[!,:act_score] + cus[!,:tenure_score] + cus[!,:income_score_v2] + cus[!,:employ_score]


## get scorecard rank 3_0
cus[!,:rank_ver_3_0] = rank(cus[!,:score_version_3_0],10)

freqtable(cus[!,:rank_ver_3_0])

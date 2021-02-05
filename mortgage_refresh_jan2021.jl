#########################################
##
ENV["COLUMNS"]=240
ENV["LINES"] = 100

using CSV
using FreqTables
using StatsBase
using Statistics
using Plots
using DataFrames
using StatsPlots
using Dates

using CategoricalArrays
using Missings
using DelimitedFiles

gr()


############################################
# import customer data
df = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\mortgage_scorecard_refresh2021jan\\cus_df_447k_20210113.csv",missingstrings=["","NULL"]))

[names(df) eltype.(eachcol(df))]

## only kept customer within age range 18 to 100
df = df[df.age_today .!== missing,:]
df = df[(df.age_today .>= 18) .& (df.age_today .<= 100),:]
df = df[df.act_acct .> 0,:]

## manipulate Employment Type
df[!,:ET_EmploymentType] = [ismissing(x) ? "Unknown2" : x for x in df[!,:ET_EmploymentType]]
# generate a revise version
df[!,:Emplyment_refine] = [x=="Unknown"   ? "Unreliable" : x=="Unknown2"   ?
            "Unreliable" : (x=="Student" && y>=45)  ? "Unreliable" : (x=="Retired" && y<=45) ? "Unreliable" : x
            for (x,y) in zip(df[!,:ET_EmploymentType],df[!,:age_today])]

df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\d\D\d$",x) ? uppercase(x) : missing
                 for x in df[!,:PostalCode]]





#################################################################
# part 1: get scorecard ranking

## rank 1:
# age
age_cuts = [25,30,40,50,65]
df.age_ca = cut(df.age_today, age_cuts, extend = true)
freqtable(df.age_ca)
levels(df.age_ca)

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
df[!,:age_score]= [get(mycode, s, missing) for s in df.age_ca]

## rank 2:
# equity
# updated on April 29, add more bin on higher end
equity_cuts = [-Inf,1000,5000,10000,50000,100000,150000,200000,Inf]
df[!,:Equity_ca] = cut(df[!,:EquityCADlmavg],equity_cuts, extend = true)
levels(df[!,:Equity_ca])

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
df[!,:Equity_score]= [get(mycode, s, missing) for s in df[!,:Equity_ca]]


## rank 3:
# active account number
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



## rank 4:
# trade last quarter
summarystats(df[!,:trade_time_lq])
trade_cuts =[1,5,10,30,maximum(skipmissing(df[!,:trade_time_lq]))]
df[!,:trade_ca] =  cut(df[!,:trade_time_lq], trade_cuts, extend = true)
freqtable(df[!,:trade_ca])

mycode =nothing
mycode = Dict(
                "[1, 5)"  => 1,
                "[5, 10)"   => 2,
                "[10, 30)"  => 3,
                "[30, $(maximum(skipmissing(df[!,:trade_time_lq])))]" => 4,
                 missing  => 0,
                )
df[!,:trade_score]= [get(mycode, s, missing) for s in df[!,:trade_ca]]


## rank 5: Tenure
tenure_cuts =[0,2,36,60,maximum(skipmissing(df[!,:MTD]))]
df[!,:tenure_ca] =  cut(df[!,:MTD], tenure_cuts, extend = true)
levels(df[!,:tenure_ca])
typeof(df[!,:tenure_ca])

#
mycode =nothing
mycode = Dict(
            "[0, 2)"    => - 99,
            "[2, 36)"   => 10,
            "[36, 60)"  => 7,
            "[60, $(maximum(skipmissing(df[!,:MTD])))]" => 4,
            )
df[!,:tenure_score]= [get(mycode, s, missing) for s in df[!,:tenure_ca]]



## rank 6: income
income_cuts =[0,15000,45000,60000,80000,100000,150000,250000,350000,maximum(skipmissing(df[!,:Income]))]
df[!,:income_ca] =  cut(df[!,:Income], income_cuts, extend = true)
levels(df[!,:income_ca]) ## maximum bin might change become of the max income will change over time
typeof(df[!,:income_ca])

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
            "[350000.0, $(maximum(skipmissing(df[!,:Income])))]"  => 0, # double check this bin when rerun
            missing                  => 0,
            )
df[!,:income_score_v2]= [get(mycode, s, missing) for s in df[!,:income_ca]]




## rank 7:
# add in employment info
freqtable(df[!,:Emplyment_refine])
levels(df[!,:Emplyment_refine])

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
df[!,:employ_score]= [get(mycode, s, missing) for s in df[!,:Emplyment_refine]]


######################################################################
# ranking our cusomter base

function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

## get score for version 3 first round
df[!,:score_version_3_0] = df[!,:trade_score] + df[!,:age_score] + df[!,:Equity_score] + df[!,:act_score] + df[!,:tenure_score] + df[!,:income_score_v2] + df[!,:employ_score]

# get scorecard rank 3_0
df[!,:rank_ver_3_0] = rank(df[!,:score_version_3_0],10)

freqtable(df[!,:rank_ver_3_0])

# transfer to deciles
mycode = nothing
mycode = Dict(
            1    => "Decile 10",
            2    => "Decile 9",
            3    => "Decile 8",
            4    => "Decile 7",
            5    => "Decile 6",
            6    => "Decile 5",
            7    => "Decile 4",
            8    => "Decile 3",
            9    => "Decile 2",
            10   => "Decile 1"
            )
df[!,:rank_group]= [get(mycode, s, missing) for s in df[!,:rank_ver_3_0]]


# get output
CSV.write("C:\\Users\\012790\\Desktop\\mortgage_scorecard_refresh2021jan\\cus_score_447k_20210113.csv",df)














#########################################################################################
# refresh part 2: insurability
#######################################
# import data from EA
ws  = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\WealthScapes2019_CY_GEO_v2.csv",missingstrings=["","NULL"]))
mat = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\ePCCF_match.csv",missingstrings=["","NULL"]))

[names(ws) eltype.(eachcol(ws))]
ws[!,:WSPRIMREAVG] = ws[!,:WSPRIMREV] ./ ws[!,:WSPRIMREI]
ws[!,:WSLIQORAVG]  = ws[!,:WSLIQORB] ./ ws[!,:WSLIQORI]


a = ws[ws.GEO .== "PRCDDA",:]
a[!,:CODE] = [parse(Int64,x) for x in a[!,:CODE]]

prcdda = innerjoin(a,mat,on= :CODE=>:PRCDDA)
ws=nothing
mat = nothing

# add in postal to customer base
dff = df[df.PC_ref .!== missing,:]
# df[(ismissing.(df.PC_ref)).& (df.target_rsp .==1),:]

df_full = leftjoin(dff,prcdda,on= :PC_ref=>:FSALDU)
prcdda =nothing
dff    =nothing
a      =nothing



## calculate customerbase
df_full[!,:downpay_20] = [ismissing(x) ? missing : x*0.1999 for x in df_full[!,:WSPRIMREAVG]]

df_full[!,:insurability] = [ismissing(x) ? "None" : x>= 1000000 ? "Not Insurable" : z<y ? "Insured" : "Insurable"
                        for (x,y,z) in zip(df_full[!,:WSPRIMREAVG],df_full[!,:downpay_20],df_full[!,:WSLIQORAVG])]

freqtable(df_full[!,:insurability])

# get only column needed
dff= df_full[!,[:PrimaryClientID,:WSPRIMREAVG,:WSLIQORAVG,:downpay_20,:insurability]]
df_full = leftjoin(df,dff,on= :PrimaryClientID)
dff = nothing


## ouput data 2 with insurability
CSV.write("C:\\Users\\012790\\Desktop\\mortgage_scorecard_refresh2021jan\\cus_score_insurability_447k_20210113.csv",df_full)






#################################################################################
# part 3: add in equifax info
df = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\mortgage_scorecard_refresh2021jan\\cus_score_insurability_447k_20210113.csv",missingstrings=["","NULL"]))

eq = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\equifax_file3_mortgage_s2.csv",missingstrings=["","NULL"]))

[names(df) eltype.(eachcol(df))]
[names(eq) eltype.(eachcol(eq))]

eq[!,:ACCOUNT_NUMBER]
freqtable(eq[!,:ACCOUNT_NUMBER])
# since very little people don't have sin, and if they don't have sin, not likely to sale mortgage
# remove missing rows
length(df[ismissing.(df.SIN),:SIN])
length(eq[ismissing.(eq.SOCIAL_INSURANCE_NUMBER),:SOCIAL_INSURANCE_NUMBER])

df = df[df.SIN .!== missing,:]
eq = eq[eq.SOCIAL_INSURANCE_NUMBER .!== missing,:]

#left join eq to df
df_full = leftjoin(df,eq, on = :SIN =>:SOCIAL_INSURANCE_NUMBER,makeunique = true )


CSV.write("C:\\Users\\012790\\Desktop\\mortgage_scorecard_refresh2021jan\\cus_score_insurability_eqfax_447k_2021011.csv",df_full)

##################################
# look at the duplicate issue
sort(freqtable(df[!,:SIN]),rev = true)
sort(freqtable(eq[!,:SOCIAL_INSURANCE_NUMBER]),rev = true)
sort(freqtable(df_full[!,:SIN]),rev = true)

freqtable(df[!,:rank_group])
freqtable(df_full[!,:rank_group])


# duplicate list
l = unique(df[nonunique(df,:SIN),:SIN])

df[(x in l for x in df[!,:SIN]),:SIN]
temp_df[in.(temp_df[:IndexVal], ([1,3,5],)), :]

df[in.(df[!,:SIN], (l,)), :]

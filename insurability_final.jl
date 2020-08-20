## ###########################################################
# start date: June,15nd,2020
# given certain attributes, we could divide the attrrite in different groups
# then assigne score to each groups
# finally rank every customer by total score
# based on v3, we want to add in layer of EA data

ENV["COLUMNS"]=240
ENV["LINES"] = 200

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


#######################################
# import data from EA
ws  = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\WealthScapes2019_CY_GEO_v2.csv",missingstrings=["","NULL"]))
mat = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\ePCCF_match.csv",missingstrings=["","NULL"]))


#################################
# check freq level
# minimal level is PRCDDA
sort(freqtable(ws[!,:GEO]),rev=true)

# get average and pen for ws
# WSLIQASTI	WealthScapes Liquid Assets - Incidence
# WSLIQASTB	WealthScapes Liquid Assets - Balance
# WSCHQSAVB	Chequing & Savings Accounts - Balance
# WSSAVNGB	Total Savings - Balance
# WSSAVNGI	Total Savings - Incidence
# WSCHQSAVI	Chequing & Savings Accounts - Incidence
# WSPRIMREI	Primary Real Estate - Incidence
# WSPRIMREV	Primary Real Estate - Value
# WSMORTI	Mortgage - Incidence
# WSMORTB	Mortgage - Balance
# WSLIQORI	WealthScapes Liquid Assets - Non-RSP - Incidence
# WSLIQORB	WealthScapes Liquid Assets - Non-RSP - Balance


# liquid asset
ws[!,:LIQASTAVG] = ws[!,:WSLIQASTB] ./ ws[!,:WSLIQASTI]
ws[!,:LIQASTPEN] = ws[!,:WSLIQASTI] ./ ws[!,:WSHHDTOT]

# Mortgatge
ws[!,:WSMORTAVG] = ws[!,:WSMORTB] ./ ws[!,:WSMORTI]
ws[!,:WSMORTPEN] = ws[!,:WSMORTI] ./ ws[!,:WSHHDTOT]

# Primary Real Estate
ws[!,:WSPRIMREAVG] = ws[!,:WSPRIMREV] ./ ws[!,:WSPRIMREI]
ws[!,:WSPRIMREPEN] = ws[!,:WSPRIMREI] ./ ws[!,:WSHHDTOT]

# total Savings
ws[!,:WSSAVNGAVG] = ws[!,:WSSAVNGB] ./ ws[!,:WSSAVNGI]
ws[!,:WSSAVNGPEN] = ws[!,:WSSAVNGI] ./ ws[!,:WSHHDTOT]

# Liquid asset - non-RSP
ws[!,:WSLIQORAVG] = ws[!,:WSLIQORB] ./ ws[!,:WSLIQORI]
ws[!,:WSLIQORPEN] = ws[!,:WSLIQORI] ./ ws[!,:WSHHDTOT]


##inner join to get PRCDDA level info
# PRCDDA with 56590 records
a = ws[ws.GEO .== "PRCDDA",:]
a[!,:CODE] = [parse(Int64,x) for x in a[!,:CODE]]
prcdda = innerjoin(a,mat,on= :CODE=>:PRCDDA)


# CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\prcdda.csv",prcdda)

# customer profile linked with prcdda level
cus[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in cus[!,:PostalCode]]
df = leftjoin(cus, prcdda, on= :PC_ref => :FSALDU)

[names(df) eltype.(eachcol(df))]

summarystats(df[!,:WSHHDTOT])
# customer's allocation of PRCDDA density, not that urban
quantile(skipmissing(df[!,:WSHHDTOT]),0:0.1:1)

#CSV.write("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_318k_superfull.csv",df_full)
#



########################################################################################
# analysys from here
# at PRCDDA level
# df = CSV.read("C:\\Users\\012790\\Desktop\\scorecard_mortgage_v4\\cus_318k_superfull.csv",missingstrings=["","NULL","NaNs"])

[names(df) eltype.(eachcol(df))]



# ratio of downpatment vs. property, using liquid asset non rsp now
df[!,:DtoP_ratio] = df[!,:WSLIQORAVG] ./df[!,:WSPRIMREAVG]

# nan is type number, not missing... need to change first then could apply
df[!,:WSLIQORAVG] = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSLIQORAVG]]
df[!,:LIQASTAVG]  = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:LIQASTAVG]]
df[!,:WSMORTAVG]  = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSMORTAVG]]
df[!,:WSPRIMREAVG]= [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSPRIMREAVG]]
df[!,:WSSAVNGAVG] = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:WSSAVNGAVG]]
df[!,:DtoP_ratio] = [ismissing(x) ? missing : isnan(x) ? missing : x for x in df[!,:DtoP_ratio]]


## insurability test

[names(df) eltype.(eachcol(df))]

df[!,:downpay_20] = [ismissing(x) ? missing : x*0.1999 for x in df[!,:WSPRIMREAVG]]

df[!,:insurability] = [ismissing(x) ? "None" : x>= 1000000 ? "Not Insurable" : z<y ? "Insured" : "Insurable"
                        for (x,y,z) in zip(df[!,:WSPRIMREAVG],df[!,:downpay_20],df[!,:WSLIQORAVG])]


t = freqtable(df[!,:insurability])
prop(t)



freqtable(df[!,:rank_ver_3_0],df[!,:insurability])

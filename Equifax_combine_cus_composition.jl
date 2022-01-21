#######################################################
# start: 2021-02-18
# we want to look at the information on TCHE,TCIN TCMG
# look at Equifax raw data

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
using Missings
using DelimitedFiles

gr()



######################################################################
# read file
tc  = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\Equifax_file1.csv",missingstrings=["","NULL"]))
[names(tc) eltype.(eachcol(tc))]


# there are 351,217 records about the mortgage
sort(freqtable(tc[tc.typecode .== "M",:member]),rev=true)
tc=tc[tc.typecode .== "M",:]

CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\report_file1\\Equifax_file1_tc_mortgage.csv",tc)


# 181,487 clients have mortgage recods
length(unique(tc[tc.typecode .== "M",:custref]))


# this one have duplicate, one clients might have multiple property
Med = string("\$",round(median(skipmissing(tc[!,:highcr]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(tc[!,:highcr])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Mortgage High CR Distribution",
      label = "High CR",
      xlabel = "Mortgage High CR",
      ylabel = "Count ",
      xticks = 0:100000:1000000,
      nbins = 0:10000:1000000,
#      legend =:topleft
)
plot!([median(tc[!,:highcr])], seriestype="vline"
     ,label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(round(Int,x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\tc_mortgage_highcr.png")








##############
# get file 3 aggregate info

df   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\equifax_file3_tcHE_IN_MG.csv",missingstrings=["","NULL"]))
[names(df) eltype.(eachcol(df))]


# find the cusomter whoever have mortgage previously

list = groupby(tc,:custref)
list = combine(list) do x
      (N     = length(x.custref)
      ,highcr_max   = maximum(x.highcr)
      ,dtreport_max   = maximum(x.dtreport)
      ,dtreport_min   = minimum(x.dtreport)
      )
end

# this one no duplicate, only show max
Med = string("\$",round(median(skipmissing(list[!,:highcr_max]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(list[!,:highcr_max])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Mortgage Max High CR Distribution",
      label = "Max High CR",
      xlabel = "Mortgage Max High CR",
      ylabel = "Count of Customer",
      xticks = 0:100000:1000000,
      nbins = 0:10000:1000000,
#      legend =:topleft
)
plot!([median(list[!,:highcr_max])], seriestype="vline"
     ,label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(round(Int,x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\tc_mortgage_max_highcr.png")







#=
TCMG020	NUMBER OF TRADES WITH DATE REPORTED IN THE PAST 24 MONTHS
TCMG029	NUMBER OF OPEN TRADES
TCMG030	NUMBER OF OPEN TRADES WITH BALANCE > 0
TCMG031	TOTAL BALANCE FOR OPEN TRADES
TCMG034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES
TCMG035	TOTAL MONTHLY TERM FOR OPEN TRADES
TCMG036	UTILIZATION PERCENTAGE FOR OPEN TRADES
=#

sort(freqtable(df[!,:TCMG029]),rev=true)

# remove cusomter who don't have equifax info
df = df[df.TCMG029 .!== missing,:]


##
# combine two version file together

dff = leftjoin(df,list,on= :CUSTOMER_REFERENCE_NUMBER => :custref)
[names(dff) eltype.(eachcol(dff))]

sort(freqtable(dff[!,:N]),rev = true)
sort(freqtable(dff[!,:TCMG029]),rev=true)

# munipulate columns to get info needed
dff[!,:N] = [ismissing(x) ? 0 : x for x in dff[!,:N]]

freqtable(dff[!,:N],dff[!,:TCMG029])

dff[!,:ever_mortgage] = [x==0 ? "Never Mortgage" : y == 0 ? "No Mortgage Now" : "Have Mortgage Now" for (x,y) in zip(dff[!,:N],dff[!,:TCMG029])]
freqtable(dff[!,:ever_mortgage])


dff[!,:current_mortgage_number] = [x==0 ? "No Mortgage currently" : x ==1 ? "One Mortgage" : ">1 Mortgage" for x in dff[!,:TCMG029]]
freqtable(dff[!,:current_mortgage_number])
freqtable(dff[!,:ever_mortgage],dff[!,:current_mortgage_number])







################################################
# link to customer base
match   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\CUSTOMER_MATCHINGKEY.csv",missingstrings=["","NULL"]))
cus     = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\cus_profile_656k_20210413.csv",missingstrings=["","NULL"]))

# list for the customer who ever had mortgage
list = unique(mortgage[!,:custref ])

list = DataFrame(custref =list)
list[!,:ind] .=1


#
[names(match) eltype.(eachcol(match))]
[names(cus)  eltype.(eachcol(cus))]

join1 = innerjoin(match,cus,on=:PrimaryClientID)

full = innerjoin(join1,dff, on=:Customer_referece_number =>:CUSTOMER_REFERENCE_NUMBER)
join1 = nothing
join2 = nothing
df    = nothing
cus   = nothing

[names(full)  eltype.(eachcol(full))]

CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\mortgage_cus_composition_full_20210413_396k.csv",full)



###########################################################
# Part 2: info from here

[names(full)  eltype.(eachcol(full))]








# age bin
age_cuts = [25,35,45,55,65]
full.age_ca = cut(join2.age_today, age_cuts, extend = true)

freqtable(full[!,:age_ca],full[!,:cat])



##################################################
# look at distribution by age
summarystats(full[!,:age_today])


histogram(
      collect(skipmissing(full[!,:age_today])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Age Distribution",
      label = "Customer",
      xlabel = "Current Age",
      ylabel = "Count of Customer",
      xticks = 20:5:80,
      nbins = 18:80,
#      legend =:topleft
)
plot!([median(collect(skipmissing(full[!,:age_today])))], seriestype="vline"
     ,label="Median Age = $(median(collect(skipmissing(full[!,:age_today]))))"
      ,linestyle = :dash
      ,yformatter = x->string(round(Int,x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\age_distribution.png")


# split by mortgage group
freqtable(dff[!,:ever_mortgage],dff[!,:current_mortgage_number])
full = full[full.age_today .!== missing,:]

# age distribution by have ever have mortgage
histogram(
      collect(skipmissing(full[!,:age_today])),
      group = full[!,:ever_mortgage],
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Age Distribution",
      xlabel = "Current Age",
      ylabel = "Count of Customer",
      xticks = 20:5:80,
      nbins = 18:80,
#      legend =:topleft
)

savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\age_distribution_by_mortgage.png")


# age distribution by how many mortgage they have
histogram(
      collect(skipmissing(full[!,:age_today])),
      group = full[!,:current_mortgage_number],
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Age Distribution",
      xlabel = "Current Age",
      ylabel = "Count of Customer",
      xticks = 20:5:80,
      nbins = 18:80,
#      legend =:topleft
)
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\age_distribution_by_mortgage_num.png")









########################
# merge in postal code info
full   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\mortgage_cus_composition_full_20210413_396k.csv",missingstrings=["","NULL"]))
EA     = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\ePCCF_match.csv",missingstrings=["","NULL"]))

[names(full)  eltype.(eachcol(full))]
[names(EA)  eltype.(eachcol(EA))]

full =full[full.PostalCode .!== missing, :]

full = leftjoin(full,EA,on = :PostalCode =>:FSALDU)

CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\mortgage_cus_composition_full_20210413_396k.csv",full)

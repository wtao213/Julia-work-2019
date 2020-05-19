## ###########################################################
# start date: May,15,2020
# Anymization test to make sure data sensitive level


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



########################################################
# import sample data
# df = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_sr_ver_3_0.csv",missingstrings=["","NULL"])
df = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md.csv")
[names(df) eltype.(eachcol(df))]

freqtable(df[!,:age_today])
freqtable(df[!,:PostalCode])

# after change not ascii to "not ascii" and uppercase city
# return 6292 out of 10186 original file
freqtable(df[!,:CityName])

summarystats(df[!,:age_today])
summarystats(df[!,:MTD])


# there are missing inside and french as well... how to deal with them in the future
df[!,:CityName] =[ismissing(x) ? missing : isascii(x) ? uppercase(x) : "not ascii" for x in df[!,:CityName]]

## before do anything, check your postalcode formal
df[!,:PC_size] = sizeof.(df[!,:PostalCode])

maximum(sizeof.(df[!,:PostalCode]))
minimum(sizeof.(df[!,:PostalCode]))


# check the postalcode meet format and standize the letter
# \w could be num and word, \D is non dig
occursin(r"^\D\d\D\d\D\d$","a1d3f4")
r"^\D\d\D\d\D\d$"

df[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\d\D\d$",x) ? uppercase(x) : missing
                 for x in df[!,:PostalCode]]

freqtable(df[!,:PC_ref])





################################################
# real work start from, check your postal code

a = groupby(df, :PC_ref)

b = combine(a, :PC_ref => length => :count )
b[b.count .>=50,:]

sum(b[(b.PC_ref .!== missing).& (b.count .>=50),:count])
# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.8,0.9,0.95,0.97,0.98,0.99])

histogram(collect(skipmissing(b[!,:count])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Post Code distribution",  xlabel = "customer num in a post code" ,ylabel = "Count of postal code"
      ,nbins = 0:2:100)













##################################################################
## from here, start part 2: remove postalcode letter
## what about have left 5 digitals


## check left 5 characters

df[!, :Postal_l5] = [
      ismissing(x) ? missing : chop(strip(x), head = 0, tail = 1)
      for x in df[!,:PC_ref]
]

freqtable(df[!,:Postal_l5])


a = groupby(df, :Postal_l5)
b = combine(a, :Postal_l5 => length => :count )
b[b.count .>=50,:]

sum(b[(b.Postal_l5 .!== missing).& (b.count .>=50),:count])

# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.8,0.9,0.95,0.97,0.98,0.99])

histogram(collect(skipmissing(b[!,:count])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Post Code distribution"
      ,xlabel = "customer num in a PC L5"
      ,ylabel = "Count of postal code"
      ,yformatter = x->string(Int(x/1e3),"K")
      ,nbins = 0:2:100)





## check left 4 characters

df[!,:Postal_l4] = [ ismissing(x) ? missing : chop(strip(x),head=0,tail=2) for x in df[!,:PC_ref]]

freqtable(df[!,:Postal_l4])

a = groupby(df, :Postal_l4)
b = combine(a, :Postal_l4 => length => :count )
b[b.count .>=50,:]

sum(b[(b.Postal_l4 .!== missing).& (b.count .>=50),:count])


# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.7,0.8,0.9])

histogram(collect(skipmissing(b[!,:count]))
            ,nbins = 0:2:100
            ,fillalpha = 0.4
            ,linealpha = 0.1
            ,legend = false
            ,title = "Post Code distribution"
            ,xlabel = "customer num in a PC L4"
            ,ylabel = "Count of postal code"
            )


# let's check where these customer located
list = b[(b.Postal_l4 .!== missing).& (b.count .>=50),:]
list = list[!,:Postal_l4]

# inner join with customer base to get there location
c = innerjoin(df, list, on = :Postal_l4)

t = freqtable(c[!,:province])
prop(t)


t = freqtable(c[!,:CityName])
sort!(t,rev = true)
prop(t)


freqtable(c[!,:CityName])
##
## check left 3 characters

df[!,:Postal_l3] = [ ismissing(x) ? missing : chop(strip(x),head=0,tail=3) for x in df[!,:PC_ref]]

freqtable(df[!,:Postal_l3])

a = groupby(df, :Postal_l3)
b = combine(a, :Postal_l3 => length => :count )

b[b.count .>=50,:]

sum(b[(b.Postal_l3 .!== missing).& (b.count .>=50),:count])
# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.7,0.8,0.9])

histogram(collect(skipmissing(b[!,:count]))
            ,nbins = 6:2:1000
            ,fillalpha = 0.4
            ,linealpha = 0.1
            ,legend = false
            ,title = "Post Code distribution"
            ,xlabel = "customer num in a PC L4"
            ,ylabel = "Count of postal code"
            )



#
# check left 2 characters

df[!,:Postal_l2] = [ ismissing(x) ? missing : chop(strip(x),head=0,tail=4) for x in df[!,:PC_ref]]

freqtable(df[!,:Postal_l2])

a = groupby(df, :Postal_l2)
b = combine(a, :Postal_l2 => length => :count )
b[b.count .>=50,:]

sum(b[(b.Postal_l2 .!== missing).& (b.count .>=50),:count])

# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.7,0.8,0.9])

histogram(collect(skipmissing(b[!,:count]))
            ,nbins = 0:2:500
            ,fillalpha = 0.4
            ,linealpha = 0.1
            ,legend = false
            ,title = "Post Code distribution"
            ,xlabel = "customer num in a PC L2"
            ,ylabel = "Count of postal code"
            )








##########################
# export your data
CSV.write("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md.csv",df)











##############################################
#=
      for the Anymization test, if a row is unique, will remove as supressed row
      for whichever left, will got new row count
      count_values_of_allvar/new_row_count as avg_risk
      count(distinct all_var) as count_values_of_allvar

      average risk = total combination of metrics/ non unique row total
      for us, we have to have the risk <=0.355

=#

[names(df) eltype.(eachcol(df))]




test = groupby(df, [:age_today ,:MTD])

test2 =combine(test, :PrimaryClientID =>length => :cus_count)

count_values_of_allvar = length(test2[test2.cus_count .> 1,:cus_count])
new_row_count          = sum(test2[test2.cus_count .> 1,:cus_count])

avg_risk = count_values_of_allvar/new_row_count

typeof([:age_today ,:MTD])


function Anonymize_Check(df::DataFrame, vars::Vector)
      test  = groupby(df, vars)
      test2 = combine(test, :PrimaryClientID =>length => :cus_count)

      count_values_of_allvar = length(test2[test2.cus_count .> 1,:cus_count])
      new_row_count          = sum(test2[test2.cus_count .> 1,:cus_count])
      suppressed_row_count   = sum(test2[test2.cus_count .== 1,:cus_count])

      avg_risk = new_row_count==0 ? "--" : count_values_of_allvar/new_row_count

      println("""Anoymize Check
      suppressed rows count = $suppressed_row_count
      new rows count        = $new_row_count
      Average Risk          = $avg_risk""")
end

Anonymize_Check(df,[:age_today ,:MTD])








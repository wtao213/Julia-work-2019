## ###########################################################
# start date: May,15,2020
# Anymization test to make sure data sensitive level


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



########################################################
# import sample data
# df = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_sr_ver_3_0.csv",missingstrings=["","NULL"])
df = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md_v2.csv")
[names(df) eltype.(eachcol(df))]

freqtable(df[!,:age_today])
freqtable(df[!,:PostalCode])
freqtable(df[!,:Equity_ca])

quantile(df[!,:age_today],[0,0.25,0.5,0.75,1])
summarystats(df[!,:age_today])
mode(df[!,:age_today])
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
# after check there are 155,007 records compare with original 157,304
# after change format, there are 154,684
freqtable(df[!,:PC_ref])

temp_df[findall(in([1,3,5]), temp_df[:IndexVal]), :]


ps6 = innerjoin(df,l1,on = :PC_ref)
ps6[!,:PS_final] = ps6[!,:PC_ref]
################################################
# real work start from, check your postal code
# group by postal code to check the customer larger than 50
a = groupby(df, :PC_ref)
# out of the 157304 postal code, only 47 have customer greater than 50
b = combine(a, :PC_ref => length => :count)
b[b.count .>= 15,:]

# list 1 contain customer remain full postal code
# inner join need dataframe
# l1 = collect(skipmissing(b[b.count .>= 15,:PC_ref]))
l1 = b[(b.count .>= 15) .& (b.PC_ref .!== missing),:]

sum(b[(b.PC_ref .!== missing).& (b.count .>= 15),:count])
# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.8,0.9,0.95,0.97,0.98,0.99])


####
histogram(
      collect(skipmissing(b[!, :count])),
      fillalpha = 0.4,
      linealpha = 0.1,
      label = "Postal Code Count",
#      legend = false,
      title = "Postal Code Distribution",
      xlabel = "Customer num in a post code",
      ylabel = "Count of postal code",
      yformatter = x->string(Int(x/1e3),"K"),
      nbins = 0:1:50,
)
plot!([15], seriestype="vline", label="Aggregate 15",linestyle = :dash)













##################################################################
## from here, start part 2: remove postalcode letter
## what about have left 5 digitals


## check left 5 characters
# then there are 67,753 value, and only 189 have customer greater than 50
# 65,043 PC in pc_ref, and 14,598 customer covered
df[!, :Postal_l5] = [
      ismissing(x) ? missing : chop(strip(x), head = 0, tail = 1)
      for x in df[!,:PC_ref]
]

freqtable(df[!,:Postal_l5])

# out of the 67751 records, only 189 have customer greater than 50
a = groupby(df, :Postal_l5)
b = combine(a, :Postal_l5 => length => :count )
b[b.count .>=15,:]

sum(b[(b.Postal_l5 .!== missing).& (b.count .>=15),:count])

# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.8,0.9,0.95,0.97,0.98,0.99])

histogram(collect(skipmissing(b[!,:count])),fillalpha = 0.4, linealpha = 0.1,legend = false
      ,title = "Post Code distribution"
      ,xlabel = "customer num in a PC L5"
      ,label = "Postal Code Count"
      ,yformatter = x->string(Int(x/1e3),"K")
      ,ylabel = "Count of postal code"
      ,nbins = 0:2:100)
plot!([15], seriestype="vline", label="Aggregate 15",linestyle = :dash)




## check left 4 characters
# then there are 9,161 value, and 1,989 has larger than 50
df[!,:Postal_l4] = [ ismissing(x) ? missing : chop(strip(x),head=0,tail=2) for x in df[!,:PC_ref]]

freqtable(df[!,:Postal_l4])

a = groupby(df, :Postal_l4)
b = combine(a, :Postal_l4 => length => :count )
b[b.count .>=15,:]

sum(b[(b.Postal_l4 .!== missing).& (b.count .>=15),:count])


# distribution of it
summarystats(b[!,:count])
quantile(b[!,:count],[0.7,0.8,0.9])

histogram(collect(skipmissing(b[!,:count]))
            ,nbins = 0:2:100
            ,fillalpha = 0.4
            ,linealpha = 0.1
            ,label = "Postal Code Count"
      #      ,legend = false
            ,title = "Post Code distribution"
            ,xlabel = "customer num in a PC L4"
            ,ylabel = "Count of postal code"
            )
plot!([15], seriestype="vline", label="Aggregate 15",linestyle = :dash)



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
# then there are 2,947 value, and 1,098 has larger than 50
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
# then there are 2,947 value, and 1,098 has larger than 50
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


############
# real funcion start here
function Anonymize_Check(df::DataFrame, vars::Vector)
      test  = groupby(df, vars)
      test2 = combine(test, :PrimaryClientID =>length => :cus_count)

      count_values_of_allvar = length(test2[test2.cus_count .> 1,:cus_count])
      new_row_count          = sum(test2[test2.cus_count .> 1,:cus_count])
      suppressed_row_count   = sum(test2[test2.cus_count .== 1,:cus_count])

      avg_risk = new_row_count == 0 ? "--" : count_values_of_allvar/new_row_count

      println("""
      Anoymize Check
      --------------------------------------------------
      suppressed rows count = $suppressed_row_count
      new rows count        = $new_row_count
      Average Risk          = $avg_risk
      """)
end

Anonymize_Check(df,[:age_today ,:MTD])

[names(df) eltype.(eachcol(df))]

# overall risk 0.418
Anonymize_Check(df,[:age_today ,:MTD,:Equity_ca,:rank_ver_3_0,:Postal_l3])
# overal risk  0.343
Anonymize_Check(df,[:age_ca ,:MTD,:Equity_ca,:rank_ver_3_0,:Postal_l3])

# overal risk  0.3524, which is exacttly less than 0.355
# but only 145,279 rows left
# if removed postal code left 4, then only 30 people removed, and risk is 0.002
Anonymize_Check(df,[:age_ca ,:tenure_ca,:Equity_ca,:rank_ver_3_0,:Postal_l4])
Anonymize_Check(df,[:age_ca ,:tenure_ca,:Equity_ca,:rank_ver_3_0])













#################################################
# part 3: restructure postal code
# start date: june, 22,2020
df = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md_v2.csv")
[names(df) eltype.(eachcol(df))]

# delete a column by name
select!(df, Not(:count))
################################################
# real work start from, check your postal code
# group by postal code to check the customer larger than 15
#
a = groupby(df, :PC_ref)
# out of the 157304 postal code, only 47 have customer greater than 50
b = combine(a, :PC_ref => length => :count)
b[b.count .>= 15,:]

sum(b[(b.PC_ref .!== missing).& (b.count .>= 15),:count])
# list 1 contain customer remain full postal code
# inner join need dataframe
#l1 = b[(b.count .>= 15) .& (b.PC_ref .!== missing),:]
#l1 = b[(b.count .>= 50) .& (b.PC_ref .!== missing),:]
l1 = b[(b.count .>= 25) .& (b.PC_ref .!== missing),:]

# get customer with 6 digital
ps6 = innerjoin(df,l1,on = :PC_ref)
ps6[!,:PS_final] = ps6[!,:PC_ref]


## get remaining customers, and do it in left 5 level
df2 = leftjoin(df,l1,on = :PC_ref)
df2 = df2[ ismissing.(df2.count),:]
df2 = df2[!,1:55]
[names(df2) eltype.(eachcol(df2))]

a = groupby(df2, :Postal_l5)
b = combine(a, :Postal_l5 => length => :count)

b[b.count .>= 15,:]
sum(b[(b.Postal_l5 .!== missing).& (b.count .>= 15),:count])

# get left 5 digital customers
#l2 = b[(b.count .>= 15) .& (b.Postal_l5 .!== missing),:]
#l2 = b[(b.count .>= 50) .& (b.Postal_l5 .!== missing),:]
l2 = b[(b.count .>= 25) .& (b.Postal_l5 .!== missing),:]

ps5 = innerjoin(df2,l2,on = :Postal_l5)
ps5[!,:PS_final] = ps5[!,:Postal_l5]


## get remaining customers, and do it in left 4 level
df2 = leftjoin(df2,l2,on = :Postal_l5)
df2 = df2[ ismissing.(df2.count),:]
df2 = df2[!,1:55]
[names(df2) eltype.(eachcol(df2))]

a = groupby(df2, :Postal_l4)
b = combine(a, :Postal_l4 => length => :count)

b[b.count .>= 15,:]
sum(b[(b.Postal_l4 .!== missing).& (b.count .>= 15),:count])

# get left 5 digital customers
# l3 = b[(b.count .>= 15) .& (b.Postal_l4 .!== missing),:]
#l3 = b[(b.count .>= 50) .& (b.Postal_l4 .!== missing),:]
l3 = b[(b.count .>= 25) .& (b.Postal_l4 .!== missing),:]

ps4 = innerjoin(df2,l3,on = :Postal_l4)
ps4[!,:PS_final] = ps4[!,:Postal_l4]



## get remaining customers, and do it in left 3 level
df2 = leftjoin(df2,l3,on = :Postal_l4)
df2 = df2[ ismissing.(df2.count),:]
df2 = df2[!,1:55]
[names(df2) eltype.(eachcol(df2))]

a = groupby(df2, :Postal_l3)
b = combine(a, :Postal_l3 => length => :count)

b[b.count .>= 15,:]
sum(b[(b.Postal_l3 .!== missing).& (b.count .>= 15),:count])

# get left 5 digital customers
# l4 = b[(b.count .>= 15) .& (b.Postal_l3 .!== missing),:]
# l4 = b[(b.count .>= 50) .& (b.Postal_l3 .!== missing),:]
l4 = b[(b.count .>= 25) .& (b.Postal_l3 .!== missing),:]

ps3 = innerjoin(df2,l4,on = :Postal_l3)
ps3[!,:PS_final] = ps3[!,:Postal_l3]



## get remaining customers, and do it in left 2 level
df2 = leftjoin(df2,l4,on = :Postal_l3)
df2 = df2[ ismissing.(df2.count),:]
df2 = df2[!,1:55]
[names(df2) eltype.(eachcol(df2))]

a = groupby(df2, :Postal_l2)
b = combine(a, :Postal_l2 => length => :count)

b[b.count .>= 15,:]
sum(b[(b.Postal_l2 .!== missing).& (b.count .>= 15),:count])

# get left 2 digital customers
# l5 = b[(b.count .>= 15) .& (b.Postal_l2 .!== missing),:]
# l5 = b[(b.count .>= 50) .& (b.Postal_l2 .!== missing),:]
l5 = b[(b.count .>= 25) .& (b.Postal_l2 .!== missing),:]

ps2 = innerjoin(df2,l5,on = :Postal_l2)
ps2[!,:PS_final] = ps2[!,:Postal_l2]



#####################
# finally append all together
df2 = leftjoin(df2,l5,on = :Postal_l2)
df2 = df2[ ismissing.(df2.count),:]
df2[!,:PS_final] .= "NoAssi"




dff=[ps6;ps5;ps4;ps3;ps2;df2]
[names(dff) eltype.(eachcol(dff))]

sort(freqtable(dff[!,:PrimaryClientID]),rev=true)

dff = innerjoin(df,dff,on=:PrimaryClientID)
rename!(dff, Dict(:PS_final => "PS_final_15",:PS_final_1 => "PS_final_50"))

CSV.write("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md_v2.csv",dff)

# PS=dff[!,[1,57]]
# dff=innerjoin(df,PS,on=:PrimaryClientID,makeunique = true)
df = nothing
PS = nothing


## recut age _Ca, have split at 35, don't need to change assign score
# prepare future need swicher vs first time home buyer
age_cuts = [25,30,35,40,50,65]
dff.age_ca_2 = cut(dff.age_today, age_cuts, extend = true)
freqtable(dff.age_ca_2)



##################################################
# do the anoymization test
function Anonymize_Check(df::DataFrame, vars::Vector)
      test  = groupby(df, vars)
      test2 = combine(test, :PrimaryClientID =>length => :cus_count)

      count_values_of_allvar = length(test2[test2.cus_count .> 1,:cus_count])
      new_row_count          = sum(test2[test2.cus_count .> 1,:cus_count])
      suppressed_row_count   = sum(test2[test2.cus_count .== 1,:cus_count])

      avg_risk = new_row_count == 0 ? "--" : count_values_of_allvar/new_row_count

      println("""
      Anoymize Check
      --------------------------------------------------
      vars = $vars
      suppressed rows count = $suppressed_row_count
      new rows count        = $new_row_count
      Average Risk          = $avg_risk
      """)
end


##
dff = CSV.read("C:\\Users\\012790\\Desktop\\TU_prepare\\cus_score_full_318k_md_v2.csv")
[names(dff) eltype.(eachcol(dff))]

# end goal: r need to <= 0.355
# 0.418
Anonymize_Check(dff,[:age_today ,:MTD,:Equity_ca,:rank_ver_3_0,:Postal_l3])

# if ps _final is aggregate at 15 level, then 0.027
Anonymize_Check(dff,[:PS_final_15])

# 0.409 , remaining 98k, ps >= 15 aggregate
# 0.355, remaining 166k, ps >= 50 aggregate
Anonymize_Check(dff,[:age_ca ,:tenure_ca,:Equity_ca,:rank_ver_3_0,:PS_final_50])
# 0.299, remain 227k, ps >= 50 aggregate
Anonymize_Check(dff,[:age_ca ,:tenure_ca,:Equity_ca,:PS_final_50])
# 0.08,318k remianing, ps >= 50 level
Anonymize_Check(dff,[:rank_ver_3_0,:PS_final_50])


# 0.21, 287k remaining, ps >=50
Anonymize_Check(dff,[:age_ca,:Equity_ca,:PS_final_1])


# score_version_3_0 bin to different level and test?
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

dff[!,:rank_50] = rank(dff[!,:score_version_3_0],50)
dff[!,:rank_20] = rank(dff[!,:score_version_3_0],20)

# rank 50 groups,ps 15, 0.341 and 223k remain
# rank 50 groups,ps 25, 0.288 and 265 remain
# rank 50 groups,ps 50, 0.232 and 296 remain
Anonymize_Check(dff,[:rank_50,:PS_final_15])
Anonymize_Check(dff,[:rank_50,:PS_final_50])
Anonymize_Check(dff,[:rank_50,:PS_final_25])




# rank 20 groups,ps 15, 0.285 and 267k remain
# rank 20 groups,ps 25, 0.222 and 296 remain
# rank 20 groups,ps 50, 0.160 and 313 remain
Anonymize_Check(dff,[:rank_20,:PS_final_15])
Anonymize_Check(dff,[:rank_20,:PS_final_25])
Anonymize_Check(dff,[:rank_20,:PS_final_50])


# 0.351, remain 200k,age ps >=50
Anonymize_Check(dff,[:age_ca ,:rank_50,:PS_final_1])
# 0.310, remain 238k,age ps >=50
Anonymize_Check(dff,[:age_ca ,:rank_20,:PS_final_1])
# 0.340, remain 220k,age ps >=50
Anonymize_Check(dff,[:age_ca_2,:rank_20,:PS_final_1])
# 0.28, remain 268,ps >=50
Anonymize_Check(dff,[:age_ca_2,:rank_ver_3_0,:PS_final_1])



# rank 20 groups,age_ca2,ps 15, 0.341 and 223k remain
# rank 20 groups,age_ca2,ps 25, 0.288 and 265 remain
# rank 20 groups,age_ca2,ps 50, 0.232 and 296 remain
Anonymize_Check(dff,[:age_ca_2,:rank_20,:PS_final_15])
Anonymize_Check(dff,[:age_ca_2,:rank_20,:PS_final_25])
Anonymize_Check(dff,[:age_ca_2,:rank_20,:PS_final_50])



# rank 20 groups,age_ca2,ps 15, 0.341 and 223k remain
# rank 20 groups,age_ca2,ps 25, 0.288 and 265 remain
# rank 20 groups,age_ca2,ps 50, 0.232 and 296 remain
Anonymize_Check(dff,[:age_ca_2,:rank_ver_3_0,:PS_final_15])
Anonymize_Check(dff,[:age_ca_2,:rank_ver_3_0,:PS_final_25])
Anonymize_Check(dff,[:age_ca_2,:rank_ver_3_0,:PS_final_50])


## look at the customers be suppressed
test  = groupby(dff, [:age_ca_2,:rank_ver_3_0,:PS_final_25])
test2 = combine(test, :PrimaryClientID =>length => :cus_count)

remove = test2[test2.cus_count .== 1,:]

freqtable(remove[!,:age_ca_2])
freqtable(remove[!,:rank_ver_3_0])
freqtable(remove[!,:PS_final_25])

##############################
#= discusion:
1. do we need to exclude Quebec from entire file?
2. other than age, what else we really want to keep and use after got data back
      to split/identify FTHB and Swicher?
3. do we want to sacrifice some amount of clients to add in more variables/ smaller bin?




=#

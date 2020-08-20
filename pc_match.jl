## ###########################################################
# start date: Augest,6nd,2020
#  look at the data ouput from TU

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

##############################################



###################################################################
# THIS IS OUR RESULT!
# cvsc100: credit risk score, cvsc110: bankdruptycy score
# cvsc100: credit risk score, cvsc110: bankdruptycy score
# prop_score_first_mtg: tu propensity model to predit likelihood of a
# customer openning a first mortgage loan within 3 months after scoreing
shuf = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\TU_output\\output_shuffled.txt", delim = '|',missingstrings=["","NULL"]))

link = DataFrame!(CSV.File("C:\\Users\\012790\\Desktop\\TU_output\\province_link.csv", missingstrings=["","NULL"]))

[names(shuf ) eltype.(eachcol(shuf))]
[names(link)  eltype.(eachcol(link))]


CSV.write("C:\\Users\\012790\\Desktop\\TU_output\\province_link.csv",link)
#######################################################

shuf[!,:cr] = [x>=680 ? ">= 680" : "<680" for x in shuf[!,:cvsc100]]


# match postcalcode_revise2 to postalcode
link[!,:PC_ref] = [ismissing(x) ? missing :
                 occursin(r"^\D\d\D\s*\d\D\d$",x) ? uppercase(replace(x,r"\s*"=>"")) : missing
                 for x in link[!,:PostalCode]]



l= sort(unique(shuf[!,:postalcode_revise2]))

# group by
tt = groupby(link,[:PC_ref])
tt= combine(tt) do x
      (
      province     = first(x.province)
      ,Postal_l5   = first(x.Postal_l5)
      ,Postal_l4   = first(x.Postal_l4)
      ,Postal_l3   = first(x.Postal_l3)
      ,Postal_l2   = first(x.Postal_l2)
      ,PS_final_25 = first(x.PS_final_25)
      )
end



##############################################################################
# get level 6
l6 = collect(skipmissing([length(x)==6 ? x : missing for x in l]))

pc6 = tt[(tt.PC_ref .!== missing).& (x ∈ l6 for x in tt[!,:PC_ref] ),:]
pc6[!,:pc_match] = pc6[!,:PC_ref]
[names(pc6)  eltype.(eachcol(pc6))]



# get level 5
#remaining data frame in tt

t= tt[(tt.PC_ref .!== missing).& (x ∉ l6 for x in tt[!,:PC_ref] ),:]

l5 = collect(skipmissing([length(x)==5 ? x : missing for x in l]))

pc5 = t[(t.Postal_l5 .!== missing).& (x ∈ l5 for x in t.Postal_l5),:]
pc5[!,:pc_match] = pc5[!,:Postal_l5]


#get level 4
t= t[(t.Postal_l5 .!== missing).& (x ∉ l5 for x in t.Postal_l5),:]

l4 = collect(skipmissing([length(x)==4 ? x : missing for x in l]))

pc4 = t[(t.Postal_l4 .!== missing).& (x ∈ l4 for x in t.Postal_l4),:]
pc4[!,:pc_match] = pc4[!,:Postal_l4]


#get level 3
t= t[(t.Postal_l4 .!== missing).& (x ∉ l4 for x in t.Postal_l4),:]

l3 = collect(skipmissing([length(x)==3 ? x : missing for x in l]))

pc3 = t[(t.Postal_l3 .!== missing).& (x ∈ l3 for x in t.Postal_l3),:]
pc3[!,:pc_match] = pc3[!,:Postal_l3]



#get level 2
t= t[(t.Postal_l3 .!== missing).& (x ∉ l3 for x in t.Postal_l3),:]

l2 = collect(skipmissing([length(x)==2 ? x : missing for x in l]))

pc2 = t[(t.Postal_l2 .!== missing).& (x ∈ l2 for x in t.Postal_l2),:]
pc2[!,:pc_match] = pc2[!,:Postal_l2]


#get remaining
t= t[(t.Postal_l2 .!== missing).& (x ∉ l2 for x in t.Postal_l2),:]

t[!,:pc_match] .= ""


###############################################
# append all levels together
dff=[pc6;pc5;pc4;pc3;pc2;t]
[names(dff) eltype.(eachcol(dff))]



CSV.write("C:\\Users\\012790\\Desktop\\TU_output\\province_link_match.csv",dff)



##################################################################
# left join with shuf
# don't use dff directly.... one to many..
mc = unique(dff[!,[:pc_match,:province]])
unique(dff[!,:pc_match])


test = groupby(mc,:pc_match)
test= combine(test) do x
      (
      N     = length(x.pc_match)
      ,province = first(x.province)
      )
end

test[test.N .> 1,:]

mc[mc.pc_match .== "L5A3S",:]

full = leftjoin(shuf,mc,on=:postalcode_revise2  => :pc_match)

full = nothing

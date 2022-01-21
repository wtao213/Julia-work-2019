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

###############################################################################
# equifax data with mortgage info



############################################################################################################
# slightly longer conversion
s    = readlines("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\EFX_File3", keep=true)
nl   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\Equifax_file3_name_tc.csv",missingstrings=["","NULL"]))

[names(nl) eltype.(eachcol(nl))]

p_s = nl[!,:FROM]
p_e = nl[!,:TO]
c_n = nl[!,:Col_name]


# it takes around than 3 min, but only 189 column other than 226
df = DataFrame()
for i in 1:length(p_s)
    df[!,c_n[i]]= string.(strip.(SubString.(s,p_s[i],p_e[i])))
end

# export
CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\equifax_file3_tcHE_IN_MG.csv",df)

s   = nothing
c_n = nothing
p_s = nothing
p_e = nothing




################################################################################
# doing reamining analysis, only need to read previous output
# part 2: stat general info
# thining of reading it
df   = DataFrame(CSV.File("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\equifax_file3_tcHE_IN_MG.csv",missingstrings=["","NULL"]))

[names(df) eltype.(eachcol(df))]


###########################################
#=
TCIN029	NUMBER OF OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
TCIN030	NUMBER OF OPEN TRADES WITH BALANCE > 0 (BALANCE AND HC/CL SHOULD BE <=$50K)
TCIN031	TOTAL BALANCE FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
TCIN034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
TCIN035	TOTAL MONTHLY TERM FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
TCIN036	UTILIZATION PERCENTAGE FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)


TCHE022	AGE OF NEWEST TRADE
TCHE023	AGE OF OLDEST TRADE
TCHE029	NUMBER OF OPEN TRADES
TCHE030	NUMBER OF OPEN TRADES WITH BALANCE > 0
TCHE031	TOTAL BALANCE FOR OPEN TRADES
TCHE034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES
TCHE035	TOTAL MONTHLY TERM FOR OPEN TRADES
TCHE036	UTILIZATION PERCENTAGE FOR OPEN TRADES


TCMG020	NUMBER OF TRADES WITH DATE REPORTED IN THE PAST 24 MONTHS
TCMG029	NUMBER OF OPEN TRADES
TCMG030	NUMBER OF OPEN TRADES WITH BALANCE > 0
TCMG031	TOTAL BALANCE FOR OPEN TRADES
TCMG034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES
TCMG035	TOTAL MONTHLY TERM FOR OPEN TRADES
TCMG036	UTILIZATION PERCENTAGE FOR OPEN TRADES
=#



###########################
# look up overlap between these
summarystats(df[!,:TCIN029])
quantile(collect(skipmissing(df[!,:TCIN029])),[0.8,0.9,0.95,0.96,0.98])
summarystats(df[!,:TCIN030])

summarystats(df[!,:TCHE029])
quantile(collect(skipmissing(df[!,:TCHE029])),[0.8,0.9,0.95,0.96,0.98])

summarystats(df[!,:TCMG029])
quantile(collect(skipmissing(df[!,:TCMG029])),[0.8,0.9,0.95,0.96,0.98])

# remove that 15890 no records clients
dff= df[df.TCIN029 .!== missing,:]

# look at count
length(dff[(dff.TCIN029 .>0),:CUSTOMER_REFERENCE_NUMBER])
length(dff[(dff.TCHE029 .>0),:CUSTOMER_REFERENCE_NUMBER])
length(dff[(dff.TCMG029 .>0),:CUSTOMER_REFERENCE_NUMBER])

sum(dff[(dff.TCIN029 .>0),:TCIN031])
sum(dff[(dff.TCHE029 .>0),:TCHE031])
sum(dff[(dff.TCMG029 .>0),:TCMG031])

sum(dff[(dff.TCIN029 .>0),:TCIN034])
sum(dff[(dff.TCHE029 .>0),:TCHE034])
sum(dff[(dff.TCMG029 .>0),:TCMG034])





length(dff[(dff.TCIN029 .>0) .& (dff.TCHE029 .>0),:CUSTOMER_REFERENCE_NUMBER])
length(dff[(dff.TCIN029 .>0) .& (dff.TCMG029 .>0),:CUSTOMER_REFERENCE_NUMBER])
length(dff[(dff.TCMG029 .>0) .& (dff.TCHE029 .>0),:CUSTOMER_REFERENCE_NUMBER])

length(dff[(dff.TCIN029 .>0) .& (dff.TCMG029 .>0) .& (dff.TCHE029 .>0),:CUSTOMER_REFERENCE_NUMBER])


length(dff[(dff.TCIN029 .==0) .& (dff.TCMG029 .==0) .& (dff.TCHE029 .>0),:CUSTOMER_REFERENCE_NUMBER])

############################################
# look at the size for current balance
summarystats(df[!,:TCIN031])
summarystats(dff[dff.TCIN031 .>0 ,:TCIN031])
summarystats(dff[dff.TCIN031 .>-999 ,:TCIN031])


summarystats(df[!,:TCHE031])
summarystats(dff[dff.TCHE031 .>0,:TCHE031])
summarystats(dff[dff.TCHE031 .>-999 ,:TCHE031])


summarystats(df[!,:TCMG031])
summarystats(dff[dff.TCHE031 .>0 ,:TCHE031])





#########################
# group by mortgage/heloc/installment
dff= df[df.TCIN029 .!== missing,:]
[names(dff) eltype.(eachcol(dff))]

dff[!,:TCIN] = [x>0 ? "Have Installment" : "No Installment" for x in dff[!,:TCIN029]]
dff[!,:TCMG] = [x>0 ? "Have Mortgage" : "No Mortgage" for x in dff[!,:TCMG029]]
dff[!,:TCHE] = [x>0 ? "Have HELOC" : "No HELOC" for x in dff[!,:TCHE029]]

#= change formot
for x in [:TCMG031,:TCMG034,:TCHE031,:TCHE034,:TCIN031,:TCIN034]
      dff[!,x] = [a>=0 ? a : missing for a in dff[!,x]]
end
=#

ai_df = groupby(dff,[:TCIN,:TCMG,:TCHE])
ai_df= combine(ai_df) do x
      (FICO_median  = median(x.FICO_8_0_SCORE)
      ,N            = length(x.FICO_8_0_SCORE)
#      ,median_TCMG031   = median(skipmissing(filter(x -> x>0, x.TCMG031)))
      ,median_TCMG031   = median(skipmissing( x.TCMG031))
      ,median_TCMG034   = median(skipmissing(x.TCMG034))
      ,median_TCHE031   = median(skipmissing(x.TCHE031))
      ,median_TCHE034   = median(skipmissing(x.TCHE034))
      ,median_TCIN031   = median(skipmissing(x.TCIN031))
      ,median_TCIN034   = median(skipmissing(x.TCIN034))
      )
end


CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\heloc_in_mortgage.csv",ai_df)



################################################################
# freqtable for name
sort(freqtable(df[!,:LAST_NAME]),rev=true)
# missing last name client
size(df,1) -length(collect(skipmissing(df[!,:LAST_NAME])))


# checking for postalcode
size(df,1) -length(collect(skipmissing(df[!,:POSTAL_CODE])))

tt=df[(df.LAST_NAME .!== missing) .& (df.LAST_NAME .== "TAO").& (df.FIRST_NAME .== "WANTING"),:]

CSV.write("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\test.csv",tt)

# look at distribution for fico score :FICO_8_0_SCORE
summarystats(df[!,:FICO_8_0_SCORE])
summarystats(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE])
length(df[df.FICO_8_0_SCORE .> 680,:FICO_8_0_SCORE])

#
histogram(
      df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE],
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "FICO Score Distribution",
      label =" FICO score",
      xlabel = "FICO SCORE",
      ylabel = "Count of Customer",
      xticks = 400:50:900,
      legend =:topleft
#      nbins = -5000:1000:30000,
)
plot!([median(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE]))], seriestype="vline"
     , label="Median = $(median(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE])))"
      ,linestyle = :dash
      ,yformatter = x->string(Int(x/1e3),"K"))
plot!([quantile(skipmissing(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE]),0.1)]
     , seriestype="vline", label="Bottom 10% = $(quantile(df[df.FICO_8_0_SCORE .> 0,:FICO_8_0_SCORE],0.1))"
      ,linestyle = :dash)





##
# think of: TCAM029_2	NUMBER OF OPEN TRADES
# TCAM031_2	TOTAL BALANCE FOR OPEN TRADES
summarystats(df[!,:TCAM029])
summarystats(df[!,:TCAM031])

summarystats(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031])
length(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>150000),:TCAM031])
quantile(collect(skipmissing(df[!,:TCAM031])),[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
# we can not really look at avg, cause a car or personal loan is only less than 50K
#avg will lower the mortgage amount
df[!,:TCAM_avg] = [ismissing(x) ? missing : y<0 ? missing : y/x for (x,y) in zip(df[!,:TCAM029],df[!,:TCAM031])]
summarystats(df[!,:TCAM_avg])


# look at df[!,:TCAM031]
Med = string("\$",round(median(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "All Trade lines Balance Distribution",
      label = "All Balance",
      xlabel = "All Trade lines Balance",
      ylabel = "Count of Customer",
      xticks = 0:50000:500000,
      nbins = 0:5000:500000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCAM031 .!==missing) .&(df.TCAM031 .>0),:TCAM031]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )

#
summarystats(df[!,:TCAM029_2])
summarystats(df[!,:TCAM031_2])











######################

# TCMG034  original you lend
# TCM029 open trade
[names(df) eltype.(eachcol(df))]

df[!,:TCMG_cur_avg] = [ismissing(x) ? missing : y<0 ? missing : y/x for (x,y) in zip(df[!,:TCMG029],df[!,:TCMG031])]
df[!,:TCMG_org_avg] = [ismissing(x) ? missing : y<0 ? missing : y/x for (x,y) in zip(df[!,:TCMG029],df[!,:TCMG034])]

# 31072 client has more than 1 mortgage
summarystats(df[!,:TCMG029])
summarystats(df[(df.TCMG029 .!==missing) .&(df.TCMG029 .>0),:TCMG029])
df[(df.TCMG029 .!==missing) .&(df.TCMG029 .>1),:]
quantile(df[(df.TCMG029 .!==missing) .&(df.TCMG029 .>0),:TCMG029],[0.90,0.95,0.98,0.99])

# for customer, who only has 1 mortgage, average current balance and principle mortgage size
summarystats(df[(df.TCMG029 .!==missing) .&(df.TCMG029 .== 1),:TCMG031])
summarystats(df[(df.TCMG029 .!==missing) .&(df.TCMG029 .== 1),:TCMG034])

# mortgage info
summarystats(df[!,:TCMG034])
summarystats(df[(df.TCMG034 .!== missing) .& (df.TCMG034 .>= 0),:TCMG034])

#look at current
summarystats(df[!,:TCMG_cur_avg])
summarystats(df[(df.TCMG_cur_avg .!== missing) .& (df.TCMG_cur_avg .>= 0),:TCMG_cur_avg])

# look at original principle
summarystats(df[!,:TCMG_org_avg])
summarystats(df[(df.TCMG_org_avg .!== missing) .& (df.TCMG_org_avg .>= 0),:TCMG_org_avg])



# look at current plot
Med = string("\$",round(median(skipmissing(df[(df.TCMG031 .!==missing) .&(df.TCMG031 .>0),:TCMG031]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCMG031 .!==missing) .&(df.TCMG031 .>0),:TCMG031])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Mortgage Current Balance Distribution",
      label = "Current Balance",
      xlabel = "Mortgage Current Balance",
      ylabel = "Count of Customer",
      xticks = 0:100000:1000000,
      nbins = 0:10000:1000000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCMG031 .!==missing) .&(df.TCMG031 .>0),:TCMG031]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(round(Int,x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCMG_current.png")




#
# look at Principle plot
Med = string("\$",round(median(skipmissing(df[(df.TCMG034 .!==missing) .&(df.TCMG034  .>0),:TCMG034 ]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCMG034 .!==missing) .&(df.TCMG034 .>0),:TCMG034])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Mortgage Principal Distribution",
      label = "Principal",
      xlabel = "Mortgage Principal",
      ylabel = "Count of Customer",
      xticks = 0:100000:1000000,
      nbins = 0:10000:1000000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCMG034 .!==missing) .&(df.TCMG034 .>0),:TCMG034]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(round(Int,x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCMG_Principal.png")


# try to have an idea of how many term already paid
#TCMG035	TOTAL MONTHLY TERM FOR OPEN TRADES

# this part be carfule, not all trade start at same time
df[!,:term_paid] = [ismissing(x) ? missing : y<0 ? missing : (x-y)/z for (x,y,z) in zip(df[!,:TCMG034],df[!,:TCMG031],df[!,:TCMG035])]










####################################
# TCHE029	NUMBER OF OPEN TRADES
# TCHE030	NUMBER OF OPEN TRADES WITH BALANCE > 0
# TCHE031	TOTAL BALANCE FOR OPEN TRADES
# TCHE034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES
# TCHE035	TOTAL MONTHLY TERM FOR OPEN TRADES
dff = df[df.TCHE029 .!== missing,:]
summarystats(df[!,:TCHE029])
summarystats(df[!,:TCHE030])
summarystats(df[!,:TCHE031])
summarystats(df[!,:TCHE034])
quantile(collect(skipmissing(df[!,:TCHE029])),[0.1,0.25,0.5,0.7,0.9])

unique(df[!,:TCHE029])
unique(df[!,:TCHE030])

## look at the open trade with 0 balance
length(dff[dff.TCHE029 .> 0 ,:TCHE029])
dff[!,:HE_usage] = [(x==0)&(y==-999) ? missing : x>y ? x-y : missing for (x,y) in zip(dff[!,:TCHE029],dff[!,:TCHE031])]
length(collect(skipmissing(dff[!,:HE_usage])))
sum(collect(skipmissing(dff[!,:HE_usage])))

length(dff[(dff.TCHE031 .< 1000) .&(dff.TCHE031 .>0),:HE_usage])




sort(freqtable(df[!,:TCHE029]) ,rev = true)


# look at Heloc usage rate, for tche 029 >0, look at their balance =0 vs remation
summarystats(df[(df.TCHE029 .!==missing) .&(df.TCHE029 .>0),:TCHE034])
summarystats(df[(df.TCHE029 .!==missing) .&(df.TCHE029 .>0) .&(df.TCHE034.>0),:TCHE034])



# look at current plot
Med = string("\$",round(median(skipmissing(df[(df.TCHE031 .!==missing) .&(df.TCHE031 .>0),:TCHE031]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCHE031 .!==missing) .&(df.TCHE031 .>0),:TCHE031])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Home Equity Current Balance Distribution",
      label = "Current Balance",
      xlabel = "Home Equity Current Balance",
      ylabel = "Count of Customer",
      xticks = 0:50000:500000,
      nbins = 0:5000:500000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCHE031 .!==missing) .&(df.TCHE031 .>0),:TCHE031]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCHE_current.png")



#
# look at current plot
summarystats(df[(df.TCHE034 .!==missing) .&(df.TCHE034 .>0),:TCHE034])
quantile(df[(df.TCHE034 .!==missing) .&(df.TCHE034 .>0),:TCHE034],[0.9,0.95])

Med = string("\$",round(median(skipmissing(df[(df.TCHE034 .!==missing) .&(df.TCHE034 .>0),:TCHE034]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCHE034 .!==missing) .&(df.TCHE034 .>0),:TCHE034])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Home Equity High Credit Distribution",
      label = "Home Equity High Credit",
      xlabel = "Home Equity High Credit",
      ylabel = "Count of Customer",
      xticks = 0:50000:500000,
      nbins = 0:5000:500000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCHE034 .!==missing) .&(df.TCHE034 .>0),:TCHE034]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCHE_highcredit.png")



####################################
# TCIN029
summarystats(df[!,:TCIN029])
summarystats(df[!,:TCIN031])
summarystats(df[!,:TCIN034])
quantile(collect(skipmissing(df[!,:TCIN029])),[0.1,0.25,0.5,0.7,0.9])

sort(freqtable(df[!,:TCIN029]) ,rev = true)


# look at current plot
Med = string("\$",round(median(skipmissing(df[(df.TCIN031 .!==missing) .&(df.TCIN031 .>0),:TCIN031]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCIN031 .!==missing) .&(df.TCIN031 .>0),:TCIN031])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Installment Current Balance Distribution",
      label = "Current Balance",
      xlabel = "Installment Current Balance",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCIN031 .!==missing) .&(df.TCIN031 .>0),:TCIN031]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCIN_current.png")


# TCIN030	NUMBER OF OPEN TRADES WITH BALANCE > 0 (BALANCE AND HC/CL SHOULD BE <=$50K)
# TCIN031	TOTAL BALANCE FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
# TCIN034	TOTAL HIGH CREDIT/CREDIT LIMIT FOR OPEN TRADES (BALANCE AND HC/CL SHOULD BE <=$50K)
# look at high credit plot
Med = string("\$",round(median(skipmissing(df[(df.TCIN034 .!==missing) .&(df.TCIN034 .>0),:TCIN034]))/1e3,digits= 1),"K")
histogram(
      collect(skipmissing(df[(df.TCIN034 .!==missing) .&(df.TCIN034 .>0),:TCIN034])),
      fillalpha = 0.4,
      linealpha = 0.1,
      title = "Installment High Credit Distribution",
      label = "Installment High Credit",
      xlabel = "Installment High Credit Balance",
      ylabel = "Count of Customer",
      xticks = 0:5000:50000,
      nbins = 0:1000:50000,
#      legend =:topleft
)
plot!([median(skipmissing(df[(df.TCIN034 .!==missing) .&(df.TCIN034 .>0),:TCIN034]))], seriestype="vline"
     , label="Median = $(Med)"
      ,linestyle = :dash
      ,xformatter = x->string("\$",Int(x/1e3),"K")
      ,yformatter = x->string(Int(x/1e3),"K")
      )
savefig("C:\\Users\\012790\\Desktop\\Equifax_Customer_Data\\TCIN_high_credit.png")

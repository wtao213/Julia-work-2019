# Customer Value Segmentation
# scoring file

ENV["COLUMNS"] = 240
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

using Interpolations

using CategoricalArrays
using Missings
using DelimitedFiles


gr()



###################################################
df = DataFrame(CSV.File("C://Users//012790//Desktop//value_segmentation_2021_11//cus_oct_cohort_20211119.csv", missingstring = ["", "NULL"]))
prcdda = DataFrame(CSV.File("C://Users//012790//Desktop//scorecard_mortgage_v4//WealthScapes2019_PRCDDA_results.csv"))


[names(df) eltype.(eachcol(df))]
[names(prcdda) eltype.(eachcol(prcdda))]

# prcdda only get the columns we need
[names(prcdda) eltype.(eachcol(prcdda))]
# ea = select(prcdda, Not(Between(:WSAVRRSPC, :WSLIQORPEN)),Not(Between(:SLI, :PRCDCSD)))
ea = select(prcdda, Not(Between(:WSAVRRSPC, :WSLIQORPEN)))
[names(ea) eltype.(eachcol(ea))]


## remove missing
# only subset of the dataframe? WS too wide file
df[!, :PC_ref] = [ismissing(x) ? missing :
                  occursin(r"^\D\d\D\s*\d\D\d$", x) ? uppercase(replace(x, r"\s*" => "")) : missing
                  for x in df[!, :PostalCode]]

# if any columns in  postcode, age_t0, income is missing, the scoring won't complete, filter them out as seperate group                       
df_missing = df[ismissing.(df.PC_ref).|ismissing.(df.age_t0).|ismissing.(df.Income), :]
df_missing[!, :segments_ind] .= "Data missing"


df = df[df.PC_ref.!==missing, :]
df = df[(df.age_t0.!==missing).&(df.Income.!==missing), :]



df3 = leftjoin(df, ea, on = :PC_ref => :FSALDU)

replace!(df3[!, :WSSAVNOS_potential], NaN => missing)
replace!(df3[!, :WSINVEST_potential], NaN => missing)
replace!(df3[!, :WSINVEIR_potential], NaN => missing)
replace!(df3[!, :WSCDEBT_potential], NaN => missing)
replace!(df3[!, :WSMORT_potential], NaN => missing)


#############################
CSV.write("C://Users//012790//Desktop//value_segmentation_2021_11//cus_oct_cohort_full_20211119.csv", df3)

df = nothing
df3 = nothing
prcdda = nothing
ea = nothing


########################################################################################################
# real sementation start from here
######################################################################################################

df = DataFrame(CSV.File("C://Users//012790//Desktop//value_segmentation_2021_11//cus_oct_cohort_full_20211119.csv", missingstring = ["", "NULL"]))
score = DataFrame(CSV.File("C://Users//012790//Desktop//value_segmentation_2021_11//scoring_file_20211117.csv", missingstring = ["", "NULL"]))


[names(df) eltype.(eachcol(df))]
[names(score) eltype.(eachcol(score))]

# short_tenure_ind to indicate whether the customer tenure is too short, if yes, we won't score on them.
df_new = df[df.short_tenure_ind.==1, :]
df_new[!, :segments_ind] .= "Too New"

df = df[df.short_tenure_ind.==0, :]




###############################################
###########################################
# Dimension 1:
## rank 1:
# Equity_t0
summarystats(df[!, :Equity_t0])
quantile(collect(skipmissing(df[!, :Equity_t0])), collect(0:0.1:1))

pcuts = score[!, :pcuts]
Equity_t0_qvals = score[!, :Equity_t0_qvals]
[pcuts Equity_t0_qvals]

wblip = LinearInterpolation(Equity_t0_qvals, pcuts, extrapolation_bc = Flat())
wblip(Equity_t0_qvals)

df[!, :Equity_t0_score_lip] = wblip(df[!, :Equity_t0])


###########
# trade_t_3
summarystats(df[!, :trade_t_3])
quantile(collect(skipmissing(df[!, :trade_t_3])), collect(0:0.05:1))


pcuts = score[!, :pcuts]
trade_t_3_qvals = convert(Vector{Float64}, score[!, :trade_t_3_qvals])

[pcuts trade_t_3_qvals]


wblip = LinearInterpolation(trade_t_3_qvals, pcuts, extrapolation_bc = Flat())
df[!, :trade_t_3_score_lip] = [ismissing(x) ? 0 : wblip(x) for x in df[!, :trade_t_3]]

freqtable(df[!, :trade_t_3_score_lip])

## Total Dimension 1 score and plot
df[!, :D1_ttl_score_lip] = df[!, :trade_t_3_score_lip] + 2 .* df[!, :Equity_t0_score_lip]






##############################################################################
# get score for dimension 2:

# AssetsTotal_bal_t_3
summarystats(df[!, :AssetsTotal_bal_t_3])
quantile(collect(skipmissing(df[!, :AssetsTotal_bal_t_3])), collect(0:0.1:1))

pcuts = score[!, :pcuts]
AssetsTotal_bal_t_3_qvals = convert(Vector{Float64}, score[!, :AssetsTotal_bal_t_3_qvals])       # find data values
wblip = LinearInterpolation(AssetsTotal_bal_t_3_qvals, pcuts, extrapolation_bc = Flat())

df[!, :AssetsTotal_bal_t_3_score_lip] = [ismissing(x) ? 0 : wblip(x) for x in df[!, :AssetsTotal_bal_t_3]]

freqtable(df[!, :AssetsTotal_bal_t_3_score_lip])



# using only ea investment df[!,:WSINVEST_potential]
summarystats(df[!, :WSINVEST_potential])

pcuts = score[!, :pcuts]
WSINVEST_potential_qvals = convert(Vector{Float64}, score[!, :WSINVEST_potential_qvals])
wblip = LinearInterpolation(WSINVEST_potential_qvals, pcuts, extrapolation_bc = Flat())



df[!, :WSINVEST_potential_score_lip] = [ismissing(x) ? 0 : wblip(x) for x in df[!, :WSINVEST_potential]]
freqtable(df[!, :WSINVEST_potential_score_lip])



################
# score for dimension 2
df[!, :D2_score_lip] = df[!, :WSINVEST_potential_score_lip] .+ df[!, :AssetsTotal_bal_t_3_score_lip]





######################################################
# dimention 3: income + age
summarystats(df[!, :Income])
quantile(collect(skipmissing(df[!, :Income])), [0.05, 0.8, 0.9, 0.95, 0.98])

summarystats(df[!, :age_t0])
quantile(collect(skipmissing(df[!, :age_t0])), [0.05, 0.8, 0.9, 0.95, 0.98])


# want to get the rank within each group
df2 = combine(groupby(df, :age_t0), sdf -> sort(sdf, :Income), s -> (rank = 1:nrow(s),), nrow => :n)
[names(df2) eltype.(eachcol(df2))]


df2[!, :age_group_quantile] = df2[!, :rank] ./ df2[!, :n]
# if customer's age is larger than 65, change their quantile all to 1, not too much future value for us
df2[!, :age_group_quantile] = [x >= 65 ? 0.0 : y for (x, y) in zip(df2[!, :age_t0], df2[!, :age_group_quantile])]

df = df2
df2 = nothing



# df[!,:age_group_quantile]
summarystats(df[!, :age_group_quantile])

pcuts = score[!, :pcuts]
age_group_quantile_qvals = convert(Vector{Float64}, score[!, :age_group_quantile_qvals])       # find data values
wblip = LinearInterpolation(age_group_quantile_qvals, pcuts, extrapolation_bc = Flat())


########
# lower income in age group are in top ranking, using aseding rank of inceome
df[!, :D3_ttl_score_lip] = [ismissing(x) ? 0 : wblip(x) for x in df[!, :age_group_quantile]]
sort(freqtable(df[!, :D3_ttl_score_lip]), rev = true)


#############################################################################
# 2 *2*2 metrics
[names(df) eltype.(eachcol(df))]

d1_med = score[!, :d1_med][1]
d2_med = score[!, :d2_med][1]
d3_med = score[!, :d3_med][1]
len = length(df[!, 1])
trade_ttl = sum(replace(df[!, :trade_t_3], missing => 0))
equity_ttl = sum(replace(df[!, :Equity_t0], missing => 0))
EA_mortgage_ttl = sum(replace(df[!, :WSMORT_potential], missing => 0))
EA_lending_potential_ttl = sum(replace(df[!, :WSCDEBT_potential], missing => 0))
EA_Chg_ttl = sum(replace(df[!, :WSSAVNOS_potential], missing => 0))
EA_investment_ttl = sum(replace(df[!, :WSINVEST_potential], missing => 0))

df[!, :D1_lip_ind] = [x > d1_med ? "High" : "Low" for x in df[!, :D1_ttl_score_lip]]
df[!, :D2_lip_ind] = [x > d2_med ? "High" : "Low" for x in df[!, :D2_score_lip]]
df[!, :D3_lip_ind] = [x > d3_med ? "High" : "Low" for x in df[!, :D3_ttl_score_lip]]

freqtable(df[!, :D1_lip_ind])
freqtable(df[!, :D2_lip_ind])
freqtable(df[!, :D3_lip_ind])


#######################################
## look at external data
df[!, :WSMORTAVG] = df[!, :WSMORT_potential] ./ df[!, :WSMORT_incidence]

# add in too new tenure customers
DF = vcat(df, df_new, df_missing, cols = :union)


################################################
#
###############
# look at summary table
ai_df = groupby(df, [:D1_lip_ind, :D2_lip_ind, :D3_lip_ind])
ai_df = combine(ai_df) do x
    (N = length(x.D1_lip_ind), N_pct = length(x.D1_lip_ind) / len,
        Equity_t0_mean = mean(collect(skipmissing(x.Equity_t0))),
        Equity_t0_pct = sum(collect(skipmissing(x.Equity_t0))) ./ equity_ttl,
        trade_t_3_mean = mean(replace(x.trade_t_3, missing => 0)),
        trade_t_3_pct = sum(collect(skipmissing(x.trade_t_3))) ./ trade_ttl,
        AssetsTotal_bal_t_3_mean = mean(replace(x.AssetsTotal_bal_t_3, missing => 0)),
        Income_mean = mean(replace(y -> y > 300000 ? 300000 : y, x.Income)),
        Age_med = median(x.age_t0), Age_mean = mean(x.age_t0),
        Tenure_mean = mean(x.MTD_t0),
        EA_mortgage_inc = mean(collect(skipmissing(replace(x.WSMORT_incidence, NaN => missing)))),
        EA_mortgage_avg = mean(collect(skipmissing(replace(x.WSMORTAVG, NaN => missing)))),
        EA_mortgage_pct = sum(collect(skipmissing(x.WSMORT_potential))) ./ EA_mortgage_ttl,
        EA_lending_inc = mean(collect(skipmissing(replace(x.WSCDEBT_incidence, NaN => missing)))),
        EA_lending_avg = mean(collect(skipmissing(replace(x.WSCDEBTAVG, NaN => missing)))),
        EA_lending_pct = sum(collect(skipmissing(x.WSCDEBT_potential))) ./ EA_lending_potential_ttl,
        EA_Chg_inc = mean(collect(skipmissing(replace(x.WSSAVNOS_incidence, NaN => missing)))),
        EA_Chg_avg = mean(collect(skipmissing(replace(x.WSSAVNOSAVG, NaN => missing)))),
        EA_Chg_pct = sum(collect(skipmissing(x.WSSAVNOS_potential))) ./ EA_Chg_ttl,
        EA_investment_inc = mean(collect(skipmissing(replace(x.WSINVEIR_incidence, NaN => missing)))),
        EA_investment_avg = mean(collect(skipmissing(replace(x.WSINVEIRAVG, NaN => missing)))),
        EA_investment_pct = sum(collect(skipmissing(x.WSINVEST_potential))) ./ EA_investment_ttl
    )
end


CSV.write("C://Users//012790//Desktop//value_segmentation_2021_11//cus_oct_profile_lip_2by2.csv", ai_df)
CSV.write("C://Users//012790//Desktop//value_segmentation_2021_11//cus_oct_seg_final_616k.csv", DF)

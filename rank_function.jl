## using tiedrank to get the ranking of the column, so that if there are multiple observation have same value between groups edge, 
## the weight of position will tell us where to put these values
## ranking function: floor(rank * k /(n+1))  there are k groups we want, and we have n non-missing observations in the vector.

## function to replace proc rank
function rank(x::AbstractVector,k::Integer)
    ceil.(Int,tiedrank(x)*k/(length(x) +1))
end

function rank(x::AbstractVector,p::AbstractFloat)
          0< p <=1 || error("p must between 0 to 1")
    isinteger(1/p) || error("need ratio be exact divided by 1")
    ceil.(Int,tiedrank(x)/(p*(length(x)+1)))
end

df1[!,:asset_rank] = rank(df1[!,:TotalAssets_ttl_t12], 0.05)
df1[!,:asset_rank] = rank(df1[!,:TotalAssets_ttl_t12], 10)

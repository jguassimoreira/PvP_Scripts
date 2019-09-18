library(psych)
library(readr)

##Load in data
ippaDat = read_csv("IPPA_for_reliability.csv")
ippaDat = drop_na(ippaDat) # get rid of missing values


parDat = ippaDat[,2:29] #split data for IPPA parent
friDat = ippaDat[,30:54] #split data for IPPA friend

##Compute pearson and polychoric correlations on EFA data
parR = cor(parDat)
friR = cor(friDat)

##Plot the correlation matrices
corPlot(parR, numbers = TRUE) # I know, I know, I'm plotting pearon's but use polychoric below. Sue me.
corPlot(friR, numbers = TRUE) # But seriously, I think it's Ok to do here since this is just for descriptive purposes

##Examine the hierarchical structure, just for fun
bassAckward(parR,2)

bassAckward(friR,2)

##Examine alpha, again just for fun. 
alpha(parDat)
alpha(friDat)

##Get omega
omega(parDat,2, poly=TRUE) #going with the most parsimonious bi-factor model possible (i.e., just two factors)
omega(friDat,2, poly=TRUE) 


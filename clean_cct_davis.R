library(readr)
library(spatstat.utils)

##Loop over subjects and...

##1 Read in their data

##2 Select the columns/data that we need

##3 Save it to a 'running' dataframe

##4 Check to make sure the data look OK (QA)

##5 Spit out a csv of data

subs = c(5000:5022)

outDat = data.frame(ID=integer(), trialNum = integer(), Deck=character(), Context=integer(), Decision=integer(), EV=double(), SD=double())

for (s in 1:length(subs)) {
  
  parFile = Sys.glob(file.path("C:","Users","jguas","Desktop","PvP_Prep","Davis","Data", sprintf("%s*ParentGain*.csv", subs[s])))
  friFile = Sys.glob(file.path("C:","Users","jguas","Desktop","PvP_Prep","Davis","Data", sprintf("%s*FriendGain*.csv", subs[s])))
  
  parDat = read_csv(parFile)
  friDat = read_csv(friFile)
  
  if (subs[s] != parDat$participant[1]) {
    print(sprintf("participant ID %s does not match data in ParentGain file, skipping to next subject", subs[s]))
    next
  }
  
  if (subs[s] != friDat$participant[1]) {
    print(sprintf("participant ID %s does not match data in ParentGain file, skipping to next subject", subs[s]))
    next
  }
  
  parDat = parDat[!is.na(parDat$deckResp.corr),c("participant", "Deck", "Context", "deckResp.corr", "EV", "SD")]
  friDat = friDat[!is.na(friDat$deckResp.corr),c("participant", "Deck", "Context", "deckResp.corr", "EV", "SD")]
  
  parDat[,"trialNum"] = c(1:length(parDat$participant))
  friDat[,"trialNum"] = c(1:length(friDat$participant))
  
  parDat = parDat[,c("participant", "trialNum", "Deck", "Context", "deckResp.corr", "EV", "SD")]
  friDat = friDat[,c("participant", "trialNum", "Deck", "Context", "deckResp.corr", "EV", "SD")] 

    
  if(length(parDat$deckResp.corr) < 24) {
    print(sprintf("number of PGFL responses for subject %s less than minimum, inspect responses manually", subs[s]))
    next
  }
  if(length(friDat$deckResp.corr) < 24) {
    print(sprintf("number of FGPL responses for subject %s less than minimum, inspect responses manually", subs[s]))
    next
  }
  
  outDat = rbind(outDat, parDat, friDat)
  
}

names(outDat) = c("ID", "trialNum", "Deck", "Context", "Decision", "Return", "Risk")

if(mean(inside.range(outDat$Risk, c(0, 40))) != 1) {
  print("at least 1 Risk value is out of range, inspect responses manually")
}

if(mean(inside.range(outDat$Return, c(-60, 16.88))) != 1) {
  print("at least 1 Return value is out of range, inspect responses manually")
}

write.csv(outDat, file = "CCT_Level1.csv", row.names = FALSE, col.names = TRUE)

mod1 <- glmer(Decision ~ Context + Return + Risk + (1+Context+Return+Risk|ID), data = outDat, family = binomial, control = glmerControl(optimizer = "bobyqa"))
summary(mod1)

#### Parents vs Peers Neuro Behavioral Data Processing Script ####

## This script creates a pipeline for cleaning and processing
## data from the behavioral part of the  PvP_Neuro study. It does so by creating
## a set of functions for processing each individual task and then
## assigns them into a broader class that cleans all the subject's
## data when called.

import pandas as pd
import numpy as np
import glob
import inspect

### REFRESH AGGREGATE FILES BY CLEARING OLD DATA ###
#CCT
cct = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\CCT_Level1.csv")
cct.iloc[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\CCT_Level1.csv", index=False)

#Cups
cups = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\Cups_Level1.csv")
cups[0:0].to_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\Cups_Level1.csv", index=False)


### CLASS DEFINEMENT ###

#Define the class 'clean', which is going to clean all the subject's data
class clean:
    
    def __init__(self, name):
        self.name = name
    
    def clean_CCT(*args,**kwargs):
        print("Cleaning CCT")
        out = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\CCT_Level1.csv")
        for dir in taskDirs["CCT"]:
            data = pd.read_csv("%s"%(dir))
            subID = dir.split('\\')[-4]
            subdir = str(taskDirs["CCT"][0])
            dataProc = pd.concat([data.loc[:,'participant'], data.loc[:,'Deck'], data.loc[:,'Context'], data.loc[:,'deckResp.corr'], data.loc[:,'EV'], data.loc[:,'SD']],  axis = 1)
            dataProc = dataProc.dropna()
            tn = np.arange(1,len(dataProc)+1)
            dataProc.insert(loc=1, column="trialNum", value=tn)
            dataProc = dataProc.rename(index=str, columns={"participant": "ID", "deckResp.corr": "Decision", "EV": "Return", "SD": "Risk"})
            outdir = '\\'.join(subdir.split("\\")[:-3])
            if dataProc.iloc[0,3] == 1:
                condLab = "ParentGain_FriendLose"
            else:
                condLab = "FriendGain_ParentLose"
            dataProc.to_csv("%s\\Lab_session\\Clean\\%s_CCT_%s.csv"%(outdir, subID, condLab), index = False)
            out = out.append(dataProc)
            out.to_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\CCT_Level1.csv", index=False)
    # print(inspect.getargspec(clean_CCT))
    
    def clean_colorCard(*args,**kwargs):
        print("Cleaning colorCard")
        for dir in taskDirs["colorCard"]:
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            subdir = str(taskDirs["colorCard"][0])
            subID = dir.split('\\')[-4]
            dataProc = pd.concat([data.loc[:,'PgFg_Code'], data.loc[:,'PlFl_Code'], data.loc[:,'PgFl_Code'], data.loc[:,'PlFg_Code'], data.loc[:,'choiceKey.keys'], data.loc[:,'parentEarningsThisi'], data.loc[:,'friendEarningsThisi']], axis = 1)
            dataProc = dataProc.dropna()
            dataProc = dataProc.rename(index = str, columns = {"choiceKey.keys": "decRaw", "parentEarningsThisi": "pOut", "friendEarningsThisi": "fOut"})
            dec = np.where(dataProc['PgFg_Code']==dataProc['decRaw'], 1, 
                  np.where(dataProc['PlFl_Code']==dataProc['decRaw'], 2,
                  np.where(dataProc['PgFl_Code']==dataProc['decRaw'], 3,
                  np.where(dataProc['PlFg_Code']==dataProc['decRaw'], 4, 999))))
            for d in dec: assert d != 999, "Error. Invalid condition code. Do you have missing decisions or mismatched dataframe columns?"
            dataProc.insert(loc = 7, column = "dec", value = dec)
            outdir = '\\'.join(subdir.split("\\")[:-3])
            dataProc.to_csv("%s\\Lab_session\\Clean\\%s_colorCard.csv"%(outdir, subID), index = False)
    
    def clean_cups(*args,**kwargs):
        print("Cleaning cups")
        out = pd.read_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\Cups_Level1.csv")
        for dir in taskDirs["cups"]:
            data = pd.read_csv("%s"%(dir)).iloc[1:,:]
            subdir = str(taskDirs['cups'][0])
            subID = dir.split('\\')[-4]
            dataProc = pd.concat([data.loc[:,'participant'], data.loc[:,'cueResp.corr'], data.loc[:,'EV'], data.loc[:,'SD']], axis = 1)
            dataProc = dataProc.dropna()
            tn = np.arange(1,len(dataProc)+1)
            dataProc.insert(loc=1, column="trialNum", value=tn)
            dataProc = dataProc.rename(index=str, columns={"participant": "ID", "cueResp.corr": "Decision", "EV": "Return", "SD": "Risk"})
            tt = np.where(dataProc['Return']>0, 1, 0)
            dataProc.insert(loc = 5, column = "TrialType", value = tt)
            outdir = '\\'.join(subdir.split("\\")[:-3])
            dataProc.to_csv("%s\\Lab_session\\Clean\\%s_CupsSelf.csv"%(outdir, subID), index=False)
            out = out.append(dataProc)
            out.to_csv("P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\Cups_Level1.csv", index=False)
        
    
    def clean_all(self, *args, **kwargs):
        self.clean_CCT(*args, **kwargs)
        self.clean_colorCard(*args, **kwargs)
        self.clean_cups(*args, **kwargs)

        
#Define subject IDs and list of tasks
#Can be adapted to include different tasks for future studies
datadir = "P:\\Parents_vs_Peers_(PvP)\\Neuro\\Data\\Behavioral_Data"
subList = glob.glob("%s\PP*"%(datadir))
taskList = ["CCT", "colorCard", "cups"]
taskDirs = {} #Initialize task list -- subject specific task directories will go here in just a bit

### CLEAN THE DATA BY DEFINING 'MAIN' FUNCTION ### 
def main():
    for sub in subList:

        ### SET UP DATA IDENTIFICATION PROCEDURES ###
        behavfiles = glob.glob("%s\%s\Lab_session\Raw\*.csv"%(datadir,sub.split('\\')[-1])) #Get behav files
        for task in taskList: taskDirs[task] = [] #populate taskDirs dict with labels
        for task in taskDirs: taskDirs[task] = glob.glob("%s\%s\Lab_session\Raw\*%s*.csv"%(datadir,sub.split('\\')[-1],task)) #populate taskDirs dict with behav directories
        print(taskDirs)

        ### LET USER KNOW THE DATA EACH SUBJECT HAS (FOR QA PURPOSES) ###
        for t, task in enumerate(taskDirs):
            if len(taskDirs[task]) == 0:
                print("There are no %s files"%(task))
                continue
            print(task)

        ### FINALLY CLEAN THE DATA
        cln = clean('cln')
        cln.clean_all(taskDirs)
        


### RUN THE SCRIPT ### 
if __name__ == "__main__":
  main()